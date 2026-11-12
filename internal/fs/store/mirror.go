package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

// MirrorStore keeps one logical repository mirrored across two or more Store
// backends (e.g. a primary + standby S3). It provides:
//
//   - write path: every immutable/data object is written to the current primary
//     strictly and to every healthy standby best-effort; a failing standby is
//     marked down and excluded from service but does not fail the operation
//     (its missing data is caught up by the background repair loop).
//   - read path: control-plane objects (current / locks / dav-activities /
//     format / uuid) always read from the current primary (strong consistency);
//     data objects (revs/revs-loose/revprops/txn) are read round-robin across
//     healthy mirrors for load balancing.
//   - availability: the primary is the first healthy mirror; if the primary
//     fails, the next healthy mirror is elected automatically.
//   - self-healing: a background goroutine periodically (and on demand) walks
//     the primary's full object set and copies any object a mirror is missing,
//     then re-marks the mirror healthy. This is what "recovered S3 automatically
//     catches up its data" means at the object level.
//
// Store semantics: objects are immutable except those managed via CAS/Delete.
// The single mutable object is `current` (the HEAD pointer); it is always
// written to the primary via CAS and mirrored to healthy standbys so any
// healthy mirror can serve the latest HEAD after a failover.
type MirrorStore struct {
	mirrors []Store
	healthy []bool

	mu   sync.Mutex
	prim int // index of the elected primary

	stop       chan struct{}
	done       chan struct{}
	repairable bool // set true when a mirror is marked down; repair reacts to it

	// repairOnce dedupes concurrent repair runs.
	repairOnce sync.Mutex

	// recovering is set while a fast recovery loop is running after a mirror
	// went down, so redundancy is restored in seconds instead of waiting for
	// the 60s periodic pass.
	recovering bool

	// writeQuorum is the number of copies a write must reach before it may be
	// acknowledged (0/1 = primary only, the historical behaviour). With >=2
	// the store behaves like a quorum-replicated log: a commit cannot be
	// acknowledged while too few replicas are reachable.
	writeQuorum int

	// repoPrefix scopes repair/listing to one repository. Without it, mirrors
	// sharing a bucket or a local root would copy every other repository's
	// objects into this repository's backends during repair.
	repoPrefix string

	// mutableOrder orders two values of a mutable key (<0: a older than b).
	// fs sets it so a rejoining mirror cannot overwrite a newer HEAD pointer
	// with its stale copy. Without it, values are treated as equal.
	mutableOrder func(key string, a, b []byte) int

	// controlKey names the repository's HEAD pointer. ackedCurrent holds the
	// bytes of the last HEAD this process successfully wrote (or adopted at
	// startup); after a primary failure the election must not accept writes
	// on a standby whose copy is older, otherwise a failover could lose an
	// acknowledged revision.
	controlKey   string
	ackedCurrent []byte
	fenced       bool

	// Replication status, for /_metrics and operators.
	lastSuccess []time.Time
	lastErr     []string
	repairing   map[string]bool

	// pending records the keys written while a mirror was out of service.
	// A mirror that was whole when it went down only misses these, so recovery
	// copies the delta instead of enumerating a million objects. Deletes are
	// not recorded (a leftover unreferenced object is harmless); the pending
	// set is cleared only when the mirror is whole again.
	pending         []map[string]bool
	pendingOverflow []bool

	// wholeAtDown is set when a healthy (hence complete) mirror goes down; it
	// enables the fast delta recovery. verify asks for one full pass after a
	// fast recovery, so a path that bypassed pending accounting is still caught.
	wholeAtDown []bool
	verify      []bool

	// repairOwner identifies this process in the cross-process repair lease,
	// so only one svngod instance runs the expensive full listing at a time.
	repairOwner string

	// lazy makes NewMirror return immediately and lets the leased background
	// repair catch up. The server enables it; tests and custom stores keep the
	// deterministic blocking alignment.
	lazy bool
}

// ReplicaStatus is a snapshot of one mirror's health.
type ReplicaStatus struct {
	Index       int
	Healthy     bool
	Primary     bool
	LastSuccess time.Time
	LastError   string
}

// SetMutableOrder installs the ordering used to reconcile mutable objects
// (the HEAD pointer) across mirrors during repair.
func (m *MirrorStore) SetMutableOrder(fn func(key string, a, b []byte) int) {
	m.mu.Lock()
	m.mutableOrder = fn
	m.mu.Unlock()
}

func (m *MirrorStore) order(key string, a, b []byte) int {
	m.mu.Lock()
	fn := m.mutableOrder
	m.mu.Unlock()
	if fn == nil {
		return 0
	}
	return fn(key, a, b)
}

// MirrorOption configures a MirrorStore before its initial repair.
type MirrorOption func(*MirrorStore)

// WithMutableOrder installs the ordering used to reconcile mutable objects
// (the HEAD pointer) during repair. It must be supplied before NewMirror's
// initial alignment pass runs, which is why it is an option.
func WithMutableOrder(fn func(key string, a, b []byte) int) MirrorOption {
	return func(m *MirrorStore) { m.mutableOrder = fn }
}

// WithRepoPrefix scopes repair and health accounting to one repository
// prefix (e.g. "repos/demo"). Required when several repositories share the
// same bucket or local directory.
func WithRepoPrefix(prefix string) MirrorOption {
	return func(m *MirrorStore) { m.repoPrefix = strings.Trim(prefix, "/") }
}

// WithLazyRepair makes NewMirror return without a blocking alignment pass; a
// leased background repair catches up. The server uses it so a restart does
// not wait for a full object scan (and so four processes do not each scan the
// bucket). Tests leave it off for deterministic construction.
func WithLazyRepair() MirrorOption {
	return func(m *MirrorStore) { m.lazy = true }
}

// WithWriteQuorum requires every write to reach n copies before it returns.
// n <= 1 keeps the best-effort standby behaviour.
func WithWriteQuorum(n int) MirrorOption {
	return func(m *MirrorStore) { m.writeQuorum = n }
}

// Journaling reports that commits should be described by a WAL record. The
// record is written even in availability mode (single-copy ack): it is what
// lets a restarted/rejoined replica reconcile HEAD instead of trusting a stale
// pointer, without making a standby outage block commits.
func (m *MirrorStore) Journaling() bool { return true }

// LiveCount returns how many mirrors are currently in service. Availability
// mode runs lease operations against this many replicas: all reachable nodes
// must agree, but a node outage shrinks the requirement to one copy instead of
// blocking commits.
func (m *MirrorStore) LiveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, h := range m.healthy {
		if h {
			n++
		}
	}
	return n
}

func NewMirror(mirrors []Store, opts ...MirrorOption) *MirrorStore {
	if len(mirrors) < 2 {
		panic("MirrorStore requires at least 2 mirrors")
	}
	mirrors = canonicalMirrors(mirrors)
	m := &MirrorStore{
		mirrors:         mirrors,
		healthy:         make([]bool, len(mirrors)),
		lastSuccess:     make([]time.Time, len(mirrors)),
		lastErr:         make([]string, len(mirrors)),
		repairing:       map[string]bool{},
		pending:         make([]map[string]bool, len(mirrors)),
		pendingOverflow: make([]bool, len(mirrors)),
		wholeAtDown:     make([]bool, len(mirrors)),
		verify:          make([]bool, len(mirrors)),
		prim:            0,
		stop:            make(chan struct{}),
		done:            make(chan struct{}),
		repairOwner:     fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano()),
	}
	for i := range m.healthy {
		m.healthy[i] = true
	}
	for _, opt := range opts {
		opt(m)
	}
	// A real repository serves immediately. Alignment is the repair loop's
	// job: it runs in the background under a cross-process lease, so four
	// svngod instances no longer each block startup on a full scan (and no
	// longer hammer S3 with four simultaneous listings). Reads prefer the
	// primary; writes are fenced until a failover target has caught up, so
	// serving before alignment cannot publish a stale revision.
	//
	// Unit-test/custom stores (no repository prefix) keep the deterministic
	// blocking alignment they assert: a test sees a fully replicated store as
	// soon as NewMirror returns.
	if m.lazy {
		// Lazy startup trusts no replica until a full pass has verified it.
		// Marking every mirror healthy and relying on the HEAD alone let a new
		// or rebuilt replica become "healthy" as soon as `current` was copied,
		// while data objects were still missing; upToDate then skipped the rest
		// of the repair. verify forces the first background pass to complete.
		m.mu.Lock()
		for i := range m.verify {
			m.verify[i] = true
		}
		m.mu.Unlock()
	}
	if !m.lazy {
		reachable := make([]bool, len(m.mirrors))
		var probeWG sync.WaitGroup
		for i, s := range m.mirrors {
			probeWG.Add(1)
			go func(i int, s Store) {
				defer probeWG.Done()
				ctx, cancel := context.WithTimeout(context.Background(), startupProbeTimeout)
				defer cancel()
				reachable[i] = m.probeReachable(ctx, i) == nil
			}(i, s)
		}
		probeWG.Wait()
		allReachable := true
		for i := range m.mirrors {
			if !reachable[i] {
				allReachable = false
			}
		}
		if allReachable {
			m.repairInternal()
		}
	}
	go m.repairLoop()
	go m.healthLoop()
	if m.anyDown() {
		m.mu.Lock()
		m.recovering = true
		m.mu.Unlock()
		go m.recoveryLoop()
	}
	if m.lazy {
		go m.repair()
	}
	return m
}

// Close stops the background repair loop.
// Mirrors exposes the member stores (GeoStore ships directly to remote
// members; see geostore.go).
func (m *MirrorStore) Mirrors() []Store { return m.mirrors }

func (m *MirrorStore) Close() {
	select {
	case <-m.stop:
		return
	default:
	}
	close(m.stop)
	<-m.done
}

// --- control-plane key detection ---
//
// MirrorStore lives in the store package next to keys.go, so it can tell the
// mutable/control objects (which must be served by the primary for strong
// consistency) apart from immutable data objects (safe to load-balance).
func isControlKey(key string) bool {
	base := key[strings.LastIndex(key, "/")+1:]
	switch {
	case base == "current", base == "format", base == "uuid", base == "locks.json":
		// locks.json is the mutable lock table (dav-locks/locks.json).
		return true
	case strings.Contains(key, "/locks/"), strings.Contains(key, "/dav-activities/"):
		return true
	case strings.Contains(key, "/dav-txns/") && !strings.Contains(key, "/dav-txns/data/"):
		// Session manifests are rewritten on every PUT, so a standby may hold
		// an older copy; start-up recovery must read the primary. The bodies
		// under dav-txns/data are content addressed and safe anywhere.
		return true
	}
	return false
}

// SetControlKey names the repository HEAD object so failover can compare the
// standby's copy with the last acknowledged one. Call after the repo is open.
func (m *MirrorStore) SetControlKey(key string) {
	m.mu.Lock()
	m.controlKey = key
	m.mu.Unlock()
	if key == "" {
		return
	}
	// For a two-node pair the arbiter (mirror 0) is authoritative: every
	// commit publishes the revision and the HEAD pointer there first, so its
	// current value is safe to adopt even when the standby is unreachable.
	// The old majority rule fenced a cold start with the standby down, which
	// turned a routine restart into a commit outage. With three or more nodes
	// (or when the arbiter itself is unreachable) fall back to the majority
	// agreement below.
	if m.ArbiterRequired() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		b, err := m.mirrors[0].Get(ctx, key, nil)
		cancel()
		if err == nil {
			m.noteAcked(b)
			return
		}
	}
	// Adopt the HEAD only when a majority of replicas agree. A cold start
	// while too few mirrors are reachable cannot know the true acknowledged
	// HEAD; adopting the survivor's stale pointer would allow a commit at an
	// old revision number and overwrite a different revision's objects
	// (observed at 800k objects). Fence until a majority agrees.
	var agreed []byte
	reachable := 0
	mismatch := false
	for _, s := range m.mirrors {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		b, err := s.Get(ctx, key, nil)
		cancel()
		if err != nil {
			continue
		}
		reachable++
		if agreed == nil {
			agreed = append([]byte(nil), b...)
			continue
		}
		if ContentETag(agreed) != ContentETag(b) {
			mismatch = true
		}
	}
	quorum := len(m.mirrors)/2 + 1
	if agreed == nil || reachable < quorum || mismatch {
		m.mu.Lock()
		m.fenced = true
		m.mu.Unlock()
		return
	}
	m.noteAcked(agreed)
}

// writePrimary runs a write on the elected primary; on a transport failure it
// marks that mirror down and retries on the next healthy one, so a PUT or
// commit does not fail just because the elected primary went away between the
// read and the write. Precondition/missing-object results are not retried.
func (m *MirrorStore) writePrimary(fn func(Store) error) (int, error) {
	p, pi := m.primary()
	cerr := m.checkCaughtUp(p, pi)
	if errors.Is(cerr, ErrReplicaLagging) {
		return pi, cerr
	}
	err := cerr
	if err == nil {
		err = fn(p)
	}
	if err == nil {
		m.noteWrite(pi)
		return pi, nil
	}
	if errors.Is(err, ErrPreconditionFailed) || errors.Is(err, ErrNotFound) {
		return pi, err
	}
	m.markDown(pi)
	n := len(m.mirrors)
	for step := 1; step < n; step++ {
		idx := (pi + step) % n
		if !m.isHealthy(idx) {
			continue
		}
		if e := fn(m.mirrors[idx]); e == nil {
			m.noteWrite(idx)
			return idx, nil
		} else if errors.Is(e, ErrPreconditionFailed) || errors.Is(e, ErrNotFound) {
			return idx, e
		} else {
			m.markDown(idx)
			m.noteError(idx, e)
			err = e
		}
	}
	return pi, err
}

// --- Store interface ---

func (m *MirrorStore) Put(ctx context.Context, key string, data []byte) error {
	if isRevisionKey(key) {
		return m.putRevision(ctx, key, data)
	}
	pi, err := m.writePrimary(func(s Store) error { return s.Put(ctx, key, data) })
	if err != nil {
		return err
	}
	if key == m.controlName() {
		m.noteAcked(data)
	}
	if q := m.writeQuorum; q > 1 {
		return m.quorumMirror(ctx, pi, q, key, true, func(ctx context.Context, s Store) error { return s.Put(ctx, key, data) })
	}
	// Best-effort mirror to healthy standbys.
	m.mirrorWrites(ctx, pi, key, true, func(ctx context.Context, s Store) error { return s.Put(ctx, key, data) })
	return nil
}

// putRevision writes a revision object in configuration order, so a
// two-node pair always has it on the arbiter (the replica that holds the
// commit lease and the HEAD pointer) before any standby. A commit that cannot
// reach the arbiter fails instead of publishing a revision only on a standby,
// which used to let a retry write different bytes for the same revision on
// each replica.
func (m *MirrorStore) putRevision(ctx context.Context, key string, data []byte) error {
	// A replica that missed commits must not publish, even when this process
	// marked the arbiter down: the elected standby has to prove it reached
	// the acknowledged HEAD first (the original fencing rule).
	p, pi := m.primary()
	if err := m.checkCaughtUp(p, pi); err != nil {
		return err
	}
	arbiter := m.ArbiterRequired()
	q := m.RegisterQuorum()
	wrote := 0
	for i, s := range m.mirrors {
		if i == 0 {
			if !arbiter && !m.isHealthy(0) {
				continue
			}
		} else if !m.isHealthy(i) {
			continue
		}
		c, cancel := context.WithTimeout(ctx, quorumWriteTimeout)
		err := s.Put(c, key, data)
		cancel()
		if err != nil {
			m.markDown(i)
			m.noteError(i, err)
			m.skipWrite(i, key)
			if i == 0 && arbiter {
				// The arbiter is authoritative for revisions; a standby copy
				// alone must not become visible.
				return fmt.Errorf("%w: arbiter unavailable: %v", ErrQuorumUnmet, err)
			}
			continue
		}
		m.noteWrite(i)
		wrote++
	}
	if wrote < q {
		return fmt.Errorf("%w: %d of %d copies (need %d)", ErrQuorumUnmet, wrote, len(m.mirrors), q)
	}
	return nil
}

func (m *MirrorStore) PutReconstructible(ctx context.Context, key string, data []byte) error {
	pi, err := m.writePrimary(func(s Store) error { return PutReconstructible(ctx, s, key, data) })
	if err != nil {
		return err
	}
	if q := m.writeQuorum; q > 1 {
		return m.quorumMirror(ctx, pi, q, key, true, func(ctx context.Context, s Store) error {
			return PutReconstructible(ctx, s, key, data)
		})
	}
	m.mirrorWrites(ctx, pi, key, true, func(ctx context.Context, s Store) error { return PutReconstructible(ctx, s, key, data) })
	return nil
}

func (m *MirrorStore) PutFromReconstructible(ctx context.Context, key string, r io.Reader, size int64) error {
	tmp, err := os.CreateTemp("", fmt.Sprintf("svngod-mirror-fast-%d-", os.Getpid()))
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := io.Copy(tmp, io.LimitReader(r, size)); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	pi, err := m.writePrimary(func(s Store) error {
		f, oerr := os.Open(tmpName)
		if oerr != nil {
			return oerr
		}
		defer f.Close()
		return PutFromReconstructible(ctx, s, key, f, size)
	})
	if err != nil {
		return err
	}
	fn := func(ctx context.Context, s Store) error {
		f, oerr := os.Open(tmpName)
		if oerr != nil {
			return oerr
		}
		defer f.Close()
		return PutFromReconstructible(ctx, s, key, f, size)
	}
	if q := m.writeQuorum; q > 1 {
		return m.quorumMirror(ctx, pi, q, key, true, fn)
	}
	m.mirrorWrites(ctx, pi, key, true, fn)
	return nil
}

// SyncAll flushes the primary first (the copy that must be durable before the
// commit is acknowledged), then the healthy standbys in parallel.
func (m *MirrorStore) SyncAll(ctx context.Context) error {
	p, pi := m.primary()
	if err := SyncAll(ctx, p); err != nil {
		m.markDown(pi)
		return err
	}
	m.mirrorWrites(ctx, pi, "", false, func(ctx context.Context, s Store) error { return SyncAll(ctx, s) })
	return nil
}

func (m *MirrorStore) Get(ctx context.Context, key string, rng *Range) ([]byte, error) {
	m.mu.Lock()
	fenced := m.fenced && key == m.controlKey && key != ""
	ack := append([]byte(nil), m.ackedCurrent...)
	m.mu.Unlock()
	if fenced && len(ack) > 0 && rng == nil {
		// Never expose a HEAD older than the last one this process
		// acknowledged, but do not hide a newer one either: after an outage the
		// other replica (or another svngod process) may be ahead, and returning
		// the local ack forever made a healed cluster read a stale revision
		// (observed 52 while both nodes held 53). Take the newest reachable
		// value; without an order function fall back to the acknowledged copy.
		if m.mutableOrder == nil {
			return ack, nil
		}
		best := ack
		for _, s := range m.mirrors {
			c, cancel := context.WithTimeout(ctx, 2*time.Second)
			b, err := s.Get(c, key, nil)
			cancel()
			if err == nil && m.mutableOrder(key, b, best) > 0 {
				best = b
			}
		}
		return best, nil
	}
	// Read the primary first: it is the only copy guaranteed to have every
	// object written so far (a standby write is best effort and may still be
	// in flight). Fall back to standbys without marking them down when they
	// simply do not have the object yet.
	p, pi := m.primary()
	pc, pcancel := context.WithTimeout(ctx, primaryReadTimeout)
	b, err := p.Get(pc, key, rng)
	pcancel()
	if err == nil {
		return b, nil
	}
	if isControlKey(key) && errors.Is(err, ErrNotFound) {
		// The primary is authoritative for mutable state: a standby copy is
		// either the same or stale, so do not fall back to it.
		return nil, err
	}
	if !errors.Is(err, ErrNotFound) {
		m.markDownHung(pi)
	}
	n := len(m.mirrors)
	for step := 1; step < n; step++ {
		idx := (pi + step) % n
		if !m.isHealthy(idx) {
			continue
		}
		c, cancel := context.WithTimeout(ctx, standbyReadTimeout)
		bb, e := m.mirrors[idx].Get(c, key, rng)
		cancel()
		if e == nil {
			if rng == nil && !isControlKey(key) {
				go m.repairKey(idx, key)
			}
			return bb, nil
		}
		if !errors.Is(e, ErrNotFound) {
			m.markDownHung(idx)
		}
	}
	return nil, err
}

// PutFrom writes the stream to the primary and best-effort to standbys. The
// reader is buffered through a temp file because a standby needs the bytes a
// second time.
func (m *MirrorStore) PutFrom(ctx context.Context, key string, r io.Reader, size int64) error {
	tmp, err := os.CreateTemp("", fmt.Sprintf("svngod-mirror-%d-", os.Getpid()))
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := io.Copy(tmp, io.LimitReader(r, size)); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	pi, err := m.writePrimary(func(s Store) error {
		f, oerr := os.Open(tmpName)
		if oerr != nil {
			return oerr
		}
		defer f.Close()
		return s.PutFrom(ctx, key, f, size)
	})
	if err != nil {
		return err
	}
	fn := func(ctx context.Context, s Store) error {
		f, oerr := os.Open(tmpName)
		if oerr != nil {
			return oerr
		}
		defer f.Close()
		return s.PutFrom(ctx, key, f, size)
	}
	if q := m.writeQuorum; q > 1 {
		return m.quorumMirror(ctx, pi, q, key, true, fn)
	}
	m.mirrorWrites(ctx, pi, key, true, fn)
	return nil
}

// Exists asks the primary first, then any healthy standby.
func (m *MirrorStore) Exists(ctx context.Context, key string) (bool, error) {
	if q := m.writeQuorum; q > 1 && !isControlKey(key) {
		// A deduplicated object only counts as present when enough replicas
		// hold it. Reporting false makes the caller rewrite it through the
		// quorum path, which repairs the gap.
		n := 0
		for _, s := range m.mirrors {
			c, cancel := context.WithTimeout(ctx, 3*time.Second)
			ok, err := s.Exists(c, key)
			cancel()
			if err == nil && ok {
				n++
			}
		}
		return n >= q, nil
	}
	p, pi := m.primary()
	pc, pcancel := context.WithTimeout(ctx, primaryReadTimeout)
	ok, err := p.Exists(pc, key)
	pcancel()
	if err == nil && ok {
		return true, nil
	}
	if isControlKey(key) && err == nil {
		return false, nil
	}
	if err != nil && !errors.Is(err, ErrNotFound) {
		m.markDownHung(pi)
	}
	n := len(m.mirrors)
	for step := 1; step < n; step++ {
		idx := (pi + step) % n
		if !m.isHealthy(idx) {
			continue
		}
		c, cancel := context.WithTimeout(ctx, standbyReadTimeout)
		ok, e := m.mirrors[idx].Exists(c, key)
		cancel()
		if e == nil && ok {
			return true, nil
		}
		if e != nil && !errors.Is(e, ErrNotFound) {
			m.markDownHung(idx)
		}
	}
	return false, err
}

// GetTo streams an object from the primary, falling back only when the
// primary wrote nothing yet (retrying after a partial write would duplicate
// bytes in w).
func (m *MirrorStore) GetTo(ctx context.Context, key string, w io.Writer) (int64, error) {
	p, pi := m.primary()
	n, err := p.GetTo(ctx, key, w)
	if err == nil {
		return n, nil
	}
	if n > 0 {
		return n, err
	}
	if isControlKey(key) && errors.Is(err, ErrNotFound) {
		return 0, err
	}
	if !errors.Is(err, ErrNotFound) {
		m.markDown(pi)
	}
	total := len(m.mirrors)
	for step := 1; step < total; step++ {
		idx := (pi + step) % total
		if !m.isHealthy(idx) {
			continue
		}
		got, e := m.mirrors[idx].GetTo(ctx, key, w)
		if e == nil {
			return got, nil
		}
		if got > 0 {
			return got, e
		}
		if !errors.Is(e, ErrNotFound) {
			m.markDown(idx)
		}
	}
	return n, err
}

// Head asks the primary first, falling back to standbys.
func (m *MirrorStore) Head(ctx context.Context, key string) (string, bool, error) {
	p, pi := m.primary()
	etag, ok, err := p.Head(ctx, key)
	if err == nil && ok {
		return etag, ok, nil
	}
	if isControlKey(key) && err == nil {
		return etag, false, nil
	}
	if err != nil && !errors.Is(err, ErrNotFound) {
		m.markDown(pi)
	}
	n := len(m.mirrors)
	for step := 1; step < n; step++ {
		idx := (pi + step) % n
		if !m.isHealthy(idx) {
			continue
		}
		etag, ok, e := m.mirrors[idx].Head(ctx, key)
		if e == nil && ok {
			return etag, ok, nil
		}
		if e != nil && !errors.Is(e, ErrNotFound) {
			m.markDown(idx)
		}
	}
	return etag, ok, err
}

// CreateIfAbsent creates the object on the primary (the only place the lock
// must be atomic) and overwrites it on standbys: a standby that already holds
// the same lock object must not be treated as a failure.
func (m *MirrorStore) CreateIfAbsent(ctx context.Context, key string, data []byte) error {
	pi, err := m.writePrimary(func(s Store) error { return s.CreateIfAbsent(ctx, key, data) })
	if err != nil {
		return err
	}
	m.mirrorWrites(ctx, pi, key, true, func(ctx context.Context, s Store) error { return s.Put(ctx, key, data) })
	return nil
}

func (m *MirrorStore) CAS(ctx context.Context, key string, expectETag string, data []byte) (string, error) {
	if key == m.controlName() {
		// HEAD publication must agree with the commit lease: on a two-node
		// pair the arbiter holds both, so a CAS based on a stale standby value
		// is refused. The old "elected primary + unconditional mirror" path
		// let a retry publish the same revision number on a different replica.
		if err := m.CASQuorum(ctx, key, expectETag, data, m.RegisterQuorum()); err != nil {
			return "", err
		}
		m.noteAcked(data)
		m.mirrorWrites(ctx, 0, key, true, func(ctx context.Context, s Store) error { return s.Put(ctx, key, data) })
		return ContentETag(data), nil
	}
	var newEtag string
	pi, err := m.writePrimary(func(s Store) error {
		var cerr error
		newEtag, cerr = s.CAS(ctx, key, expectETag, data)
		return cerr
	})
	if err != nil {
		return "", err
	}
	// Standbys mirror the current value unconditionally (their own etag does
	// not matter; they only serve reads after failover).
	m.mirrorWrites(ctx, pi, key, true, func(ctx context.Context, s Store) error { return s.Put(ctx, key, data) })
	return newEtag, nil
}

func (m *MirrorStore) controlName() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.controlKey
}

func (m *MirrorStore) ListPrefix(ctx context.Context, prefix, continuation string, max int) ([]ListedObj, string, error) {
	p, pi := m.primary()
	objs, next, err := p.ListPrefix(ctx, prefix, continuation, max)
	if err != nil {
		m.markDown(pi)
		return nil, "", err
	}
	return objs, next, nil
}

func (m *MirrorStore) DeleteIfMatch(ctx context.Context, key, etag string) error {
	pi, err := m.writePrimary(func(s Store) error { return DeleteIfMatch(ctx, s, key, etag) })
	if err != nil {
		return err
	}
	m.mirrorWrites(ctx, pi, key, false, func(ctx context.Context, s Store) error { return DeleteIfMatch(ctx, s, key, etag) })
	return nil
}

func (m *MirrorStore) Delete(ctx context.Context, keys []string) error {
	pi, err := m.writePrimary(func(s Store) error { return s.Delete(ctx, keys) })
	if err != nil {
		return err
	}
	m.mirrorWrites(ctx, pi, "", false, func(ctx context.Context, s Store) error { return s.Delete(ctx, keys) })
	return nil
}

// DeletePrimary removes keys from the arbiter only. Bulk cleanup of loose
// objects uses it: a packed shard's loose copies on a standby are harmless
// (repair ignores them), while mirroring thousands of deletes knocked a live
// standby out of service when a 1000-key DELETE exceeded the standby budget.
func (m *MirrorStore) DeletePrimary(ctx context.Context, keys []string) error {
	p, pi := m.primary()
	if err := p.Delete(ctx, keys); err != nil {
		m.markDown(pi)
		return err
	}
	return nil
}

// DeletePrefixPrimary drops a prefix on the elected primary only.
func (m *MirrorStore) DeletePrefixPrimary(ctx context.Context, prefix string) error {
	p, pi := m.primary()
	if err := DeletePrefix(ctx, p, prefix); err != nil {
		m.markDown(pi)
		return err
	}
	return nil
}

// DeletePrefix drops a whole prefix on the primary and on healthy standbys.
func (m *MirrorStore) DeletePrefix(ctx context.Context, prefix string) error {
	p, pi := m.primary()
	if err := DeletePrefix(ctx, p, prefix); err != nil {
		m.markDown(pi)
		return err
	}
	m.mirrorWrites(ctx, pi, "", false, func(ctx context.Context, s Store) error { return DeletePrefix(ctx, s, prefix) })
	return nil
}

// --- self-healing repair ---

// --- quorum register (commit lease) -----------------------------------------
