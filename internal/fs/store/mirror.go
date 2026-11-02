package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
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
}

// ReplicaStatus is a snapshot of one mirror's health.
type ReplicaStatus struct {
	Index       int
	Healthy     bool
	Primary     bool
	LastSuccess time.Time
	LastError   string
}

// Status returns a snapshot of every mirror's health and last successful
// write, so a scrape can show replication lag.
func (m *MirrorStore) Status() []ReplicaStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]ReplicaStatus, len(m.mirrors))
	for i := range m.mirrors {
		out[i] = ReplicaStatus{Index: i, Healthy: m.healthy[i], Primary: i == m.prim}
		if i < len(m.lastSuccess) {
			out[i].LastSuccess = m.lastSuccess[i]
			out[i].LastError = m.lastErr[i]
		}
	}
	return out
}

func (m *MirrorStore) noteWrite(i int) {
	m.mu.Lock()
	if i < len(m.lastSuccess) {
		m.lastSuccess[i] = time.Now()
		m.lastErr[i] = ""
	}
	m.mu.Unlock()
}

func (m *MirrorStore) noteError(i int, err error) {
	m.mu.Lock()
	if i < len(m.lastErr) && err != nil {
		m.lastErr[i] = err.Error()
	}
	m.mu.Unlock()
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

// WithWriteQuorum requires every write to reach n copies before it returns.
// n <= 1 keeps the best-effort standby behaviour.
func WithWriteQuorum(n int) MirrorOption {
	return func(m *MirrorStore) { m.writeQuorum = n }
}

// WriteQuorum reports the configured number of copies (0 = best effort).
func (m *MirrorStore) WriteQuorum() int { return m.writeQuorum }

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

// NewMirror builds a MirrorStore over the given backends, aligns them, and
// starts the background repair loop.
func NewMirror(mirrors []Store, opts ...MirrorOption) *MirrorStore {
	if len(mirrors) < 2 {
		panic("MirrorStore requires at least 2 mirrors")
	}
	m := &MirrorStore{
		mirrors:     mirrors,
		healthy:     make([]bool, len(mirrors)),
		lastSuccess: make([]time.Time, len(mirrors)),
		lastErr:     make([]string, len(mirrors)),
		repairing:   map[string]bool{},
		prim:        0,
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
	}
	for i := range m.healthy {
		m.healthy[i] = true
	}
	for _, opt := range opts {
		opt(m)
	}
	// Align the mirrors before serving: a mirror that comes back after an
	// outage may be missing objects or (worse) hold an older HEAD pointer, and
	// electing it before it is caught up would hide committed revisions.
	m.repairInternal()
	go m.repairLoop()
	if m.anyDown() {
		m.mu.Lock()
		m.recovering = true
		m.mu.Unlock()
		go m.recoveryLoop()
	}
	return m
}

// Close stops the background repair loop.
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

// isHealthy reports whether mirror i is currently online.
func (m *MirrorStore) isHealthy(i int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.healthy[i]
}

func (m *MirrorStore) setHealthy(i int, ok bool) {
	m.mu.Lock()
	m.healthy[i] = ok
	if !ok {
		m.repairable = true
	}
	m.mu.Unlock()
}

// primary returns the current primary store. The elected mirror stays primary
// while it is healthy, so a mirror that just finished catching up never starts
// serving ahead of the one that has been taking writes.
func (m *MirrorStore) primary() (Store, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.healthy[m.prim] {
		return m.mirrors[m.prim], m.prim
	}
	for i, h := range m.healthy {
		if h {
			m.prim = i
			return m.mirrors[i], i
		}
	}
	// All mirrors down: fall back to the original primary so the caller gets
	// an error from it rather than nil.
	m.prim = 0
	return m.mirrors[0], 0
}

// markDown records a mirror as failed so it is excluded from service. Losing
// the current primary fences writes until the newly elected standby proves it
// has the acknowledged HEAD.
func (m *MirrorStore) markDown(i int) {
	m.mu.Lock()
	wasHealthy := m.healthy[i]
	m.healthy[i] = false
	m.repairable = true
	if i == m.prim {
		m.fenced = true
	}
	start := wasHealthy && !m.recovering
	if start {
		m.recovering = true
	}
	m.mu.Unlock()
	if start {
		go m.recoveryLoop()
	}
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
	// Adopt the HEAD only when every reachable mirror agrees. NewMirror's
	// initial repair has already reconciled them (using mutableOrder), so a
	// disagreement here means the state is genuinely uncertain and must not
	// be used as an RPO baseline.
	var agreed []byte
	for _, s := range m.mirrors {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		b, err := s.Get(ctx, key, nil)
		cancel()
		if err != nil {
			continue
		}
		if agreed == nil {
			agreed = append([]byte(nil), b...)
			continue
		}
		if ContentETag(agreed) != ContentETag(b) {
			return
		}
	}
	if len(agreed) > 0 {
		m.noteAcked(agreed)
	}
}

func (m *MirrorStore) noteAcked(b []byte) {
	m.mu.Lock()
	m.ackedCurrent = append([]byte(nil), b...)
	m.fenced = false
	m.mu.Unlock()
}

func (m *MirrorStore) ackedCopy() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]byte(nil), m.ackedCurrent...)
}

// checkCaughtUp refuses writes on a standby whose HEAD differs from the last
// acknowledged one, and asks the repair loop to catch it up.
func (m *MirrorStore) checkCaughtUp(p Store, pi int) error {
	m.mu.Lock()
	key, want, fenced := m.controlKey, append([]byte(nil), m.ackedCurrent...), m.fenced
	m.mu.Unlock()
	if !fenced || key == "" || len(want) == 0 {
		return nil
	}
	// A recovering mirror is usually only seconds behind. Failing the write on
	// the first mismatch turns a failover into a client-visible commit
	// conflict, so poll while a repair pass copies the missing objects.
	wantETag := ContentETag(want)
	go m.repair()
	deadline := time.Now().Add(8 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		got, err := p.Get(ctx, key, nil)
		cancel()
		if err == nil && ContentETag(got) == wantETag {
			m.mu.Lock()
			m.fenced = false
			m.mu.Unlock()
			return nil
		}
		if err != nil && !errors.Is(err, ErrNotFound) {
			// The elected primary is unreachable, not merely behind: let the
			// caller fail over instead of waiting out the poll deadline.
			m.markDown(pi)
			return err
		}
		if time.Now().After(deadline) {
			return ErrReplicaLagging
		}
		time.Sleep(200 * time.Millisecond)
	}
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
	pi, err := m.writePrimary(func(s Store) error { return s.Put(ctx, key, data) })
	if err != nil {
		return err
	}
	if key == m.controlName() {
		m.noteAcked(data)
	}
	if q := m.writeQuorum; q > 1 {
		return m.quorumMirror(ctx, pi, q, func(ctx context.Context, s Store) error { return s.Put(ctx, key, data) })
	}
	// Best-effort mirror to healthy standbys.
	m.mirrorWrites(ctx, pi, func(ctx context.Context, s Store) error { return s.Put(ctx, key, data) })
	return nil
}

func (m *MirrorStore) PutReconstructible(ctx context.Context, key string, data []byte) error {
	pi, err := m.writePrimary(func(s Store) error { return PutReconstructible(ctx, s, key, data) })
	if err != nil {
		return err
	}
	if q := m.writeQuorum; q > 1 {
		return m.quorumMirror(ctx, pi, q, func(ctx context.Context, s Store) error {
			return PutReconstructible(ctx, s, key, data)
		})
	}
	m.mirrorWrites(ctx, pi, func(ctx context.Context, s Store) error { return PutReconstructible(ctx, s, key, data) })
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
		return m.quorumMirror(ctx, pi, q, fn)
	}
	m.mirrorWrites(ctx, pi, fn)
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
	m.mirrorWrites(ctx, pi, func(ctx context.Context, s Store) error { return SyncAll(ctx, s) })
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
	b, err := p.Get(ctx, key, rng)
	if err == nil {
		return b, nil
	}
	if isControlKey(key) && errors.Is(err, ErrNotFound) {
		// The primary is authoritative for mutable state: a standby copy is
		// either the same or stale, so do not fall back to it.
		return nil, err
	}
	if !errors.Is(err, ErrNotFound) {
		m.markDown(pi)
	}
	n := len(m.mirrors)
	for step := 1; step < n; step++ {
		idx := (pi + step) % n
		if !m.isHealthy(idx) {
			continue
		}
		bb, e := m.mirrors[idx].Get(ctx, key, rng)
		if e == nil {
			if rng == nil && !isControlKey(key) {
				go m.repairKey(idx, key)
			}
			return bb, nil
		}
		if !errors.Is(e, ErrNotFound) {
			m.markDown(idx)
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
		return m.quorumMirror(ctx, pi, q, fn)
	}
	m.mirrorWrites(ctx, pi, fn)
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
	ok, err := p.Exists(ctx, key)
	if err == nil && ok {
		return true, nil
	}
	if isControlKey(key) && err == nil {
		return false, nil
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
		ok, e := m.mirrors[idx].Exists(ctx, key)
		if e == nil && ok {
			return true, nil
		}
		if e != nil && !errors.Is(e, ErrNotFound) {
			m.markDown(idx)
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
	m.mirrorWrites(ctx, pi, func(ctx context.Context, s Store) error { return s.Put(ctx, key, data) })
	return nil
}

func (m *MirrorStore) CAS(ctx context.Context, key string, expectETag string, data []byte) (string, error) {
	var newEtag string
	pi, err := m.writePrimary(func(s Store) error {
		var cerr error
		newEtag, cerr = s.CAS(ctx, key, expectETag, data)
		return cerr
	})
	if err != nil {
		return "", err
	}
	if key == m.controlName() {
		m.noteAcked(data)
	}
	// Standbys mirror the current value unconditionally (their own etag does
	// not matter; they only serve reads after failover).
	m.mirrorWrites(ctx, pi, func(ctx context.Context, s Store) error { return s.Put(ctx, key, data) })
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
	m.mirrorWrites(ctx, pi, func(ctx context.Context, s Store) error { return DeleteIfMatch(ctx, s, key, etag) })
	return nil
}

func (m *MirrorStore) Delete(ctx context.Context, keys []string) error {
	pi, err := m.writePrimary(func(s Store) error { return s.Delete(ctx, keys) })
	if err != nil {
		return err
	}
	m.mirrorWrites(ctx, pi, func(ctx context.Context, s Store) error { return s.Delete(ctx, keys) })
	return nil
}

// DeletePrefix drops a whole prefix on the primary and on healthy standbys.
func (m *MirrorStore) DeletePrefix(ctx context.Context, prefix string) error {
	p, pi := m.primary()
	if err := DeletePrefix(ctx, p, prefix); err != nil {
		m.markDown(pi)
		return err
	}
	m.mirrorWrites(ctx, pi, func(ctx context.Context, s Store) error { return DeletePrefix(ctx, s, prefix) })
	return nil
}

// quorumWriteTimeout bounds one standby copy in quorum mode. It is larger
// than the best-effort budget because the copy is now required for the write
// to be acknowledged (a 100MiB body over a LAN takes a couple of seconds).
const quorumWriteTimeout = 15 * time.Second

// quorumMirror writes fn to the healthy standbys after the primary has the
// object, returning ErrQuorumUnmet unless at least quorum copies exist.
func (m *MirrorStore) quorumMirror(ctx context.Context, pi, quorum int, fn func(context.Context, Store) error) error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok := 0
	for i, s := range m.mirrors {
		if i == pi || !m.isHealthy(i) {
			continue
		}
		wg.Add(1)
		go func(i int, s Store) {
			defer wg.Done()
			wctx, cancel := context.WithTimeout(ctx, quorumWriteTimeout)
			defer cancel()
			if err := fn(wctx, s); err != nil {
				m.markDown(i)
				m.noteError(i, err)
				return
			}
			m.noteWrite(i)
			mu.Lock()
			ok++
			mu.Unlock()
		}(i, s)
	}
	wg.Wait()
	if 1+ok < quorum {
		return fmt.Errorf("%w: %d of %d copies (need %d)", ErrQuorumUnmet, 1+ok, len(m.mirrors), quorum)
	}
	return nil
}

// QuorumGet returns the value held by the largest number of mirrors and that
// count. It is used to replay the commit journal after a crash: a record only
// counts when a quorum holds the same bytes.
func (m *MirrorStore) QuorumGet(ctx context.Context, key string) ([]byte, int, error) {
	type cand struct {
		val []byte
		n   int
	}
	var best *cand
	for _, s := range m.mirrors {
		c, cancel := context.WithTimeout(ctx, quorumWriteTimeout)
		b, err := s.Get(c, key, nil)
		cancel()
		if err != nil {
			continue
		}
		if best == nil {
			best = &cand{val: b, n: 1}
			continue
		}
		if bytes.Equal(best.val, b) {
			best.n++
		}
	}
	if best == nil {
		return nil, 0, ErrNotFound
	}
	return best.val, best.n, nil
}

// standbyWriteTimeout bounds how long a commit waits for one standby. It is
// far above a healthy write (tens of ms locally) but short enough that a dead
// node costs a few seconds, not the S3 SDK's full retry budget.
const standbyWriteTimeout = 8 * time.Second

// mirrorWrites fans a standby write out to every healthy mirror in parallel:
// standbys are best effort, so their latency must not add to the request.
func (m *MirrorStore) mirrorWrites(ctx context.Context, pi int, fn func(context.Context, Store) error) {
	var wg sync.WaitGroup
	for i, s := range m.mirrors {
		if i == pi || !m.isHealthy(i) {
			continue
		}
		wg.Add(1)
		go func(i int, s Store) {
			defer wg.Done()
			// A standby that just died must not hold the commit for the S3
			// client's full retry budget. The primary already holds the data;
			// anything skipped here is copied by the periodic repair pass.
			wctx, cancel := context.WithTimeout(ctx, standbyWriteTimeout)
			defer cancel()
			if err := fn(wctx, s); err != nil {
				m.markDown(i)
				m.noteError(i, err)
				fmt.Printf("mirror[%d] standby write failed: %v\n", i, err)
			} else {
				m.noteWrite(i)
			}
		}(i, s)
	}
	wg.Wait()
}

// repairKey copies one object from the mirror that served it back to the
// others, so a standby that answered a read heals the primary immediately
// instead of waiting for the periodic full repair. Only full, reasonably
// small objects are repaired: a ranged read would copy a partial object and
// huge texts are left to the streaming repair pass.
func (m *MirrorStore) repairKey(src int, key string) {
	const maxRepairObject = 16 << 20
	m.mu.Lock()
	if m.repairing[key] {
		m.mu.Unlock()
		return
	}
	m.repairing[key] = true
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.repairing, key)
		m.mu.Unlock()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	b, err := m.mirrors[src].Get(ctx, key, &Range{Offset: 0, Length: maxRepairObject + 1})
	if err != nil || len(b) > maxRepairObject {
		return
	}
	for i := range m.mirrors {
		if i == src {
			continue
		}
		if err := m.mirrors[i].Put(ctx, key, b); err == nil {
			m.noteWrite(i)
		}
	}
}

// --- self-healing repair ---

func (m *MirrorStore) repairLoop() {
	defer close(m.done)
	t := time.NewTicker(60 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-t.C:
			m.repair()
		}
	}
}

// anyDown reports whether any mirror is currently out of service.
func (m *MirrorStore) anyDown() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, h := range m.healthy {
		if !h {
			return true
		}
	}
	return false
}

// recoveryLoop retries repair while a mirror is down. The periodic pass runs
// every 60s, which is too slow to restore redundancy after a short restart; a
// degraded mirror is repaired every few seconds until it is whole again.
func (m *MirrorStore) recoveryLoop() {
	defer func() {
		m.mu.Lock()
		m.recovering = false
		m.mu.Unlock()
	}()
	for {
		select {
		case <-m.stop:
			return
		default:
		}
		if !m.anyDown() {
			return
		}
		m.repair()
		delay := 3 * time.Second
		if !m.anyReachable() {
			delay = 10 * time.Second
		}
		select {
		case <-m.stop:
			return
		case <-time.After(delay):
		}
	}
}

// anyReachable reports whether at least one mirror is in service; when all are
// down there is nothing useful for a repair pass to copy.
func (m *MirrorStore) anyReachable() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, h := range m.healthy {
		if h {
			return true
		}
	}
	return false
}

// RepairNow runs one repair pass immediately and synchronously (used on
// startup / when a mirror is marked down). It must not silently skip because a
// background repair holds the lock: callers expect the pass to have finished.
func (m *MirrorStore) RepairNow() { m.repairInternal() }

func (m *MirrorStore) repair() {
	if !m.repairOnce.TryLock() {
		return
	}
	defer m.repairOnce.Unlock()
	m.repairInternal()
}

// repairInternal is one repair pass; callers coordinate with repairOnce.
//
// It is deliberately symmetric: every reachable mirror is treated as a source
// for objects the others lack. Electing one "primary" as the only source was
// unsafe -- after a failover the returning ex-primary is stale, and copying
// from it could hide revisions committed on the standby (and overwrite the
// newer HEAD with its old one).
func (m *MirrorStore) repairInternal() {
	// List every mirror in parallel with its own deadline: one unreachable
	// backend must not consume the budget of the others (which previously left
	// all mirrors marked down just because the primary was slow to fail).
	n := len(m.mirrors)
	sets := make([]map[string]bool, n)
	reachable := make([]bool, n)
	var wg sync.WaitGroup
	for i := range m.mirrors {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), repairTimeout)
			defer cancel()
			ks, err := listAll(ctx, m.mirrors[i])
			if err != nil {
				log.Printf("mirror[%d] unreachable during repair: %v", i, err)
				return
			}
			reachable[i] = true
			set := make(map[string]bool, len(ks))
			for _, k := range ks {
				set[k] = true
			}
			sets[i] = set
		}(i)
	}
	wg.Wait()

	keys := make(map[string]bool)
	for i := range sets {
		for k := range sets[i] {
			keys[k] = true
		}
	}
	// A mirror with every object is complete; anything missing keeps it out of
	// service until the background loop has copied the data (a healed mirror
	// must not serve reads for objects it lacks).
	// missing counts the objects each reachable mirror still lacks; a mirror
	// is healthy only when that reaches zero (or every copy attempt failed in
	// a way we could see). Counting is stricter than "it was incomplete but no
	// put errored": a key no reachable mirror can serve must keep the mirror
	// out of service.
	missing := make([]int, n)
	for i := range m.mirrors {
		if reachable[i] {
			missing[i] = len(keys) - len(sets[i])
		}
	}
	failed := make([]bool, n)

	ctx, cancel := context.WithTimeout(context.Background(), repairTimeout)
	defer cancel()
	for k := range keys {
		if isMutableKey(k) {
			m.reconcileMutable(ctx, k, sets, reachable)
			continue
		}
		var val []byte
		for i := range m.mirrors {
			if !reachable[i] || !sets[i][k] {
				continue
			}
			b, err := m.mirrors[i].Get(ctx, k, nil)
			if err != nil {
				continue
			}
			val = b
			break
		}
		if val == nil {
			continue
		}
		for i := range m.mirrors {
			if !reachable[i] || sets[i][k] {
				continue
			}
			if ctx.Err() != nil {
				failed[i] = true
				continue
			}
			if err := m.mirrors[i].Put(ctx, k, val); err != nil {
				log.Printf("mirror[%d] repair put %q: %v", i, k, err)
				failed[i] = true
			} else {
				missing[i]--
			}
		}
	}

	for i := range m.mirrors {
		m.setHealthy(i, reachable[i] && !failed[i] && missing[i] <= 0)
	}
}

// reconcileMutable picks the newest value of a mutable key and writes it to
// every reachable mirror that is missing it or holds an older one.
func (m *MirrorStore) reconcileMutable(ctx context.Context, k string, sets []map[string]bool, reachable []bool) {
	var best []byte
	for i := range m.mirrors {
		if !reachable[i] || !sets[i][k] {
			continue
		}
		b, err := m.mirrors[i].Get(ctx, k, nil)
		if err != nil {
			continue
		}
		if best == nil || m.order(k, b, best) > 0 {
			best = b
		}
	}
	if best == nil {
		return
	}
	for i := range m.mirrors {
		if !reachable[i] {
			continue
		}
		if sets[i][k] {
			cur, err := m.mirrors[i].Get(ctx, k, nil)
			if err == nil && m.order(k, best, cur) <= 0 {
				continue // already the newest
			}
		}
		if err := m.mirrors[i].Put(ctx, k, best); err != nil {
			log.Printf("mirror[%d] repair put %q: %v", i, k, err)
			reachable[i] = false
		}
	}
}

// repairTimeout bounds one repair pass over all mirrors.
const repairTimeout = 5 * time.Second

// isMutableKey reports whether an object's content must always be mirrored
// from the primary (the only mutable object today is the HEAD `current`).
func isMutableKey(key string) bool {
	return strings.HasSuffix(key, "/current")
}

func listAll(ctx context.Context, s Store) ([]string, error) {
	var keys []string
	next := ""
	for {
		objs, n, err := s.ListPrefix(ctx, "", next, 1000)
		if err != nil {
			return nil, err
		}
		for _, o := range objs {
			keys = append(keys, o.Key)
		}
		if n == "" {
			break
		}
		next = n
	}
	return keys, nil
}

// --- quorum register (commit lease) -----------------------------------------

// CreateIfAbsentQuorum creates the key on a quorum. Replicas are tried in
// configuration order: the first reachable node is the arbiter, so two
// processes cannot each win on a different node and then roll each other back
// (that livelocked concurrent commits). A node that already holds the key
// stops the attempt immediately.
func (m *MirrorStore) CreateIfAbsentQuorum(ctx context.Context, key string, data []byte, quorum int) error {
	etag := ContentETag(data)
	var created []int
	rollback := func() {
		for _, i := range created {
			c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = DeleteIfMatch(c, m.mirrors[i], key, etag)
			cancel()
		}
	}
	for i, s := range m.mirrors {
		c, cancel := context.WithTimeout(ctx, quorumWriteTimeout)
		err := s.CreateIfAbsent(c, key, data)
		cancel()
		switch {
		case err == nil:
			created = append(created, i)
			m.noteWrite(i)
			if len(created) >= quorum {
				return nil
			}
		case errors.Is(err, ErrPreconditionFailed):
			rollback()
			return ErrPreconditionFailed
		default:
			m.markDown(i)
			m.noteError(i, err)
		}
	}
	if len(created) < quorum {
		rollback()
		return fmt.Errorf("%w: %d of %d created (need %d)", ErrQuorumUnmet, len(created), len(m.mirrors), quorum)
	}
	return nil
}

// CASQuorum applies the CAS in configuration order and requires quorum
// successes. A mismatch is terminal (the register diverged); successes are
// rolled back so the register is never half-updated.
func (m *MirrorStore) CASQuorum(ctx context.Context, key, expect string, data []byte, quorum int) error {
	etag := ContentETag(data)
	var done []int
	rollback := func() {
		for _, i := range done {
			c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = DeleteIfMatch(c, m.mirrors[i], key, etag)
			cancel()
		}
	}
	for i, s := range m.mirrors {
		c, cancel := context.WithTimeout(ctx, quorumWriteTimeout)
		_, err := s.CAS(c, key, expect, data)
		cancel()
		switch {
		case err == nil:
			done = append(done, i)
			m.noteWrite(i)
			if len(done) >= quorum {
				return nil
			}
		case errors.Is(err, ErrPreconditionFailed):
			rollback()
			return ErrPreconditionFailed
		default:
			m.markDown(i)
			m.noteError(i, err)
		}
	}
	if len(done) < quorum {
		rollback()
		return fmt.Errorf("%w: %d of %d cas (need %d)", ErrQuorumUnmet, len(done), len(m.mirrors), quorum)
	}
	return nil
}

// DeleteIfMatchQuorum deletes in configuration order and requires quorum
// successes (an already-gone replica counts as released).
func (m *MirrorStore) DeleteIfMatchQuorum(ctx context.Context, key, etag string, quorum int) error {
	ok := 0
	for i, s := range m.mirrors {
		c, cancel := context.WithTimeout(ctx, quorumWriteTimeout)
		err := DeleteIfMatch(c, s, key, etag)
		cancel()
		switch {
		case err == nil, errors.Is(err, ErrPreconditionFailed), errors.Is(err, ErrNotFound):
			ok++
			m.noteWrite(i)
			if ok >= quorum {
				return nil
			}
		default:
			m.markDown(i)
			m.noteError(i, err)
		}
	}
	if ok < quorum {
		return fmt.Errorf("%w: %d of %d deleted (need %d)", ErrQuorumUnmet, ok, len(m.mirrors), quorum)
	}
	return nil
}
