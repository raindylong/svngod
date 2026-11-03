package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
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

// WithRepoPrefix scopes repair and health accounting to one repository
// prefix (e.g. "repos/demo"). Required when several repositories share the
// same bucket or local directory.
func WithRepoPrefix(prefix string) MirrorOption {
	return func(m *MirrorStore) { m.repoPrefix = strings.Trim(prefix, "/") }
}

// WithWriteQuorum requires every write to reach n copies before it returns.
// n <= 1 keeps the best-effort standby behaviour.
func WithWriteQuorum(n int) MirrorOption {
	return func(m *MirrorStore) { m.writeQuorum = n }
}

// WriteQuorum reports the configured number of copies (0 = best effort).
func (m *MirrorStore) WriteQuorum() int { return m.writeQuorum }

// RegisterQuorum returns the number of copies a commit-lease operation must
// reach. A two-node pair uses the arbiter rule (one copy on the configured
// first mirror is safe because a second writer cannot commit without that
// mirror). Three or more nodes require a strict majority, so any two writers'
// quorums intersect and cannot both hold the lease.
func (m *MirrorStore) RegisterQuorum() int {
	m.mu.Lock()
	n := len(m.mirrors)
	m.mu.Unlock()
	if n <= 2 {
		return 1
	}
	return n/2 + 1
}

// ArbiterRequired reports whether register operations must always target the
// configured first mirror. Only the two-node pair needs that rule; with a
// majority quorum a health-view difference cannot produce two disjoint sets.
func (m *MirrorStore) ArbiterRequired() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.mirrors) <= 2
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

// NewMirror builds a MirrorStore over the given backends, aligns them, and
// starts the background repair loop.
// canonicalMirrors orders replicas by their identity. Every process with the
// same replica set therefore agrees on the arbiter/first writer even when its
// config file lists the replicas in a different order. Mixed order used to
// create two lease arbiters and publish the same revision with two different
// trees (observed in the split-order experiment). Stores without an identity
// keep the configured order (tests, custom backends).
func canonicalMirrors(mirrors []Store) []Store {
	type pair struct {
		id string
		s  Store
	}
	ps := make([]pair, len(mirrors))
	for i, s := range mirrors {
		id, ok := s.(interface{ Identity() string })
		if !ok {
			return mirrors
		}
		ps[i] = pair{id.Identity(), s}
	}
	sort.SliceStable(ps, func(i, j int) bool { return ps[i].id < ps[j].id })
	out := make([]Store, len(ps))
	for i, p := range ps {
		out[i] = p.s
	}
	return out
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
	// Probe first: a backend that cannot answer a trivial list within the
	// startup budget must not hold every repository's initialization for a
	// full repair timeout (with one S3 node down, a six-repo process used to
	// take ~90s before listening). It is marked down and the recovery loop
	// catches it up instead.
	reachable := make([]bool, len(m.mirrors))
	var probeWG sync.WaitGroup
	for i, s := range m.mirrors {
		probeWG.Add(1)
		go func(i int, s Store) {
			defer probeWG.Done()
			ctx, cancel := context.WithTimeout(context.Background(), startupProbeTimeout)
			defer cancel()
			_, _, err := s.ListPrefix(ctx, m.repoPrefix, "", 1)
			reachable[i] = err == nil
		}(i, s)
	}
	probeWG.Wait()
	allReachable := true
	for i := range m.mirrors {
		if !reachable[i] {
			allReachable = false
			m.setHealthy(i, false)
		}
	}
	if allReachable {
		m.repairInternal()
	}
	go m.repairLoop()
	go m.healthLoop()
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
	was := m.healthy[i]
	m.healthy[i] = ok
	if !ok {
		m.repairable = true
	}
	m.mu.Unlock()
	if was != ok {
		log.Printf("mirror[%d] healthy=%v (head caught up: %v)", i, ok, ok)
	}
}

// maxPendingKeys bounds the delta recorded while a mirror is down. Beyond it
// the mirror needs a full pass: the in-memory set must not grow without bound
// during a long outage, and a bounded set is cheaper to copy than a scan.
const maxPendingKeys = 100000

// routeStandby reports whether mirror i may receive key right now. A mirror
// that is out of service gets the key recorded as pending instead; the health
// check and the recording share one lock so a recovery cannot flip healthy
// between them and lose the write.
func (m *MirrorStore) routeStandby(i int, key string) bool {
	if isMutableKey(key) {
		// The HEAD pointer is reconciled from the newest replica on recovery;
		// recording every intermediate value would only copy stale pointers.
		m.mu.Lock()
		h := m.healthy[i]
		m.mu.Unlock()
		return h
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.healthy[i] {
		return true
	}
	m.recordPendingLocked(i, key)
	return false
}

func (m *MirrorStore) recordPendingLocked(i int, key string) {
	if isMutableKey(key) {
		return
	}
	if m.pending[i] == nil {
		m.pending[i] = make(map[string]bool)
	}
	if len(m.pending[i]) >= maxPendingKeys {
		m.pendingOverflow[i] = true
		return
	}
	m.pending[i][key] = true
}

// skipWrite records a key whose standby copy failed (or was skipped). The
// next recovery copies exactly this delta, closing the window where a write
// raced a repair pass that then declared the mirror whole.
func (m *MirrorStore) skipWrite(i int, key string) {
	m.mu.Lock()
	m.recordPendingLocked(i, key)
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
func (m *MirrorStore) markDown(i int) { m.takeDown(i, false) }

// markDownHung takes a mirror out of service after it stopped answering
// (health probe or read timeout) instead of reporting a write error. Writes
// that "succeeded" into its socket buffer may never be processed if the
// process is killed, so the next repair must list every object instead of
// trusting the pending delta.
func (m *MirrorStore) markDownHung(i int) { m.takeDown(i, true) }

func (m *MirrorStore) takeDown(i int, hung bool) {
	m.mu.Lock()
	wasHealthy := m.healthy[i]
	m.healthy[i] = false
	m.repairable = true
	if wasHealthy {
		// It was complete a moment ago, so only keys written from now on can
		// be missing. The delta is recorded by routeStandby -- unless the node
		// hung, where earlier buffered writes are unaccounted for.
		m.wholeAtDown[i] = !hung
		if hung {
			m.pendingOverflow[i] = true
		}
	}
	if i == m.prim {
		m.fenced = true
	}
	start := wasHealthy && !m.recovering
	if start {
		m.recovering = true
	}
	m.mu.Unlock()
	if wasHealthy {
		log.Printf("mirror[%d] taken out of service (hung=%v fenced=%v)", i, hung, i == m.prim)
	}
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

func (m *MirrorStore) noteAcked(b []byte) {
	m.mu.Lock()
	// The acknowledged HEAD is a high-water mark: a partial repair view (the
	// newer replica unreachable, an older one still up) must never lower it.
	// Doing so cleared fencing on a lagging node and let it commit at an old
	// revision number, overwriting a different revision's objects (observed
	// at 800k objects: rev728 existed with two contents).
	if m.mutableOrder != nil && m.controlKey != "" && len(m.ackedCurrent) > 0 &&
		m.mutableOrder(m.controlKey, b, m.ackedCurrent) < 0 {
		m.mu.Unlock()
		return
	}
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
	if !fenced {
		return nil
	}
	if key == "" || len(want) == 0 {
		// HEAD is unknown (e.g. a cold start while the peer is unreachable):
		// refusing is the only safe answer. The recovery loop fills the ack
		// once a majority of replicas agree.
		return ErrReplicaLagging
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
		// A replica that is *ahead* of this process's last acknowledgement is
		// safe: another svngod process committed through it. Adopt that HEAD
		// instead of fencing forever (repair updates the replicas but cannot
		// touch this process's in-memory ack). Only a strictly older HEAD is
		// a real lag.
		if err == nil && m.mutableOrder != nil && m.mutableOrder(key, got, want) > 0 {
			m.noteAcked(got)
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

// isRevisionKey reports whether key names a revision (or its properties)
// rather than content-addressed data. Two writers can produce different bytes
// for the same revision number (node ids follow a walk order), so these keys
// must be written to the same authoritative replica as the HEAD pointer and
// must never be left divergent between replicas.
func isRevisionKey(key string) bool {
	for _, marker := range []string{"/revs-loose/", "/revs/", "/revprops-loose/", "/revprops/"} {
		if strings.Contains(key, marker) {
			return true
		}
	}
	return false
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

// quorumWriteTimeout bounds one standby copy in quorum mode. It is larger
// than the best-effort budget because the copy is now required for the write
// to be acknowledged (a 100MiB body over a LAN takes a couple of seconds).
const quorumWriteTimeout = 15 * time.Second

// quorumRegisterTimeout bounds one replica operation on a lease-sized object
// (the commit lock is a few dozen bytes). It is shorter than the data budget
// so a hung-but-still-"healthy" replica costs seconds, not a failed commit.
const quorumRegisterTimeout = 5 * time.Second

// quorumMirror writes fn to the healthy standbys after the primary has the
// object, returning ErrQuorumUnmet unless at least quorum copies exist. When
// track is set, a skipped or failed copy is recorded in the mirror's pending
// delta so recovery copies it even if it raced a repair pass.
func (m *MirrorStore) quorumMirror(ctx context.Context, pi, quorum int, key string, track bool, fn func(context.Context, Store) error) error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok := 0
	for i, s := range m.mirrors {
		if i == pi {
			continue
		}
		if track {
			if !m.routeStandby(i, key) {
				continue
			}
		} else if !m.isHealthy(i) {
			continue
		}
		wg.Add(1)
		go func(i int, s Store) {
			defer wg.Done()
			wctx, cancel := context.WithTimeout(ctx, quorumWriteTimeout)
			defer cancel()
			if err := fn(wctx, s); err != nil {
				if track {
					m.skipWrite(i, key)
				}
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

// PutAll writes to the primary and every healthy standby, requiring all of
// them. Revision-property updates use it so two processes cannot leave the
// replicas with different copies of the same mutable object.
func (m *MirrorStore) PutAll(ctx context.Context, key string, data []byte) error {
	if isRevisionKey(key) && m.ArbiterRequired() {
		return m.putRevisionAll(ctx, key, data)
	}
	n := m.LiveCount()
	if n <= 1 {
		return m.Put(ctx, key, data)
	}
	pi, err := m.writePrimary(func(s Store) error { return s.Put(ctx, key, data) })
	if err != nil {
		return err
	}
	return m.quorumMirror(ctx, pi, n, key, true, func(ctx context.Context, s Store) error {
		return s.Put(ctx, key, data)
	})
}

// putRevisionAll writes a revision object to the arbiter and every live
// standby, requiring each of them. Revision-property updates use it so two
// processes cannot leave the replicas with different bytes for the same
// revision.
func (m *MirrorStore) putRevisionAll(ctx context.Context, key string, data []byte) error {
	p, pi := m.primary()
	if err := m.checkCaughtUp(p, pi); err != nil {
		return err
	}
	wrote := 0
	for i, s := range m.mirrors {
		if i != 0 && !m.isHealthy(i) {
			continue
		}
		c, cancel := context.WithTimeout(ctx, quorumWriteTimeout)
		err := s.Put(c, key, data)
		cancel()
		if err != nil {
			m.markDown(i)
			m.noteError(i, err)
			m.skipWrite(i, key)
			if i == 0 {
				return fmt.Errorf("%w: arbiter unavailable: %v", ErrQuorumUnmet, err)
			}
			return err
		}
		m.noteWrite(i)
		wrote++
	}
	if wrote == 0 {
		return errors.New("mirror: no replica accepted the revision")
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
	for i, s := range m.mirrors {
		// An out-of-service replica cannot contribute a copy to the count and
		// must not add its retry budget to every quorum read (WAL replay runs
		// on the read path). A stale healthy flag still costs one timeout.
		if !m.isHealthy(i) {
			continue
		}
		c, cancel := context.WithTimeout(ctx, quorumReadTimeout)
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

// ArbiterGet reads key from the configured first mirror. Lease state lives
// there even when the replica is marked down: another process may still be
// using it as its arbiter, and failing over to another node for register
// operations would let both hold a lease and publish the same revision.
func (m *MirrorStore) ArbiterGet(ctx context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	a := m.mirrors[0]
	m.mu.Unlock()
	return a.Get(ctx, key, nil)
}

// GetRepairing returns the first replica copy of key that passes verify and
// overwrites the corrupt copies it observed. Used when a read fails its
// content checksum: without this a single bit flip on one replica stayed
// forever and a failover turned it into an unreadable revision.
func (m *MirrorStore) GetRepairing(ctx context.Context, key string, verify func([]byte) bool) ([]byte, error) {
	m.mu.Lock()
	mirrors := append([]Store(nil), m.mirrors...)
	healthy := append([]bool(nil), m.healthy...)
	m.mu.Unlock()
	var bad []int
	var lastErr error
	for i, s := range mirrors {
		if !healthy[i] {
			continue
		}
		c, cancel := context.WithTimeout(ctx, standbyReadTimeout)
		b, err := s.Get(c, key, nil)
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		if verify != nil && !verify(b) {
			bad = append(bad, i)
			lastErr = fmt.Errorf("store: replica %d holds an invalid %q", i, key)
			continue
		}
		for _, j := range bad {
			wc, wcancel := context.WithTimeout(context.Background(), standbyWriteTimeout)
			err := mirrors[j].Put(wc, key, b)
			wcancel()
			if err == nil {
				m.noteWrite(j)
			} else {
				m.noteError(j, err)
			}
		}
		if len(bad) > 0 {
			log.Printf("store: healed %q from replica %d (%d corrupt copies)", key, i, len(bad))
		}
		return b, nil
	}
	if lastErr == nil {
		lastErr = ErrNotFound
	}
	return nil, lastErr
}

// quorumReadTimeout bounds one replica read in QuorumGet. It is shorter than
// the write budget because a read has no durable side effect and a slow
// replica can be excluded from the count instead of holding the request.
const quorumReadTimeout = 5 * time.Second

// standbyWriteTimeout bounds how long a commit waits for one standby. It is
// far above a healthy write (tens of ms locally) but short enough that a dead
// node costs a few seconds, not the S3 SDK's full retry budget.
const standbyWriteTimeout = 8 * time.Second

// Read budgets bound a replica that accepts a request but never answers (a
// paused process, a black-holed path). Without them a commit that falls back
// to read from such a replica hung for minutes; the request only completed when
// the peer was resumed.
const (
	primaryReadTimeout = 60 * time.Second
	standbyReadTimeout = 10 * time.Second
	// startupProbeTimeout bounds the reachability check NewMirror runs before
	// its blocking alignment pass. A down backend must delay startup by
	// seconds, not by a full list/repair timeout.
	startupProbeTimeout = 5 * time.Second
)

// mirrorWrites fans a standby write out to every healthy mirror in parallel:
// standbys are best effort, so their latency must not add to the request. When
// track is set, a mirror that is down (or whose copy fails) records the key in
// its pending delta, which is what a fast recovery copies.
func (m *MirrorStore) mirrorWrites(ctx context.Context, pi int, key string, track bool, fn func(context.Context, Store) error) {
	var wg sync.WaitGroup
	for i, s := range m.mirrors {
		if i == pi {
			continue
		}
		if track {
			if !m.routeStandby(i, key) {
				continue
			}
		} else if !m.isHealthy(i) {
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
				if track {
					m.skipWrite(i, key)
				}
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
	if isRevisionKey(key) || isControlKey(key) {
		// Only content-addressed objects may be repaired from any replica.
		// A revision-numbered object on a standby can be an orphan from a
		// failed attempt, and copying it onto the arbiter would publish the
		// wrong revision. Ordered revision writes and copyKeyTo handle those.
		return
	}
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

// tryFastRecover heals every mirror that only needs the pending delta copied.
// It returns false when at least one down mirror needs a full repair pass
// (it was not whole when it went down, its delta overflowed, or a copy failed).
func (m *MirrorStore) tryFastRecover() bool {
	m.mu.Lock()
	var targets []int
	for i := range m.mirrors {
		if m.healthy[i] {
			continue
		}
		if !m.wholeAtDown[i] || m.pendingOverflow[i] {
			m.mu.Unlock()
			return false
		}
		targets = append(targets, i)
	}
	m.mu.Unlock()
	if len(targets) == 0 {
		return true
	}
	for _, i := range targets {
		if !m.fastRecoverMirror(i) {
			return false
		}
	}
	return true
}

// fastRecoverMirror copies the keys mirror i missed while it was down, syncs
// the HEAD pointer and puts it back in service. It loops so keys written while
// the delta itself is being copied are not lost; the health flip happens under
// the same lock as pending recording, so a writer cannot slip in between the
// final emptiness check and the flip.
func (m *MirrorStore) fastRecoverMirror(i int) bool {
	if !m.drainPending(i) {
		return false
	}
	key := m.controlName()
	if key != "" {
		if err := m.syncControl(i); err != nil {
			log.Printf("mirror[%d] fast recovery HEAD sync failed: %v", i, err)
			return false
		}
	}
	m.mu.Lock()
	if len(m.pending[i]) == 0 {
		m.healthy[i] = true
		m.wholeAtDown[i] = false
		m.pendingOverflow[i] = false
		m.verify[i] = true
		m.mu.Unlock()
		log.Printf("mirror[%d] fast recovery complete", i)
		// Audit the delta heal immediately: a full pass catches any write
		// that bypassed pending accounting (for example one performed by
		// another server process) instead of waiting for the 60s tick.
		go m.repair()
		return true
	}
	m.mu.Unlock()
	return false
}

// drainPending copies (or forgets) every key recorded in mirror i's pending
// delta. A key that no longer exists anywhere -- a session object the client
// abandoned, or a delete that raced the outage -- is dropped instead of being
// retried forever; that case otherwise kept a mirror out of service and turned
// a later primary failure into a 500 for the whole repository.
func (m *MirrorStore) drainPending(i int) bool {
	key := m.controlName()
	for round := 0; round < 20; round++ {
		m.mu.Lock()
		keys := make([]string, 0, len(m.pending[i]))
		for k := range m.pending[i] {
			if k != key {
				keys = append(keys, k)
			}
		}
		m.mu.Unlock()
		if len(keys) == 0 {
			return true
		}
		for _, k := range keys {
			err := m.copyKeyTo(i, k)
			if errors.Is(err, errRepairKeyGone) {
				// The object was deleted while the mirror was down.
				m.mu.Lock()
				delete(m.pending[i], k)
				m.mu.Unlock()
				continue
			}
			if err != nil {
				log.Printf("mirror[%d] fast recovery of %q failed: %v", i, k, err)
				return false
			}
			m.mu.Lock()
			delete(m.pending[i], k)
			m.mu.Unlock()
		}
	}
	return false
}

// errRepairKeyGone means the object was deleted after it was recorded as
// pending; the destination does not need a copy.
var errRepairKeyGone = errors.New("repair: object was deleted")

// copyKeyTo copies one object to dst from another replica. Large objects
// stream through a temp file. A key that no replica holds anymore returns
// errRepairKeyGone, so recovery can stop tracking it.
func (m *MirrorStore) copyKeyTo(dst int, key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// Revision-numbered objects can differ in bytes between replicas when a
	// failed attempt left an orphan block and a later commit reused the
	// number. Such a key must be overwritten (not treated as "already
	// present"), but only in the direction arbiter -> standby: the arbiter is
	// the authoritative publisher.
	overwrite := isRevisionKey(key) || key == m.controlName()
	arbiter := m.ArbiterRequired()
	if overwrite {
		// Publish the authoritative copy: the arbiter for a pair, the value
		// held by a quorum for 3+. Never overwrite the arbiter from a standby.
		if arbiter && dst == 0 {
			return nil
		}
		var b []byte
		var gerr error
		if arbiter {
			b, gerr = m.mirrors[0].Get(ctx, key, nil)
		} else {
			var n int
			b, n, gerr = m.QuorumGet(ctx, key)
			if gerr == nil && n < m.RegisterQuorum() {
				return nil // no authoritative value to publish
			}
		}
		if errors.Is(gerr, ErrNotFound) {
			return nil // the destination copy is an orphan; nothing to publish
		}
		if gerr != nil {
			return gerr
		}
		if perr := m.mirrors[dst].Put(ctx, key, b); perr != nil {
			return perr
		}
		m.noteWrite(dst)
		return nil
	}
	if _, ok, err := m.mirrors[dst].Head(ctx, key); err == nil && ok {
		return nil // already present (for example copied by a read repair)
	}
	var last error
	seen, missing := 0, 0
	for idx, src := range m.mirrors {
		if idx == dst {
			continue
		}
		seen++
		b, err := src.Get(ctx, key, &Range{Offset: 0, Length: repairStreamThreshold + 1})
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				missing++
			} else {
				last = err
			}
			continue
		}
		if int64(len(b)) <= repairStreamThreshold {
			return m.mirrors[dst].Put(ctx, key, b)
		}
		// The probe read a truncated object: stream the whole one.
		return m.copyFullStream(ctx, src, dst, key)
	}
	// A reachable replica that does not hold the key means it was deleted (or
	// never quorum-published): an unreachable third replica must not keep this
	// key in pending forever, which used to block recovery whenever one node
	// was down.
	if missing > 0 {
		return errRepairKeyGone
	}
	if last != nil {
		return last
	}
	if seen == 0 {
		return fmt.Errorf("fast recovery: no replica to read %q from", key)
	}
	return errRepairKeyGone
}

func (m *MirrorStore) copyFullStream(ctx context.Context, src Store, dst int, key string) error {
	f, err := os.CreateTemp("", "svngod-recover-")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	n, err := src.GetTo(ctx, key, f)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	rf, err := os.Open(name)
	if err != nil {
		return err
	}
	defer rf.Close()
	return m.mirrors[dst].PutFrom(ctx, key, rf, n)
}

// syncControl copies the newest HEAD pointer to dst. The primary is
// authoritative for the mutable pointer; if this process acknowledged a newer
// value, that value wins (noteAcked refuses to move backwards).
func (m *MirrorStore) syncControl(dst int) error {
	key := m.controlName()
	if key == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, _ := m.primary()
	b, err := p.Get(ctx, key, nil)
	if err != nil {
		return err
	}
	ack := m.ackedCopy()
	if len(ack) > 0 && m.mutableOrder != nil && m.mutableOrder(key, b, ack) < 0 {
		b = ack
	}
	return m.mirrors[dst].Put(ctx, key, b)
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

// healthProbeInterval / healthProbeTimeout bound how long a hung but still
// connected replica (a paused process, a black-holed network path) can keep
// receiving "successful" buffered writes before it is taken out of service.
// Three strikes at 5s keep a busy-but-healthy node from being evicted: a 2s
// list deadline flapped a standby under a 1M-object repair scan.
const (
	healthProbeInterval = 5 * time.Second
	healthProbeTimeout  = 5 * time.Second
	healthProbeStrikes  = 3
)

// healthLoop probes every healthy mirror with one cheap list request. A write
// to a paused process can succeed into its socket buffer without ever being
// persisted, so write errors alone do not detect a hang; a probe with a short
// deadline does, in about ten seconds instead of waiting for the 60s repair.
func (m *MirrorStore) healthLoop() {
	t := time.NewTicker(healthProbeInterval)
	defer t.Stop()
	fails := make([]int, len(m.mirrors))
	for {
		select {
		case <-m.stop:
			return
		case <-t.C:
			m.probeOnce(fails)
		}
	}
}

// probeOnce runs one health round; exposed for tests.
func (m *MirrorStore) probeOnce(fails []int) {
	m.mu.Lock()
	prefix := strings.Trim(m.repoPrefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	control := m.controlKey
	mirrors := append([]Store(nil), m.mirrors...)
	healthy := append([]bool(nil), m.healthy...)
	m.mu.Unlock()
	// Never evict the elected primary on a probe: its liveness is exercised by
	// every read and write with their own budgets, and a short list deadline
	// under heavy load once fenced a healthy primary, turning the cluster
	// read-only. The probe exists for standbys, where a write can otherwise
	// look successful while the process is paused.
	_, prim := m.primary()
	for i, s := range mirrors {
		if i == prim || !healthy[i] {
			fails[i] = 0
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), healthProbeTimeout)
		var err error
		if control != "" {
			// A single HEAD on the tiny HEAD pointer is O(1); listing can page
			// and took seconds under a concurrent 1M-object repair scan.
			_, _, err = s.Head(ctx, control)
		} else {
			_, _, err = s.ListPrefix(ctx, prefix, "", 1)
		}
		cancel()
		if err == nil {
			fails[i] = 0
			continue
		}
		fails[i]++
		if fails[i] >= healthProbeStrikes {
			log.Printf("mirror[%d] health probe failed %d times: %v", i, fails[i], err)
			m.markDownHung(i)
			fails[i] = 0
		}
	}
}

// recoveryLoop restores redundancy while a mirror is down. A mirror that was
// whole when it went down only misses the keys recorded in pending, so the
// common short outage is healed by copying that delta; a full listing is kept
// for a mirror that was not whole, overflowed its delta, or failed a probe.
// The loop also probes an unreachable mirror cheaply instead of listing every
// object on the healthy side every 3s (at 1M objects that scan is expensive and
// competes with foreground commits).
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
		if m.downReachable() {
			if m.tryFastRecover() {
				continue
			}
			m.repair()
		}
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

// downReachable probes every out-of-service mirror with one cheap list request.
// Listing nothing is enough to tell a restarting backend apart from one that is
// still gone, without enumerating the repository.
func (m *MirrorStore) downReachable() bool {
	m.mu.Lock()
	var down []int
	prefix := strings.Trim(m.repoPrefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	for i, h := range m.healthy {
		if !h {
			down = append(down, i)
		}
	}
	m.mu.Unlock()
	if len(down) == 0 {
		return true
	}
	for _, i := range down {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _, err := m.mirrors[i].ListPrefix(ctx, prefix, "", 1)
		cancel()
		if err == nil {
			return true
		}
	}
	return false
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
	if m.upToDate() {
		return
	}
	if !m.repairOnce.TryLock() {
		return
	}
	defer m.repairOnce.Unlock()
	m.repairInternal()
}

// upToDate reports whether every mirror is in service and already holds the
// acknowledged HEAD. In that state a full object listing cannot find anything
// to repair, and at 1M objects the periodic pass would enumerate every object
// on every replica (thousands of list requests and a large key map) while
// competing with commits for the backend. Any doubt -- a down mirror, a fence,
// an unknown/divergent HEAD, an unreachable probe -- falls through to the full
// pass, which is also what the recovery loop needs.
func (m *MirrorStore) upToDate() bool {
	m.mu.Lock()
	key := m.controlKey
	ack := append([]byte(nil), m.ackedCurrent...)
	fenced := m.fenced
	down := false
	for i, h := range m.healthy {
		if !h || m.verify[i] {
			down = true
			break
		}
	}
	mirrors := append([]Store(nil), m.mirrors...)
	m.mu.Unlock()
	if down || fenced || key == "" || len(ack) == 0 {
		return false
	}
	want := ContentETag(ack)
	for _, s := range mirrors {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		b, err := s.Get(ctx, key, nil)
		cancel()
		if err != nil || ContentETag(b) != want {
			return false
		}
	}
	return true
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
	sets := make([]map[string]int64, n)
	reachable := make([]bool, n)
	var wg sync.WaitGroup
	for i := range m.mirrors {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), repairListTimeout)
			defer cancel()
			ks, err := listAll(ctx, m.mirrors[i], m.repoPrefix)
			if err != nil {
				log.Printf("mirror[%d] unreachable during repair: %v", i, err)
				return
			}
			reachable[i] = true
			sets[i] = ks
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

	ctx, cancel := context.WithTimeout(context.Background(), repairCopyTimeout)
	defer cancel()

	// Mutable objects (the HEAD pointer) are reconciled first and serially:
	// they are few, and reconcileMutable may take a mirror out of service,
	// which the copy workers below read concurrently.
	for k := range keys {
		if isMutableKey(k) {
			m.reconcileMutable(ctx, k, sets, reachable)
		}
	}
	// Revision blocks are numbered, not content-addressed: a failed attempt
	// can leave bytes that differ from the later successful commit under the
	// same number. The copy workers only fill missing keys, so reconcile the
	// recent tail (where an outage can leave such an orphan) and any key whose
	// listed size already differs.
	m.reconcileRevisions(keys, sets, reachable)

	// Copy immutable objects with a bounded worker pool. A serial loop was the
	// recovery bottleneck: a 27-revision outage at 800k objects left ~50k
	// objects to copy and took 48s; the same work parallelises to a few
	// seconds. Objects are immutable, so copies need no ordering.
	var mu sync.Mutex
	jobs := make(chan string)
	var cw sync.WaitGroup
	for w := 0; w < repairCopyConcurrency; w++ {
		cw.Add(1)
		go func() {
			defer cw.Done()
			for k := range jobs {
				m.copyRepairObject(ctx, k, sets, reachable, &mu, missing, failed)
			}
		}()
	}
send:
	for k := range keys {
		if isMutableKey(k) {
			continue
		}
		select {
		case jobs <- k:
		case <-ctx.Done():
			break send
		}
	}
	close(jobs)
	cw.Wait()

	// A full pass is also the moment to drain keys recorded while a mirror was
	// out of service. Some of them may have been deleted since (an abandoned
	// session); leaving those in pending would keep the mirror unhealthy
	// forever and turn a later primary failure into a 500 for the repository.
	for i := range m.mirrors {
		if !reachable[i] || failed[i] || missing[i] > 0 {
			continue
		}
		if !m.drainPending(i) {
			failed[i] = true
		}
	}

	for i := range m.mirrors {
		ok := reachable[i] && !failed[i] && missing[i] <= 0
		if ok {
			// Writes skipped while this pass ran are only in pending. Marking
			// the mirror whole before they are copied would leave a hole that
			// only a later failover would expose.
			m.mu.Lock()
			if len(m.pending[i]) > 0 {
				ok = false
			}
			m.mu.Unlock()
		}
		m.setHealthy(i, ok)
		if ok {
			m.mu.Lock()
			m.verify[i] = false
			m.wholeAtDown[i] = false
			m.pendingOverflow[i] = false
			m.pending[i] = nil
			m.mu.Unlock()
		}
	}
	if os.Getenv("SVNGOD_DEBUG_REPAIR") == "1" {
		counts := make([]int, n)
		for i := range sets {
			counts[i] = len(sets[i])
		}
		log.Printf("repair: reachable=%v sets=%v keys=%d missing=%v failed=%v healthy=%v",
			reachable, counts, len(keys), missing, failed, m.healthSnapshot())
	}
}

// repairCopyConcurrency bounds the object copies of one repair pass. The old
// serial loop was the recovery bottleneck (48s for a 27-revision outage at
// 800k objects); objects are immutable, so copies can run in parallel.
const repairCopyConcurrency = 12

// revisionNumber returns the revision encoded in a revision key's last path
// segment (0 when it is not a plain number, e.g. a packed shard name).
func revisionNumber(key string) uint64 {
	i := strings.LastIndexByte(key, '/')
	if i < 0 || i+1 >= len(key) {
		return 0
	}
	n, err := strconv.ParseUint(key[i+1:], 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// reconcileRevisions rewrites numbered revision objects that differ from the
// authoritative copy. Comparing every revision byte-for-byte on every repair
// would re-read the whole history, so only the recent tail (where an outage
// can leave an orphan block) and keys whose listed sizes already differ are
// checked. For a two-node pair the arbiter's bytes win; with three or more the
// value held by a quorum wins, and a replica is never overwritten when no
// authoritative copy exists.
func (m *MirrorStore) reconcileRevisions(keys map[string]bool, sets []map[string]int64, reachable []bool) {
	const recentWindow = 200
	var maxRev uint64
	for k := range keys {
		if isRevisionKey(k) {
			if n := revisionNumber(k); n > maxRev {
				maxRev = n
			}
		}
	}
	arbiter := m.ArbiterRequired()
	for k := range keys {
		if !isRevisionKey(k) {
			continue
		}
		copies := 0
		sizeDiff := false
		var size int64 = -1
		for i := range sets {
			if !reachable[i] {
				continue
			}
			s, ok := sets[i][k]
			if !ok {
				continue
			}
			copies++
			if size < 0 {
				size = s
			} else if s != size {
				sizeDiff = true
			}
		}
		if copies < 2 {
			continue // a missing copy is the copy workers' job
		}
		recent := maxRev > 0 && revisionNumber(k)+recentWindow >= maxRev
		if !recent && !sizeDiff {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		var want []byte
		var err error
		if arbiter {
			if !reachable[0] {
				cancel()
				continue
			}
			want, err = m.mirrors[0].Get(ctx, k, nil)
		} else {
			var n int
			want, n, err = m.QuorumGet(ctx, k)
			if err == nil && n < m.RegisterQuorum() {
				err = ErrQuorumUnmet
			}
		}
		if err != nil {
			cancel()
			continue
		}
		for i := range m.mirrors {
			if !reachable[i] || (arbiter && i == 0) {
				continue
			}
			if perr := m.mirrors[i].Put(ctx, k, want); perr != nil {
				log.Printf("mirror[%d] revision reconcile %s: %v", i, k, perr)
				continue
			}
			m.noteWrite(i)
		}
		cancel()
	}
}

// repairStreamThreshold keeps large objects out of the workers' heap: above
// this size a copy streams through a temp file instead of buffering the whole
// object in memory.
const repairStreamThreshold = 4 << 20

// copyRepairObject copies one immutable object to every reachable mirror that
// lacks it, reading it from the first mirror that holds it. A failed copy
// marks the destination out of service for the rest of the pass, so a dead
// backend costs one error instead of one per object.
func (m *MirrorStore) copyRepairObject(ctx context.Context, key string, sets []map[string]int64,
	reachable []bool, mu *sync.Mutex, missing []int, failed []bool) {
	mu.Lock()
	var dests []int
	src, size := -1, int64(0)
	for i := range m.mirrors {
		if !reachable[i] {
			continue
		}
		if sz, has := sets[i][key]; has {
			if src < 0 {
				src, size = i, sz
			}
			continue
		}
		if !failed[i] {
			dests = append(dests, i)
		}
	}
	mu.Unlock()
	if src < 0 || len(dests) == 0 {
		return
	}
	for _, dst := range dests {
		var err error
		if size > repairStreamThreshold {
			err = m.copyRepairStream(ctx, src, dst, key, size)
		} else {
			var b []byte
			if b, err = m.mirrors[src].Get(ctx, key, nil); err == nil {
				err = m.mirrors[dst].Put(ctx, key, b)
			}
		}
		mu.Lock()
		if err != nil {
			log.Printf("mirror[%d] repair put %q: %v", dst, key, err)
			failed[dst] = true
		} else {
			missing[dst]--
		}
		mu.Unlock()
		if err != nil {
			return
		}
	}
}

// copyRepairStream copies one large object through a temp file so a repair
// pass does not buffer it in memory.
func (m *MirrorStore) copyRepairStream(ctx context.Context, src, dst int, key string, size int64) error {
	f, err := os.CreateTemp("", "svngod-repair-")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	n, err := m.mirrors[src].GetTo(ctx, key, f)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if size > 0 && n != size {
		return fmt.Errorf("repair: %q copied %d of %d bytes", key, n, size)
	}
	rf, err := os.Open(name)
	if err != nil {
		return err
	}
	defer rf.Close()
	return m.mirrors[dst].PutFrom(ctx, key, rf, n)
}

// reconcileMutable picks the newest value of a mutable key and writes it to
// every reachable mirror that is missing it or holds an older one.
func (m *MirrorStore) reconcileMutable(ctx context.Context, k string, sets []map[string]int64, reachable []bool) {
	var best []byte
	for i := range m.mirrors {
		if !reachable[i] {
			continue
		}
		if _, has := sets[i][k]; !has {
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
	if k == m.controlName() {
		// Never adopt a HEAD from an uncertain view. The rules:
		//  - if this process already acknowledged a HEAD, best must not be
		//    older (repair may copy the known baseline or anything newer);
		//  - with no baseline (cold start), only an all-reachable view or a
		//    majority agreeing on best may establish it. Otherwise a lagging
		//    survivor was adopted, fencing cleared and a commit overwrote an
		//    old revision number (observed at 800k objects: rev728 had two
		//    different contents).
		m.mu.Lock()
		ack := append([]byte(nil), m.ackedCurrent...)
		m.mu.Unlock()
		if len(ack) > 0 {
			if m.mutableOrder != nil && m.mutableOrder(k, best, ack) < 0 {
				return
			}
		} else {
			allReachable := true
			for i := range m.mirrors {
				if !reachable[i] {
					allReachable = false
					break
				}
			}
			if !allReachable {
				holders, disagree := 0, false
				for i := range m.mirrors {
					if !reachable[i] {
						continue
					}
					if _, has := sets[i][k]; !has {
						continue
					}
					b, err := m.mirrors[i].Get(ctx, k, nil)
					if err != nil {
						continue
					}
					if m.order(k, b, best) == 0 {
						holders++
					} else {
						disagree = true
					}
				}
				if holders < len(m.mirrors)/2+1 || disagree {
					return
				}
			}
		}
	}
	for i := range m.mirrors {
		if !reachable[i] {
			continue
		}
		if _, has := sets[i][k]; has {
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
	// The reconciled value is the newest known HEAD; adopt it so the fencing
	// state clears even when this process did not perform the CAS itself.
	if k == m.controlName() {
		m.noteAcked(best)
	}
}

// healthSnapshot copies the health flags for diagnostics.
func (m *MirrorStore) healthSnapshot() []bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]bool, len(m.healthy))
	copy(out, m.healthy)
	return out
}

// repairListTimeout bounds one mirror's object listing. At 1M objects the
// listing takes far longer than the old 5s budget; a short timeout made a
// healthy peer look unreachable and triggered the stale-HEAD bug.
const repairListTimeout = 90 * time.Second

// repairCopyTimeout bounds the copy/reconcile phase of one pass. A full 1M
// object repair needs minutes; partial progress is kept and the next pass
// continues from the missing-object counts.
const repairCopyTimeout = 10 * time.Minute

// isMutableKey reports whether an object's content must always be mirrored
// from the primary (the only mutable object today is the HEAD `current`).
func isMutableKey(key string) bool {
	return strings.HasSuffix(key, "/current")
}

// listAll returns every object under prefix with its size. Repair needs the
// size so large objects can be copied as streams instead of buffering them.
func listAll(ctx context.Context, s Store, prefix string) (map[string]int64, error) {
	keys := make(map[string]int64)
	next := ""
	p := strings.Trim(prefix, "/")
	if p != "" {
		p += "/"
	}
	for {
		objs, n, err := s.ListPrefix(ctx, p, next, 1000)
		if err != nil {
			return nil, err
		}
		for _, o := range objs {
			keys[o.Key] = o.Size
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
	arbiter := m.ArbiterRequired()
	for i, s := range m.mirrors {
		// Two-node pair: the first mirror is the arbiter and is always
		// attempted, even when this process marked it down (skipping it once
		// let two processes create leases on different mirrors and publish the
		// same revision). Three or more nodes use a majority quorum instead;
		// any two majorities intersect, so unhealthy nodes may be skipped.
		if i == 0 {
			if !arbiter && !m.isHealthy(0) {
				continue
			}
		} else if !m.isHealthy(i) {
			continue
		}
		c, cancel := context.WithTimeout(ctx, quorumRegisterTimeout)
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
			if i == 0 && arbiter {
				// Lease state is only authoritative on the arbiter; using
				// another replica would reopen the split-lease window.
				rollback()
				return fmt.Errorf("%w: arbiter unavailable: %v", ErrQuorumUnmet, err)
			}
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
	arbiter := m.ArbiterRequired()
	for i, s := range m.mirrors {
		if i == 0 {
			if !arbiter && !m.isHealthy(0) {
				continue
			}
		} else if !m.isHealthy(i) {
			continue
		}
		c, cancel := context.WithTimeout(ctx, quorumRegisterTimeout)
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
			if i == 0 && arbiter {
				rollback()
				return fmt.Errorf("%w: arbiter unavailable: %v", ErrQuorumUnmet, err)
			}
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
	arbiter := m.ArbiterRequired()
	for i, s := range m.mirrors {
		if i == 0 {
			if !arbiter && !m.isHealthy(0) {
				continue
			}
		} else if !m.isHealthy(i) {
			continue
		}
		c, cancel := context.WithTimeout(ctx, quorumRegisterTimeout)
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
			if i == 0 && arbiter {
				return fmt.Errorf("%w: arbiter unavailable: %v", ErrQuorumUnmet, err)
			}
		}
	}
	if ok < quorum {
		return fmt.Errorf("%w: %d of %d deleted (need %d)", ErrQuorumUnmet, ok, len(m.mirrors), quorum)
	}
	return nil
}
