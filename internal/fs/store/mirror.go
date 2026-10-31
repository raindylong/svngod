package store

import (
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

	// mutableOrder orders two values of a mutable key (<0: a older than b).
	// fs sets it so a rejoining mirror cannot overwrite a newer HEAD pointer
	// with its stale copy. Without it, values are treated as equal.
	mutableOrder func(key string, a, b []byte) int
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

// NewMirror builds a MirrorStore over the given backends, aligns them, and
// starts the background repair loop.
func NewMirror(mirrors []Store, opts ...MirrorOption) *MirrorStore {
	if len(mirrors) < 2 {
		panic("MirrorStore requires at least 2 mirrors")
	}
	m := &MirrorStore{
		mirrors: mirrors,
		healthy: make([]bool, len(mirrors)),
		prim:    0,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
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

// markDown records a mirror as failed so it is excluded from service.
func (m *MirrorStore) markDown(i int) {
	m.setHealthy(i, false)
}

// writePrimary runs a write on the elected primary; on a transport failure it
// marks that mirror down and retries on the next healthy one, so a PUT or
// commit does not fail just because the elected primary went away between the
// read and the write. Precondition/missing-object results are not retried.
func (m *MirrorStore) writePrimary(fn func(Store) error) (int, error) {
	p, pi := m.primary()
	err := fn(p)
	if err == nil {
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
			return idx, nil
		} else if errors.Is(e, ErrPreconditionFailed) || errors.Is(e, ErrNotFound) {
			return idx, e
		} else {
			m.markDown(idx)
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
	// Best-effort mirror to healthy standbys.
	m.mirrorWrites(ctx, pi, func(s Store) error { return s.Put(ctx, key, data) })
	return nil
}

func (m *MirrorStore) PutReconstructible(ctx context.Context, key string, data []byte) error {
	pi, err := m.writePrimary(func(s Store) error { return PutReconstructible(ctx, s, key, data) })
	if err != nil {
		return err
	}
	m.mirrorWrites(ctx, pi, func(s Store) error { return PutReconstructible(ctx, s, key, data) })
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
	m.mirrorWrites(ctx, pi, func(s Store) error {
		f, oerr := os.Open(tmpName)
		if oerr != nil {
			return oerr
		}
		defer f.Close()
		return PutFromReconstructible(ctx, s, key, f, size)
	})
	return nil
}

func (m *MirrorStore) Get(ctx context.Context, key string, rng *Range) ([]byte, error) {
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
	m.mirrorWrites(ctx, pi, func(s Store) error {
		f, oerr := os.Open(tmpName)
		if oerr != nil {
			return oerr
		}
		defer f.Close()
		return s.PutFrom(ctx, key, f, size)
	})
	return nil
}

// Exists asks the primary first, then any healthy standby.
func (m *MirrorStore) Exists(ctx context.Context, key string) (bool, error) {
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
	m.mirrorWrites(ctx, pi, func(s Store) error { return s.Put(ctx, key, data) })
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
	// Standbys mirror the current value unconditionally (their own etag does
	// not matter; they only serve reads after failover).
	m.mirrorWrites(ctx, pi, func(s Store) error { return s.Put(ctx, key, data) })
	return newEtag, nil
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
	m.mirrorWrites(ctx, pi, func(s Store) error { return DeleteIfMatch(ctx, s, key, etag) })
	return nil
}

func (m *MirrorStore) Delete(ctx context.Context, keys []string) error {
	pi, err := m.writePrimary(func(s Store) error { return s.Delete(ctx, keys) })
	if err != nil {
		return err
	}
	m.mirrorWrites(ctx, pi, func(s Store) error { return s.Delete(ctx, keys) })
	return nil
}

// DeletePrefix drops a whole prefix on the primary and on healthy standbys.
func (m *MirrorStore) DeletePrefix(ctx context.Context, prefix string) error {
	p, pi := m.primary()
	if err := DeletePrefix(ctx, p, prefix); err != nil {
		m.markDown(pi)
		return err
	}
	m.mirrorWrites(ctx, pi, func(s Store) error { return DeletePrefix(ctx, s, prefix) })
	return nil
}

// mirrorWrites fans a standby write out to every healthy mirror in parallel:
// standbys are best effort, so their latency must not add to the request.
func (m *MirrorStore) mirrorWrites(ctx context.Context, pi int, fn func(Store) error) {
	var wg sync.WaitGroup
	for i, s := range m.mirrors {
		if i == pi || !m.isHealthy(i) {
			continue
		}
		wg.Add(1)
		go func(i int, s Store) {
			defer wg.Done()
			if err := fn(s); err != nil {
				m.markDown(i)
				fmt.Printf("mirror[%d] standby write failed: %v\n", i, err)
			}
		}(i, s)
	}
	wg.Wait()
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
	need := make([]bool, n)
	for i := range m.mirrors {
		need[i] = reachable[i] && len(sets[i]) < len(keys)
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
			}
		}
	}

	for i := range m.mirrors {
		// Complete now when it already had everything, or when everything it
		// was missing has just been copied successfully.
		m.setHealthy(i, reachable[i] && !failed[i] && (len(sets[i]) == len(keys) || need[i]))
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
