package store

import (
	"context"
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
}

// NewMirror builds a MirrorStore over the given backends and starts the
// background repair loop.
func NewMirror(mirrors []Store) *MirrorStore {
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
	// Initial alignment pass (cheap when mirrors are already consistent), then
	// the periodic loop.
	go func() { m.repair() }()
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
	case base == "current", base == "format", base == "uuid":
		return true
	case strings.Contains(key, "/locks/"), strings.Contains(key, "/dav-activities/"):
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

// primary returns the current primary store (first healthy mirror).
func (m *MirrorStore) primary() (Store, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
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

// --- Store interface ---

func (m *MirrorStore) Put(ctx context.Context, key string, data []byte) error {
	p, pi := m.primary()
	if err := p.Put(ctx, key, data); err != nil {
		m.markDown(pi)
		return err
	}
	// Best-effort mirror to healthy standbys.
	for i, s := range m.mirrors {
		if i == pi || !m.isHealthy(i) {
			continue
		}
		if err := s.Put(ctx, key, data); err != nil {
			m.markDown(i)
			fmt.Printf("mirror[%d] standby Put %q failed: %v\n", i, key, err)
		}
	}
	return nil
}

func (m *MirrorStore) Get(ctx context.Context, key string, rng *Range) ([]byte, error) {
	if isControlKey(key) {
		p, pi := m.primary()
		b, err := p.Get(ctx, key, rng)
		if err != nil {
			m.markDown(pi)
			// Failover for reads: the primary's control key (e.g. current) is
			// unavailable, so fall back to a healthy standby mirror so HEAD
			// lookups still work. Writes are unaffected (Put still fails when
			// the primary is down, a sane degraded mode).
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
				m.markDown(idx)
			}
			return nil, err
		}
		return b, nil
	}
	// Data: round-robin across healthy mirrors starting after the primary.
	_, pi := m.primary()
	n := len(m.mirrors)
	for step := 0; step < n; step++ {
		idx := (pi + 1 + step) % n
		if !m.isHealthy(idx) {
			continue
		}
		b, err := m.mirrors[idx].Get(ctx, key, rng)
		if err == nil {
			return b, nil
		}
		m.markDown(idx)
	}
	// Last resort: primary itself.
	p, pi := m.primary()
	b, err := p.Get(ctx, key, rng)
	if err == nil {
		return b, nil
	}
	m.markDown(pi)
	return nil, err
}

// PutFrom writes the stream to the primary and best-effort to standbys. The
// reader is buffered through a temp file because a standby needs the bytes a
// second time.
func (m *MirrorStore) PutFrom(ctx context.Context, key string, r io.Reader, size int64) error {
	tmp, err := os.CreateTemp("", "svngod-mirror-*")
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
	p, pi := m.primary()
	f, err := os.Open(tmpName)
	if err != nil {
		return err
	}
	err = p.PutFrom(ctx, key, f, size)
	f.Close()
	if err != nil {
		m.markDown(pi)
		return err
	}
	for i, s := range m.mirrors {
		if i == pi || !m.isHealthy(i) {
			continue
		}
		f, oerr := os.Open(tmpName)
		if oerr != nil {
			continue
		}
		if perr := s.PutFrom(ctx, key, f, size); perr != nil {
			m.markDown(i)
		}
		f.Close()
	}
	return nil
}

// GetTo streams an object, preferring the same mirror Get would read from.
func (m *MirrorStore) GetTo(ctx context.Context, key string, w io.Writer) (int64, error) {
	if isControlKey(key) {
		p, pi := m.primary()
		n, err := p.GetTo(ctx, key, w)
		if err == nil {
			return n, nil
		}
		m.markDown(pi)
		for step := 1; step < len(m.mirrors); step++ {
			idx := (pi + step) % len(m.mirrors)
			if !m.isHealthy(idx) {
				continue
			}
			if n, e := m.mirrors[idx].GetTo(ctx, key, w); e == nil {
				return n, nil
			}
		}
		return n, err
	}
	_, pi := m.primary()
	n := len(m.mirrors)
	for step := 0; step < n; step++ {
		idx := (pi + 1 + step) % n
		if !m.isHealthy(idx) {
			continue
		}
		if got, err := m.mirrors[idx].GetTo(ctx, key, w); err == nil {
			return got, nil
		}
		m.markDown(idx)
	}
	p, pi := m.primary()
	got, err := p.GetTo(ctx, key, w)
	if err != nil {
		m.markDown(pi)
	}
	return got, err
}

func (m *MirrorStore) Head(ctx context.Context, key string) (string, bool, error) {
	if isControlKey(key) {
		p, pi := m.primary()
		etag, ok, err := p.Head(ctx, key)
		if err != nil {
			m.markDown(pi)
		}
		return etag, ok, err
	}
	_, pi := m.primary()
	n := len(m.mirrors)
	for step := 0; step < n; step++ {
		idx := (pi + 1 + step) % n
		if !m.isHealthy(idx) {
			continue
		}
		etag, ok, err := m.mirrors[idx].Head(ctx, key)
		if err == nil {
			return etag, ok, nil
		}
		m.markDown(idx)
	}
	p, pi := m.primary()
	return p.Head(ctx, key)
}

func (m *MirrorStore) CreateIfAbsent(ctx context.Context, key string, data []byte) error {
	p, pi := m.primary()
	if err := p.CreateIfAbsent(ctx, key, data); err != nil {
		m.markDown(pi)
		return err
	}
	// The commit lock only matters on the primary; mirror the object so a
	// failover sees the same lock, but a standby failure is not fatal.
	for i, s := range m.mirrors {
		if i == pi || !m.isHealthy(i) {
			continue
		}
		if err := s.CreateIfAbsent(ctx, key, data); err != nil {
			m.markDown(i)
		}
	}
	return nil
}

func (m *MirrorStore) CAS(ctx context.Context, key string, expectETag string, data []byte) (string, error) {
	p, pi := m.primary()
	newEtag, err := p.CAS(ctx, key, expectETag, data)
	if err != nil {
		m.markDown(pi)
		return "", err
	}
	// Standbys mirror the current value unconditionally (their own etag does
	// not matter; they only serve reads after failover).
	for i, s := range m.mirrors {
		if i == pi || !m.isHealthy(i) {
			continue
		}
		if err := s.Put(ctx, key, data); err != nil {
			m.markDown(i)
		}
	}
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

func (m *MirrorStore) Delete(ctx context.Context, keys []string) error {
	p, pi := m.primary()
	if err := p.Delete(ctx, keys); err != nil {
		m.markDown(pi)
		return err
	}
	for i, s := range m.mirrors {
		if i == pi || !m.isHealthy(i) {
			continue
		}
		if err := s.Delete(ctx, keys); err != nil {
			m.markDown(i)
		}
	}
	return nil
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
func (m *MirrorStore) repairInternal() {
	p, pi := m.primary()
	keys, err := listAll(context.Background(), p)
	if err != nil {
		return
	}
	for i, s := range m.mirrors {
		if i == pi {
			continue
		}
		if !m.isHealthy(i) {
			// A down mirror must be fully caught up before re-entering service.
			if err := m.catchUp(s, p, keys); err != nil {
				continue // still down; retry next cycle
			}
			m.setHealthy(i, true)
			continue
		}
		// A healthy mirror may still have drifted; repair is cheap to verify.
		_ = m.catchUp(s, p, keys)
	}
}

// catchUp copies any object present on the primary but missing on dst, and
// always mirrors mutable objects (current HEAD pointer) since the primary is
// authoritative for them even when the standby holds an older value.
func (m *MirrorStore) catchUp(dst, src Store, keys []string) error {
	ctx := context.Background()
	for _, k := range keys {
		if isMutableKey(k) {
			data, err := src.Get(ctx, k, nil)
			if err != nil {
				return err
			}
			if err := dst.Put(ctx, k, data); err != nil {
				return err
			}
			continue
		}
		_, exists, err := dst.Head(ctx, k)
		if err != nil || !exists {
			data, err := src.Get(ctx, k, nil)
			if err != nil {
				return err
			}
			if err := dst.Put(ctx, k, data); err != nil {
				return err
			}
		}
	}
	return nil
}

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
