package store

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"
)

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

// probeReachable checks one cheap control object instead of listing the
// repository prefix. With one journal record per revision a prefix listing can
// take longer than the probe budget, falsely marking a live backend
// unreachable and leaving the standby permanently out of service.
func (m *MirrorStore) probeReachable(ctx context.Context, i int) error {
	prefix := strings.Trim(m.repoPrefix, "/")
	if prefix == "" {
		// Test/custom stores have no repository namespace.
		_, _, err := m.mirrors[i].ListPrefix(ctx, m.repoPrefix, "", 1)
		return err
	}
	_, _, err := m.mirrors[i].Head(ctx, prefix+"/current")
	return err
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
	// A repair pass can be the first to notice that a mirror is unreachable
	// (lazy startup has no blocking probe). Without starting the recovery loop
	// here, the health loop skips the now-unhealthy mirror and nothing ever
	// re-admits it -- the standby stayed down until another process healed it.
	needRecovery := !ok && was && !m.recovering
	if needRecovery {
		m.recovering = true
	}
	m.mu.Unlock()
	if was != ok {
		log.Printf("mirror[%d] healthy=%v (head caught up: %v)", i, ok, ok)
	}
	if needRecovery {
		go m.recoveryLoop()
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

// primary returns the current primary store. The canonical arbiter (mirror 0)
// is preferred whenever it is healthy: it holds the commit lease and the HEAD
// pointer, and revision writes target it first. Keeping a standby elected
// after a transient primary timeout left write leadership on mirror 1, so
// killing that standby fenced every commit even though mirror 0 was healthy.
// A returning arbiter is only marked healthy after recovery has caught it up.
func (m *MirrorStore) primary() (Store, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.healthy[0] {
		m.prim = 0
		return m.mirrors[0], 0
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
// Three strikes at 15s keep a busy-but-healthy node from being evicted: under
// a multi-repository repair scan RustFS repeatedly needed more than 5s to
// answer one list, which flapped a live standby every minute.
const (
	healthProbeInterval = 5 * time.Second
	healthProbeTimeout  = 30 * time.Second
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
		// A backend under a full repair scan can take several seconds to
		// answer; a 2s budget kept a live standby unreachable, so recovery
		// never started and the mirror stayed out of service for good.
		ctx, cancel := context.WithTimeout(context.Background(), healthProbeTimeout)
		err := m.probeReachable(ctx, i)
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

// healthSnapshot copies the health flags for diagnostics.
func (m *MirrorStore) healthSnapshot() []bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]bool, len(m.healthy))
	copy(out, m.healthy)
	return out
}
