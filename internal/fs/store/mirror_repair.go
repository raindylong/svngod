package store

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

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
	if m.upToDate() {
		return
	}
	if !m.repairOnce.TryLock() {
		return
	}
	defer m.repairOnce.Unlock()
	ctx := context.Background()
	if !m.acquireRepairLease(ctx) {
		return
	}
	// Renew while the pass runs: a full scan of a large repository can exceed
	// the TTL, and a killed holder must block recovery for at most one TTL.
	stopRenew := make(chan struct{})
	go m.renewRepairLease(stopRenew)
	m.repairInternal()
	close(stopRenew)
	m.releaseRepairLease(context.Background())
}

// repairLeaseTTL bounds how long one process may hold the repair lease. It is
// short on purpose: a crashed or paused process leaves the lease behind, and
// every other instance skips repair until it expires. The holder renews every
// 30s, so a healthy long pass keeps the lease while recovery is never delayed
// by more than this window.
const repairLeaseTTL = 60 * time.Second

// renewRepairLease extends the lease while repairInternal runs.
func (m *MirrorStore) renewRepairLease(stop <-chan struct{}) {
	t := time.NewTicker(20 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-m.stop:
			return
		case <-t.C:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			m.renewRepairLeaseOnce(ctx)
			cancel()
		}
	}
}

// renewRepairLeaseOnce replaces the lease only if this process still owns it.
func (m *MirrorStore) renewRepairLeaseOnce(ctx context.Context) bool {
	key := m.repairLeaseKey()
	payload := []byte(fmt.Sprintf("%s %d", m.repairOwner, time.Now().Add(repairLeaseTTL).UnixNano()))
	for i := range m.mirrors {
		if !m.isHealthy(i) {
			continue
		}
		cur, err := m.mirrors[i].Get(ctx, key, nil)
		if err != nil {
			continue
		}
		if !strings.HasPrefix(string(cur), m.repairOwner+" ") {
			return false
		}
		_, err = m.mirrors[i].CAS(ctx, key, ContentETag(cur), payload)
		return err == nil
	}
	return false
}

// repairLeaseKey names the cross-process repair lease. It lives outside the
// repository prefix so full repair listings never treat it as repository
// data.
func (m *MirrorStore) repairLeaseKey() string {
	prefix := strings.Trim(m.repoPrefix, "/")
	if prefix == "" {
		prefix = "default"
	}
	return "repair-leases/" + prefix
}

// acquireRepairLease lets one process perform the expensive full repair at a
// time. Without it, every svngod instance sharing the store ran its own full
// listing and reconciliation pass after a failover -- four simultaneous scans
// of the same bucket, which is what pushed RustFS into list timeouts. The
// lease is best effort: if no replica accepts it, the caller repairs anyway;
// duplicated work is safe, only slower.
func (m *MirrorStore) acquireRepairLease(ctx context.Context) bool {
	if !m.lazy || strings.Trim(m.repoPrefix, "/") == "" {
		// Test/custom stores have no repository namespace to scope the lease;
		// keep their single-process behaviour and do not inject a lease object
		// into a whole-store listing.
		return true
	}
	key := m.repairLeaseKey()
	payload := []byte(fmt.Sprintf("%s %d", m.repairOwner, time.Now().Add(repairLeaseTTL).UnixNano()))
	_, pi := m.primary()
	candidates := []int{pi}
	for i := range m.mirrors {
		if i != pi && m.isHealthy(i) {
			candidates = append(candidates, i)
		}
	}
	for _, i := range candidates {
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := m.mirrors[i].CreateIfAbsent(c, key, payload)
		cancel()
		if err == nil {
			return true
		}
		if !errors.Is(err, ErrPreconditionFailed) {
			continue
		}
		// A lease exists. Take it over only when its deadline has passed.
		c, cancel = context.WithTimeout(ctx, 5*time.Second)
		cur, gerr := m.mirrors[i].Get(c, key, nil)
		cancel()
		if gerr != nil {
			return false
		}
		fields := strings.Fields(string(cur))
		if len(fields) != 2 {
			return false
		}
		var deadline int64
		if _, perr := fmt.Sscanf(fields[1], "%d", &deadline); perr != nil {
			return false
		}
		if deadline > time.Now().UnixNano() {
			return false
		}
		c, cancel = context.WithTimeout(ctx, 5*time.Second)
		_, cerr := m.mirrors[i].CAS(c, key, ContentETag(cur), payload)
		cancel()
		return cerr == nil
	}
	// No replica accepted the lease (backend unreachable): allow the pass. It
	// will hit the same backend anyway, and duplicate repair is harmless.
	return true
}

// releaseRepairLease removes the lease only when this process still owns it,
// so a timed-out holder cannot delete a lease another process just took.
func (m *MirrorStore) releaseRepairLease(ctx context.Context) {
	if !m.lazy || strings.Trim(m.repoPrefix, "/") == "" {
		return
	}
	key := m.repairLeaseKey()
	for i := range m.mirrors {
		if !m.isHealthy(i) {
			continue
		}
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		cur, err := m.mirrors[i].Get(c, key, nil)
		cancel()
		if err != nil || !strings.HasPrefix(string(cur), m.repairOwner+" ") {
			continue
		}
		c, cancel = context.WithTimeout(ctx, 5*time.Second)
		_ = DeleteIfMatch(c, m.mirrors[i], key, ContentETag(cur))
		cancel()
	}
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
	dbg := os.Getenv("SVNGOD_DEBUG_REPAIR") == "1"
	if dbg {
		log.Printf("repair %s: start", strings.Trim(m.repoPrefix, "/"))
	}
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
			// A mirror marked down gets a cheap probe first: a dead backend fails
			// fast, so the pass does not wait the full large-repository timeout.
			// A live but slow backend (RustFS right after a restart) can take
			// longer than a couple of seconds even for a tiny list, so the probe
			// gets 30s; it then receives the normal full-listing budget. The
			// previous 5s probe kept a live restarting standby failing forever.
			timeout := repairListTimeout
			if !m.isHealthy(i) {
				pctx, pcancel := context.WithTimeout(context.Background(), 30*time.Second)
				perr := m.probeReachable(pctx, i)
				pcancel()
				if perr != nil {
					log.Printf("mirror[%d] still unreachable during repair: %v", i, perr)
					return
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			ks, err := listRepairObjects(ctx, m.mirrors[i], m.repoPrefix)
			if err != nil {
				log.Printf("mirror[%d] unreachable during repair: %v", i, err)
				return
			}
			reachable[i] = true
			sets[i] = ks
		}(i)
	}
	wg.Wait()
	if dbg {
		log.Printf("repair %s: listed reachable=%v", strings.Trim(m.repoPrefix, "/"), reachable)
	}

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
	if dbg {
		log.Printf("repair %s: reconciling revisions (%d keys)", strings.Trim(m.repoPrefix, "/"), len(keys))
	}
	m.reconcileRevisions(keys, sets, reachable)
	if dbg {
		log.Printf("repair %s: copying objects", strings.Trim(m.repoPrefix, "/"))
	}

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
	if dbg {
		log.Printf("repair %s: copy done", strings.Trim(m.repoPrefix, "/"))
	}

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
		log.Printf("repair %s: reachable=%v sets=%v keys=%d missing=%v failed=%v healthy=%v",
			strings.Trim(m.repoPrefix, "/"), reachable, counts, len(keys), missing, failed, m.healthSnapshot())
	}
}
