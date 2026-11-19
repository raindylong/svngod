package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

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
	const recentWindow = 100
	// One shared budget for the whole reconciliation. A per-key 2-minute
	// timeout let a slow backend stall a repair pass for hours (100 keys x
	// 2 min), holding repairOnce and the lease, so the rejoining mirror never
	// became healthy.
	budget, budgetCancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer budgetCancel()
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
		if budget.Err() != nil {
			return
		}
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
		ctx, cancel := context.WithTimeout(budget, 10*time.Second)
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
			// Compare before rewriting: the normal case is identical bytes, and
			// 100 unconditional PUTs per pass dominated recovery time on RustFS.
			if cur, gerr := m.mirrors[i].Get(ctx, k, nil); gerr == nil && bytes.Equal(cur, want) {
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
	var dests, sources []int
	size := int64(0)
	for i := range m.mirrors {
		if !reachable[i] {
			continue
		}
		if sz, has := sets[i][key]; has {
			sources = append(sources, i)
			if len(sources) == 1 {
				size = sz
			}
			continue
		}
		if !failed[i] {
			dests = append(dests, i)
		}
	}
	mu.Unlock()
	if len(sources) == 0 || len(dests) == 0 {
		return
	}
	for _, dst := range dests {
		var err error
		for _, src := range sources {
			if size > repairStreamThreshold {
				err = m.copyRepairStream(ctx, src, dst, key, size)
			} else {
				var b []byte
				if b, err = m.mirrors[src].Get(ctx, key, nil); err == nil {
					err = m.mirrors[dst].Put(ctx, key, b)
				}
			}
			if err == nil || !errors.Is(err, ErrNotFound) {
				break
			}
		}
		if errors.Is(err, ErrNotFound) {
			// The object was deleted after the listing (a lease-breaker marker
			// or an abandoned session). It exists nowhere, so no copy is
			// needed and this must not take a live mirror out of service.
			mu.Lock()
			for _, d := range dests {
				if missing[d] > 0 {
					missing[d]--
				}
			}
			mu.Unlock()
			return
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
// isLockStateKey reports whether key is lock/lease state, which a two-node
// pair keeps authoritative on the arbiter only (lease CAS and delete stop
// after the first copy).
func isLockStateKey(key string) bool {
	base := key[strings.LastIndex(key, "/")+1:]
	return strings.Contains(key, "/locks/") || base == "locks.json"
}

// reconcileLockKey reconciles lock state in a two-node pair. The arbiter is
// the only writer, so its copy wins; a copy found only on the standby is a
// leftover from before a release/outage and must be deleted, never promoted.
// Copying such a stale lease back to the arbiter made a live commit fail with
// "commit lease lost" (observed while one process recovered the arbiter and
// another was committing).
func (m *MirrorStore) reconcileLockKey(ctx context.Context, k string, sets []map[string]int64, reachable []bool) {
	if !reachable[0] {
		// Without the arbiter there is no authoritative view of lock state;
		// leave both copies alone until it answers again.
		return
	}
	if _, has := sets[0][k]; !has {
		for i := 1; i < len(m.mirrors); i++ {
			if !reachable[i] {
				continue
			}
			if _, has := sets[i][k]; !has {
				continue
			}
			if err := m.mirrors[i].Delete(ctx, []string{k}); err != nil {
				log.Printf("mirror[%d] repair drop stale lock %q: %v", i, k, err)
				reachable[i] = false
			}
		}
		return
	}
	b, err := m.mirrors[0].Get(ctx, k, nil)
	if err != nil {
		return
	}
	for i := 1; i < len(m.mirrors); i++ {
		if !reachable[i] {
			continue
		}
		cur, cerr := m.mirrors[i].Get(ctx, k, nil)
		if cerr == nil && bytes.Equal(cur, b) {
			continue
		}
		if err := m.mirrors[i].Put(ctx, k, b); err != nil {
			log.Printf("mirror[%d] repair lock %q: %v", i, k, err)
			reachable[i] = false
		}
	}
}

func (m *MirrorStore) reconcileMutable(ctx context.Context, k string, sets []map[string]int64, reachable []bool) {
	if m.ArbiterRequired() && isLockStateKey(k) {
		m.reconcileLockKey(ctx, k, sets, reachable)
		return
	}
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
	dbgCur := os.Getenv("SVNGOD_DEBUG_REPAIR") == "1" && k == m.controlName()
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
			if dbgCur {
				log.Printf("reconcile current: mirror[%d] has=%v order(best,cur)=%d err=%v", i, true, m.order(k, best, cur), err)
			}
			if err == nil && m.order(k, best, cur) <= 0 {
				continue // already the newest
			}
		} else if dbgCur {
			log.Printf("reconcile current: mirror[%d] missing in listing", i)
		}
		if dbgCur {
			log.Printf("reconcile current: writing mirror[%d] best=%q", i, string(best))
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

// repairListTimeout bounds one mirror's object listing. At 1M objects the
// listing takes far longer than the old 5s budget; a short timeout made a
// healthy peer look unreachable and triggered the stale-HEAD bug.
const repairListTimeout = 90 * time.Second

// repairCopyTimeout bounds the copy/reconcile phase of one pass. A full 1M
// object repair needs minutes; partial progress is kept and the next pass
// continues from the missing-object counts.
const repairCopyTimeout = 10 * time.Minute

// repairJoinGrace is how long a repair pass waits for workers to drain after
// their context has expired. The workers hold no lock the caller needs, and
// partial progress is already recorded, so the pass reports what it got rather
// than waiting for a store that ignores cancellation.
const repairJoinGrace = 30 * time.Second

// isMutableKey reports whether an object's content must always be mirrored
// from the primary (the only mutable object today is the HEAD `current`).
func isMutableKey(key string) bool {
	return strings.HasSuffix(key, "/current")
}

// repairPrefixes are the object classes a full repair must reconcile. Session
// and idempotency metadata (bundles/, dav-activities/, dav-txns/, txn/) is
// excluded: it is recreated on demand, and listing it made a repository with
// many uploads page through thousands of intent records — the scans that timed
// out on RustFS and kept a live standby out of service.
//
// journal/ is excluded for the same reason: it holds one record per revision,
// so a 40k-revision repository made every repair listing page through 40k
// keys and time out, leaving the standby permanently out of service and tens
// of thousands of revisions behind. A record only matters for the crash window
// before HEAD is published; an acknowledged revision is already described by
// its published HEAD and packs, and an unacknowledged one may be dropped.
// segs/ and seg-revprops/ are cheap to list (one object per batch or shard,
// not per revision) and are the only copy of a format-v2 repository's revisions
// and edited properties. Leaving them out meant a standby that missed a
// segment write was never repaired and would read a missing object after
// failover.
var repairPrefixes = []string{
	"revs/", "revs-loose/", "revprops/", "revprops-loose/",
	"texts/", "chunks/", "dirs/", "locks/", "segs/", "seg-revprops/",
}

// listRepairObjects lists the repository objects repair needs, without the
// ephemeral prefixes that dominate object counts in a long-lived repository.
func listRepairObjects(ctx context.Context, s Store, repoPrefix string) (map[string]int64, error) {
	base := strings.Trim(repoPrefix, "/")
	if base == "" {
		// Test/custom stores have no repository namespace; keep the whole-store
		// listing they expect.
		return listAll(ctx, s, "")
	}
	active, haveShard := activeShard(ctx, s, base)
	out := make(map[string]int64)
	for _, p := range repairPrefixes {
		prefix := base + "/" + p
		// Keep the trailing slash: "revs/" must not also match "revs-loose/",
		// which doubled the listing and made a 40k-revision repository time
		// out during repair.
		if haveShard && (p == "revs-loose/" || p == "revprops-loose/") {
			// Only the active shard still needs loose objects; completed
			// shards are served from their pack, and their loose copies may
			// not have been collected yet (gc is asynchronous). Listing them
			// was the remaining reason a large repository timed out.
			prefix += padShard(active)
		}
		ks, err := listAll(ctx, s, prefix)
		if err != nil {
			return nil, err
		}
		for k, v := range ks {
			out[k] = v
		}
	}
	// The commit lease and its breaker marker are arbiter-only, one-shot
	// objects. Listing them made repair try to copy a key that is deleted as
	// soon as the lease is released, and the resulting NotFound marked a live
	// standby out of service on every pass.
	for k := range out {
		if strings.HasSuffix(k, "/locks/commit") || strings.HasSuffix(k, "/locks/commit.break") {
			delete(out, k)
		}
	}
	for _, k := range []string{"format", "uuid", "current", "locks.json"} {
		ks, err := listAll(ctx, s, base+"/"+k)
		if err != nil {
			return nil, err
		}
		for key, v := range ks {
			out[key] = v
		}
	}
	if len(out) == 0 {
		// A custom/test repository may store objects directly under its prefix
		// (no standard revision/text tree). Fall back to the unscoped listing so
		// those stores keep the old behaviour; real repositories always have at
		// least the format/current markers.
		return listAll(ctx, s, base+"/")
	}
	return out, nil
}

// activeShard reads the repository's format and HEAD to find the shard whose
// loose revisions are still live. Returns ok=false for custom layouts.
func activeShard(ctx context.Context, s Store, base string) (uint64, bool) {
	fb, err := s.Get(ctx, base+"/format", nil)
	if err != nil {
		return 0, false
	}
	var shard uint64
	for _, line := range strings.Split(string(fb), "\n") {
		if strings.HasPrefix(line, "SHARD_SIZE=") {
			shard, _ = strconv.ParseUint(strings.TrimPrefix(line, "SHARD_SIZE="), 10, 64)
		}
	}
	cb, err := s.Get(ctx, base+"/current", nil)
	if err != nil {
		return 0, false
	}
	var head uint64
	for _, line := range strings.Split(string(cb), "\n") {
		if strings.HasPrefix(line, "HEAD=") {
			head, _ = strconv.ParseUint(strings.TrimPrefix(line, "HEAD="), 10, 64)
		}
	}
	if shard == 0 {
		return 0, false
	}
	return head / shard, true
}

// listAll returns every object under prefix with its size. Repair needs the
// size so large objects can be copied as streams instead of buffering them.
func listAll(ctx context.Context, s Store, prefix string) (map[string]int64, error) {
	keys := make(map[string]int64)
	next := ""
	// Do not force a trailing slash: control keys (current/format/uuid) are
	// exact objects, and appending "/" made them invisible to every repair
	// pass, so a standby's HEAD was never reconciled. Directory prefixes carry
	// their own trailing slash.
	p := strings.TrimPrefix(prefix, "/")
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
