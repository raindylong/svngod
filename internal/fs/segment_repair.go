package fs

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	"svngod/internal/fs/store"
)

// Segment indexes can be lost or lose one entry: the index is a mutable object
// rewritten under the commit lease, so a crashed writer, a backend that accepted
// a PUT it then lost, or an operator mistake can leave a shard whose revisions
// are no longer reachable even though every segment object is intact.
//
// Segments are written *before* the index and are immutable, so the objects are
// the ground truth. An index can therefore be rebuilt from them, and a read that
// finds a revision missing from the index can repair the shard in place.

// segmentRepairBackoff stops a broken or permission-denied shard from turning
// every read into a listing plus rewrite.
const segmentRepairBackoff = 30 * time.Second

// segmentIndexCacheKey is the in-process cache key of a shard index body.
func segmentIndexCacheKey(sh uint64) string { return fmt.Sprintf("segi/%d", sh) }

// parseSegmentKey extracts first/count from ".../<first>-<count>.seg".
func parseSegmentKey(key string) (uint64, uint64, bool) {
	base := key
	if i := strings.LastIndexByte(base, '/'); i >= 0 {
		base = base[i+1:]
	}
	if !strings.HasSuffix(base, ".seg") {
		return 0, 0, false
	}
	base = strings.TrimSuffix(base, ".seg")
	parts := strings.Split(base, "-")
	if len(parts) != 2 || len(parts[0]) != 12 || len(parts[1]) != 12 {
		return 0, 0, false
	}
	first, err1 := strconv.ParseUint(parts[0], 10, 64)
	count, err2 := strconv.ParseUint(parts[1], 10, 64)
	if err1 != nil || err2 != nil || count == 0 {
		return 0, 0, false
	}
	return first, count, true
}

// rebuildSegmentIndex reconstructs one shard's index from its segment objects.
// Each object's digest is recomputed (the index stores it, and verify depends on
// it). Overlapping ranges are rejected rather than guessed: writing a wrong
// index would be worse than leaving the shard unrepaired.
// maxRev bounds the rebuild to revisions that actually exist. Segments beyond
// HEAD were written by a writer that never published them (index written, HEAD
// CAS never ran, or the process died); keeping them would block the retry at
// those revision numbers, so they are dropped.
func (r *Repo) rebuildSegmentIndex(ctx context.Context, sh, maxRev uint64) ([]segmentRef, error) {
	prefix := r.keys.SegmentDir(sh)
	next := ""
	var refs []segmentRef
	for {
		objs, cont, err := r.store.ListPrefix(ctx, prefix, next, 1000)
		if err != nil {
			return nil, err
		}
		for _, o := range objs {
			first, count, ok := parseSegmentKey(o.Key)
			if !ok {
				continue
			}
			b, gerr := r.store.Get(ctx, o.Key, nil)
			if gerr != nil {
				return nil, fmt.Errorf("rebuild index %d: read %s: %w", sh, o.Key, gerr)
			}
			// The header must agree with the name; a mismatch means the object
			// was written by something other than this format.
			if len(b) < segHeaderLen || string(b[:8]) != segmentMagic {
				return nil, fmt.Errorf("rebuild index %d: %s is not a segment", sh, o.Key)
			}
			hf := binary.BigEndian.Uint64(b[12:20])
			hc := binary.BigEndian.Uint64(b[20:28])
			if hf != first || hc != count {
				return nil, fmt.Errorf("rebuild index %d: %s holds %d+%d", sh, o.Key, hf, hc)
			}
			sum := sha256.Sum256(b)
			refs = append(refs, segmentRef{First: first, Count: count, SHA256: hex.EncodeToString(sum[:])})
		}
		if cont == "" || cont == next {
			break
		}
		next = cont
	}
	// A compaction that merged a shard into one wide segment can leave the
	// superseded narrow segments behind (deletion is delayed, and a CLI GC that
	// exits never runs it). Those objects are still valid: every revision's
	// block/props are immutable and identical in both copies, so the widest
	// segment covering a range is authoritative and the contained ones are
	// dropped. Partial overlaps are impossible in this layout and indicate
	// corruption, so they are refused instead of guessed.
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].First != refs[j].First {
			return refs[i].First < refs[j].First
		}
		return refs[i].Count > refs[j].Count
	})
	merged := refs[:0]
	for _, ref := range refs {
		if ref.First > maxRev {
			continue
		}
		if len(merged) == 0 {
			merged = append(merged, ref)
			continue
		}
		last := &merged[len(merged)-1]
		lastEnd := last.First + last.Count
		refEnd := ref.First + ref.Count
		if ref.First >= lastEnd {
			merged = append(merged, ref)
			continue
		}
		if refEnd <= lastEnd {
			continue // contained in the wider segment
		}
		if ref.First <= last.First && refEnd >= lastEnd {
			*last = ref
			continue
		}
		return nil, fmt.Errorf("rebuild index %d: ranges %d+%d and %d+%d overlap partially",
			sh, last.First, last.Count, ref.First, ref.Count)
	}
	return merged, nil
}

// repairSegmentIndex rebuilds and stores one shard's index under the commit
// lease, so a concurrent commit cannot have its freshly appended entry dropped
// by the rewrite. Returns the repaired entries.
func (r *Repo) repairSegmentIndex(ctx context.Context, sh uint64) ([]segmentRef, error) {
	head, err := r.HeadFresh()
	if err != nil {
		return nil, err
	}
	lease, err := r.acquireCommitLease(ctx)
	if err != nil {
		return nil, err
	}
	defer lease.Release(r.store)
	refs, err := r.rebuildSegmentIndex(ctx, sh, head)
	if err != nil {
		return nil, err
	}
	if len(refs) == 0 {
		// Every object in this shard is beyond HEAD: an unpublished write.
		// Leave no index behind rather than an empty one.
		return nil, nil
	}
	// Refuse to narrow the index: if a segment object is missing, the rebuilt
	// list stops short of HEAD and writing it would hide the loss behind
	// "revision not in index". The previous index is left in place so the error
	// names the revision that cannot be read.
	if !segmentIndexCovers(refs, r.segmentStart(sh), r.shardLast(sh, head)) {
		return nil, fmt.Errorf("shard %d: segment objects do not cover up to HEAD (%d)", sh, head)
	}
	_, etag, existed, err := r.loadSegmentIndexETag(ctx, sh)
	if err != nil {
		return nil, err
	}
	if err := r.writeSegmentIndexCAS(ctx, sh, refs, etag, existed); err != nil {
		return nil, err
	}
	return refs, nil
}

// repairSegmentIndexOnce rate-limits read-triggered repairs per shard.
func (r *Repo) repairSegmentIndexOnce(sh uint64) {
	r.segRepairMu.Lock()
	if r.segRepairAt == nil {
		r.segRepairAt = map[uint64]time.Time{}
	}
	if t, ok := r.segRepairAt[sh]; ok && time.Since(t) < segmentRepairBackoff {
		r.segRepairMu.Unlock()
		return
	}
	r.segRepairAt[sh] = time.Now()
	r.segRepairMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if _, err := r.repairSegmentIndex(ctx, sh); err != nil {
		log.Printf("segment: repair index %d: %v", sh, err)
	}
}

// RepairSegmentIndexes rebuilds every shard index up to HEAD and reports how
// many differed from what was stored.
func (r *Repo) RepairSegmentIndexes(ctx context.Context) (int, error) {
	if !r.segments {
		return 0, nil
	}
	head, err := r.HeadFresh()
	if err != nil {
		return 0, err
	}
	repaired := 0
	for sh := uint64(0); sh <= r.shard(head); sh++ {
		before, _ := r.loadSegmentIndex(sh)
		refs, rerr := r.repairSegmentIndex(ctx, sh)
		if rerr != nil {
			return repaired, rerr
		}
		if !sameSegmentRefs(before, refs) {
			repaired++
		}
	}
	return repaired, nil
}

// shardLast is the last revision a shard must cover, bounded by HEAD.
func (r *Repo) shardLast(sh, head uint64) uint64 {
	last := (sh+1)*r.shardSz - 1
	if last > head {
		last = head
	}
	return last
}

// segmentIndexCovers reports whether refs continue the shard from start through
// last without gaps or overlaps.
func segmentIndexCovers(refs []segmentRef, start, last uint64) bool {
	if last < start {
		return true
	}
	next := start
	for _, ref := range refs {
		if ref.First != next {
			return false
		}
		next = ref.First + ref.Count
	}
	return next > last
}

func sameSegmentRefs(a, b []segmentRef) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// RepairRevPropsOverrides drops override-list entries whose object is gone.
// Reads refuse to serve the committed value for a listed-but-missing override
// (silently reverting an edit), so the list must be reconciled: either the
// object is restored from a replica first, or the dead entry is removed. This
// is the explicit operator action for the latter.
func (r *Repo) RepairRevPropsOverrides(ctx context.Context) (int, error) {
	if !r.segments {
		return 0, nil
	}
	head, err := r.HeadFresh()
	if err != nil {
		return 0, err
	}
	dropped := 0
	for sh := uint64(0); sh <= r.shard(head); sh++ {
		listKey := r.keys.SegRevPropsIndex(sh)
		for attempt := 0; attempt < 8; attempt++ {
			b, gerr := r.store.Get(ctx, listKey, nil)
			if gerr != nil {
				if errors.Is(gerr, store.ErrNotFound) {
					break
				}
				return dropped, gerr
			}
			var revs []uint64
			if len(b) > 0 {
				if uerr := json.Unmarshal(b, &revs); uerr != nil {
					return dropped, fmt.Errorf("shard %d override list: %w", sh, uerr)
				}
			}
			kept := revs[:0]
			for _, rev := range revs {
				if _, ok, oerr := r.loadSegRevPropsRaw(ctx, sh, rev); oerr != nil {
					return dropped, oerr
				} else if ok {
					kept = append(kept, rev)
					continue
				}
				dropped++
			}
			if len(kept) == len(revs) {
				break
			}
			data, merr := json.Marshal(kept)
			if merr != nil {
				return dropped, merr
			}
			if _, cerr := r.store.CAS(ctx, listKey, store.ContentETag(b), data); cerr != nil {
				if errors.Is(cerr, store.ErrPreconditionFailed) {
					continue
				}
				return dropped, cerr
			}
			r.invalidateSegOverrides(sh)
			break
		}
	}
	return dropped, nil
}
