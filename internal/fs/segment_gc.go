package fs

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// segmentOrphanAge is the minimum age of an unreferenced segment object before
// GC deletes it. Compaction writes the merged object before it swaps the index,
// so a copy that is still unreferenced after this long belongs to a compaction
// that died (a live one swaps within seconds).
const segmentOrphanAge = 30 * time.Minute

// CompactSegments merges every shard whose index holds more than one segment.
// Shard-completion compaction only fires when a shard fills, so a repository
// that stops mid-shard (or was migrated shard by shard) keeps many small
// segments and pays one GET per segment on every read. Each shard is merged
// under the commit lease, so a concurrent commit can neither be lost nor
// observe a half-written index.
func (r *Repo) CompactSegments(ctx context.Context) (int, error) {
	if !r.segments {
		return 0, nil
	}
	head, err := r.HeadFresh()
	if err != nil {
		return 0, err
	}
	merged := 0
	for sh := uint64(0); sh <= r.shard(head); sh++ {
		refs, err := r.loadSegmentIndex(sh)
		if err != nil {
			return merged, err
		}
		if len(refs) <= 1 {
			continue
		}
		lease, err := r.acquireCommitLease(ctx)
		if err != nil {
			return merged, err
		}
		refs, rerr := r.loadSegmentIndex(sh)
		if rerr == nil && len(refs) > 1 {
			rerr = r.compactSegmentShard(ctx, sh)
		}
		lease.Release(r.store)
		if rerr != nil {
			return merged, rerr
		}
		merged++
	}
	return merged, nil
}

// CollectOrphanSegments deletes segment objects no shard index references and
// that are older than grace (raised to segmentOrphanAge). Orphans come from a
// compaction that died after writing its merged object but before swapping the
// index. Indexes are read fresh: the sweep must never delete a segment a live
// compaction is about to publish.
func (r *Repo) CollectOrphanSegments(ctx context.Context, grace time.Duration) (int, error) {
	if !r.segments {
		return 0, nil
	}
	if grace < segmentOrphanAge {
		grace = segmentOrphanAge
	}
	head, err := r.HeadFresh()
	if err != nil {
		return 0, err
	}
	keep := map[string]bool{}
	for sh := uint64(0); sh <= r.shard(head); sh++ {
		refs, err := r.loadSegmentIndex(sh)
		if err != nil {
			return 0, err
		}
		for _, ref := range refs {
			keep[r.segmentKeyFor(ref.First, ref.Count)] = true
		}
	}
	prefix := r.keys.Prefix + "/segs/"
	deleted := 0
	next := ""
	for {
		objs, cont, err := r.store.ListPrefix(ctx, prefix, next, 1000)
		if err != nil {
			return deleted, err
		}
		victims := make([]string, 0, len(objs))
		for _, o := range objs {
			if !strings.HasSuffix(o.Key, ".seg") || keep[o.Key] {
				continue
			}
			if o.ModTime.IsZero() || time.Since(o.ModTime) < grace {
				continue
			}
			victims = append(victims, o.Key)
		}
		if len(victims) > 0 {
			if err := r.store.Delete(ctx, victims); err != nil {
				return deleted, err
			}
			deleted += len(victims)
		}
		if cont == "" || cont == next {
			break
		}
		next = cont
	}
	return deleted, nil
}

// verifySegmentIndexes checks that every shard index covers its revisions
// contiguously from segmentFrom through HEAD. A silent gap (a lost index write,
// a bad migration) leaves those revisions unreadable while HEAD and every
// still-referenced object look healthy, so verify reports it explicitly.
func (r *Repo) verifySegmentIndexes(head uint64) ([]VerifyIssue, error) {
	var issues []VerifyIssue
	for sh := uint64(0); sh <= r.shard(head); sh++ {
		refs, err := r.loadSegmentIndex(sh)
		if err != nil {
			issues = append(issues, VerifyIssue{Rev: sh * r.shardSz, Err: fmt.Errorf("segment index %d unreadable: %w", sh, err)})
			continue
		}
		want := sh * r.shardSz
		if want < r.segmentFrom {
			want = r.segmentFrom
		}
		last := (sh+1)*r.shardSz - 1
		if last > head {
			last = head
		}
		next := want
		for _, ref := range refs {
			if ref.Count == 0 {
				issues = append(issues, VerifyIssue{Rev: next, Err: fmt.Errorf("segment index %d has an empty entry at %d", sh, ref.First)})
				continue
			}
			if ref.First != next {
				issues = append(issues, VerifyIssue{Rev: next, Err: fmt.Errorf("segment index %d covers %d, want %d", sh, ref.First, next)})
			}
			if end := ref.First + ref.Count; end > next {
				next = end
			}
		}
		if next <= last {
			issues = append(issues, VerifyIssue{Rev: next, Err: fmt.Errorf("segment index %d ends at %d, HEAD needs %d", sh, next-1, last)})
		}
	}
	return issues, nil
}
