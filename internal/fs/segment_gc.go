package fs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"
)

// segmentOrphanAge is the minimum age of an unreferenced segment object before
// GC considers deleting it. The delete phase runs under the commit lease, which
// is what actually guarantees that no compaction is mid-flight; the age filter
// only avoids churning objects this pass just listed.
const segmentOrphanAge = time.Minute

// CompactSegments merges every shard whose index holds more than one segment.
// Shard-completion compaction only fires when a shard fills, so a repository
// that stops mid-shard (or was migrated shard by shard) keeps many small
// segments and pays one GET per segment on every read. Each shard is merged
// under the commit lease, so a concurrent commit can neither be lost nor
// observe a half-written index.
func (r *Repo) CompactSegments(ctx context.Context) (int, error) {
	return r.CompactSegmentsProgress(ctx, nil)
}

// CompactSegmentsProgress is CompactSegments with per-shard progress.
func (r *Repo) CompactSegmentsProgress(ctx context.Context, progress GCProgress) (int, error) {
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
		if progress != nil {
			progress("segments", int(sh)+1, int(r.shard(head))+1)
		}
	}
	return merged, nil
}

// CollectOrphanSegments deletes segment objects no shard index references and
// that are older than grace (raised to segmentOrphanAge). Orphans come from a
// compaction that died after writing its merged object but before swapping the
// index. Indexes are read fresh: the sweep must never delete a segment a live
// compaction is about to publish.
func (r *Repo) CollectOrphanSegments(ctx context.Context, grace time.Duration) (int, error) {
	return r.CollectOrphanSegmentsProgress(ctx, grace, nil)
}

// CollectOrphanSegmentsProgress is CollectOrphanSegments with progress.
func (r *Repo) CollectOrphanSegmentsProgress(ctx context.Context, grace time.Duration, progress GCProgress) (int, error) {
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
	// List candidates without holding any lock: a listing can take a while on a
	// large repository and must not block commits.
	prefix := r.keys.ListingPrefix("segs")
	var candidates []string
	next := ""
	for {
		objs, cont, err := r.store.ListPrefix(ctx, prefix, next, 1000)
		if err != nil {
			return 0, err
		}
		for _, o := range objs {
			if !strings.HasSuffix(o.Key, ".seg") {
				continue
			}
			if o.ModTime.IsZero() || time.Since(o.ModTime) < grace {
				continue
			}
			candidates = append(candidates, normalizeListKey(o.Key))
		}
		if cont == "" || cont == next {
			break
		}
		next = cont
	}
	if len(candidates) == 0 {
		return 0, nil
	}
	// Deletion must not race a live compaction: it writes the merged object
	// before swapping the index, so an object can be unreferenced yet about to
	// become the published copy. Holding the commit lease excludes exactly that
	// window; the index set is re-read while it is held.
	lease, err := r.acquireCommitLease(ctx)
	if err != nil {
		log.Printf("segment: orphan sweep skipped, commit lease busy: %v", err)
		return 0, nil
	}
	defer lease.Release(r.store)
	keep := map[string]bool{}
	for sh := uint64(0); sh <= r.shard(head); sh++ {
		refs, err := r.loadSegmentIndex(sh)
		if err != nil {
			return 0, err
		}
		for _, ref := range refs {
			keep[normalizeListKey(r.segmentKeyFor(ref.First, ref.Count))] = true
		}
	}
	deleted := 0
	for start := 0; start < len(candidates); start += 500 {
		end := start + 500
		if end > len(candidates) {
			end = len(candidates)
		}
		batch := candidates[start:end]
		victims := make([]string, 0, len(batch))
		for _, k := range batch {
			if !keep[k] {
				victims = append(victims, k)
			}
		}
		if len(victims) == 0 {
			continue
		}
		if err := r.deletePrimaryOrAll(ctx, victims); err != nil {
			return deleted, err
		}
		deleted += len(victims)
		if progress != nil {
			progress("orphans", deleted, len(candidates))
		}
	}
	if progress != nil {
		progress("orphans", deleted, len(candidates))
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

// normalizeListKey puts a key in the form Store.ListPrefix returns (relative to
// the store prefix, no leading slash), so references and listed objects compare
// equal for scoped stores whose repository prefix is empty.
func normalizeListKey(k string) string { return strings.TrimPrefix(k, "/") }

// ReplicaDigest returns a stable digest of what makes two replicas of the same
// repository equivalent for reads: the format parameters, every shard index and
// every revision-property override (list and content). `svnadmin replicas`
// compares it across members to turn silent drift into an explicit error.
func (r *Repo) ReplicaDigest() (string, error) {
	head, err := r.HeadFresh()
	if err != nil {
		return "", err
	}
	h := sha256.New()
	fmt.Fprintf(h, "segments=%v shard=%d from=%d target=%d head=%d\n",
		r.segments, r.shardSz, r.segmentFrom, r.segmentTarget, head)
	if !r.segments {
		return hex.EncodeToString(h.Sum(nil)), nil
	}
	ctx := context.Background()
	for sh := uint64(0); sh <= r.shard(head); sh++ {
		refs, err := r.loadSegmentIndex(sh)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(h, "%d:", sh)
		for _, ref := range refs {
			fmt.Fprintf(h, "%d+%d+%s,", ref.First, ref.Count, ref.SHA256)
		}
		overrides, err := r.segRevPropsOverrides(sh, true)
		if err != nil {
			return "", err
		}
		revs := make([]uint64, 0, len(overrides))
		for rev := range overrides {
			revs = append(revs, rev)
		}
		sort.Slice(revs, func(i, j int) bool { return revs[i] < revs[j] })
		fmt.Fprintf(h, " ov=%v", revs)
		for _, rev := range revs {
			b, ok, rerr := r.loadSegRevPropsRaw(ctx, sh, rev)
			if rerr != nil {
				return "", rerr
			}
			if !ok {
				fmt.Fprintf(h, " %d:missing", rev)
				continue
			}
			sum := sha256.Sum256(b)
			fmt.Fprintf(h, " %d:%x", rev, sum)
		}
		h.Write([]byte("\n"))
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
