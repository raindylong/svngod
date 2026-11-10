package fs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"svngod/internal/cache"
	"svngod/internal/fs/store"
)

// segmentRefs splits a batch into shard-local groups and writes one segment per
// shard. It updates each shard's index and writes one v2 WAL record per group,
// then publishes the whole batch with a single HEAD CAS.
func (r *Repo) publishSegmentPlans(ctx context.Context, lease *commitLease, cur *Current, etag string,
	plans []batchPlan, created int64, revs []uint64, errs []error) ([]uint64, []error, error) {
	type group struct {
		shard  uint64
		seg    *segmentData
		ref    segmentRef
		sha    string
		first  uint64
		count  uint64
		parent uint64
	}
	var groups []*group
	for i := 0; i < len(plans); {
		sh := r.shard(plans[i].rev)
		g := &group{shard: sh, first: plans[i].rev, parent: plans[i].rev - 1}
		g.seg = &segmentData{First: g.first, Parent: g.parent}
		for i < len(plans) && r.shard(plans[i].rev) == sh {
			g.seg.Blocks = append(g.seg.Blocks, plans[i].rb)
			g.seg.Props = append(g.seg.Props, plans[i].rp)
			g.count++
			i++
		}
		g.seg.Count = g.count
		groups = append(groups, g)
	}

	// Segments first: nothing is visible until the HEAD CAS below.
	for _, g := range groups {
		data := encodeSegment(g.seg)
		sum := sha256.Sum256(data)
		g.sha = hex.EncodeToString(sum[:])
		g.ref = segmentRef{First: g.first, Count: g.count, SHA256: g.sha}
		if err := r.store.Put(ctx, r.segmentKeyFor(g.first, g.count), data); err != nil {
			for _, p := range plans {
				errs[p.idx] = err
			}
			return revs, errs, err
		}
	}
	if verr := lease.Verify(ctx, r.store); verr != nil {
		for _, p := range plans {
			errs[p.idx] = verr
		}
		return revs, errs, verr
	}
	// The shard index is a mutable object rewritten from a read-modify-write.
	// The commit lease normally serialises those writes, but a process that
	// stalls past the lease TTL must not be able to drop another writer's entry:
	// every rewrite is therefore a compare-and-set on the index ETag, and a
	// range already claimed by a different batch is refused instead of
	// overwritten. WAL records are immutable unique keys and are written in
	// parallel after the indexes.
	var journals []segmentJournalWrite
	for _, g := range groups {
		journals = append(journals, segmentJournalWrite{
			first: g.first, count: g.count,
			key: r.segmentKeyFor(g.first, g.count), sha: g.sha,
		})
	}
	batchFirst := plans[0].rev
	batchCount := plans[len(plans)-1].rev - batchFirst + 1
	// One group per shard, so their index updates touch different objects and
	// can run concurrently. A commit that crosses a shard boundary (large
	// batch) otherwise pays the index round trips twice in sequence.
	written := make([]segmentRef, len(groups))
	appendErrs := make([]error, len(groups))
	if len(groups) == 1 {
		appendErrs[0] = r.appendSegmentIndex(ctx, groups[0].ref)
		written[0] = groups[0].ref
	} else {
		var wg sync.WaitGroup
		for i, g := range groups {
			written[i] = g.ref
			wg.Add(1)
			go func(i int, ref segmentRef) {
				defer wg.Done()
				appendErrs[i] = r.appendSegmentIndex(ctx, ref)
			}(i, g.ref)
		}
		wg.Wait()
	}
	for _, aerr := range appendErrs {
		if aerr == nil {
			continue
		}
		for i, ref := range written {
			if appendErrs[i] == nil {
				r.rollbackSegmentIndex(ctx, ref)
			}
		}
		for _, p := range plans {
			errs[p.idx] = aerr
		}
		return revs, errs, aerr
	}
	if err := r.writeSegmentJournalBatch(ctx, batchFirst, batchCount, journals, lease.token, cur.Head, created); err != nil {
		for _, ref := range written {
			r.rollbackSegmentIndex(ctx, ref)
		}
		for _, p := range plans {
			errs[p.idx] = err
		}
		return revs, errs, err
	}
	last := plans[len(plans)-1].rev
	nc := &Current{Head: last, Created: created}
	if _, cerr := r.store.CAS(ctx, r.keys.Current(), etag, nc.Marshal()); cerr != nil {
		if !errors.Is(cerr, store.ErrPreconditionFailed) {
			// Nothing was published; drop our index entries so a retry at the
			// same revisions is not blocked by an unpublished entry.
			for _, ref := range written {
				r.rollbackSegmentIndex(ctx, ref)
			}
			return revs, errs, cerr
		}
		now := cur.Head
		if c, _, rerr := r.readCurrentStrict(); rerr == nil {
			now = c.Head
		}
		for _, g := range groups {
			owned := now >= g.first+g.count-1 && r.segmentOwns(ctx, g.ref, lease.token, g.parent)
			if owned {
				continue
			}
			for _, p := range plans {
				if p.rev >= g.first && p.rev < g.first+g.count && p.rev > now {
					errs[p.idx] = store.ErrQuorumUnmet
				}
			}
		}
		if now < last {
			last = now
		}
		r.setHead(last)
		r.cache.Clear()
		// Our unpublished entries above the new HEAD are dead weight: another
		// writer reuses those revision numbers and would be blocked by them.
		for _, ref := range written {
			if last < ref.First+ref.Count-1 {
				r.rollbackSegmentIndex(ctx, ref)
			}
		}
		return revs, errs, nil
	}
	r.setHead(last)
	r.cache.Clear()
	// The batch is the published truth now. Re-check its index entries: a lease
	// violator that rewrote the index between our CAS and the HEAD publication
	// must not be able to leave HEAD pointing at revisions the index no longer
	// names.
	r.ensureIndexRefs(written)

	// Compact a shard once its last revision is published. The merge runs in
	// the background: it is O(shard size), and the commit that closed the shard
	// must not wait for it (a client timeout would cancel it midway).
	for _, g := range groups {
		if (g.first+g.count)%r.shardSz == 0 {
			r.enqueueCompaction(g.shard)
		}
	}
	return revs, errs, nil
}

// initCompaction prepares the background compactor for segment repositories.
// The worker starts on the first enqueue so v1 repositories spawn no goroutine.
func (r *Repo) initCompaction() {
	if r.segments && r.compactQ == nil {
		r.compactQ = make(chan uint64, 256)
	}
	if r.segments {
		if r.segMetas == nil {
			r.segMetas = newSegMetaCache(256)
		}
		if r.segDecoded == nil {
			r.segDecoded = newSegDecodeCache(2)
		}
	}
}

// enqueueCompaction schedules a completed shard. A full queue drops the shard
// (compaction is an optimisation, never required for correctness) and the next
// GC pass or the next shard completion retries it.
func (r *Repo) enqueueCompaction(sh uint64) {
	if r.compactQ == nil {
		return
	}
	r.compactOnce.Do(func() { go r.compactLoop() })
	select {
	case r.compactQ <- sh:
	default:
		log.Printf("segment: compaction queue full, shard %d postponed", sh)
	}
}

func (r *Repo) compactLoop() {
	for sh := range r.compactQ {
		r.runCompaction(sh)
	}
}

// runCompaction merges one shard, retrying while another process holds the
// commit lease.
func (r *Repo) runCompaction(sh uint64) {
	backoff := time.Second
	for attempt := 0; attempt < 6; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		err := r.compactCompletedShard(ctx, sh)
		cancel()
		if err == nil {
			return
		}
		if !errors.Is(err, ErrCommitBusy) {
			log.Printf("segment: compact shard %d: %v", sh, err)
			return
		}
		time.Sleep(backoff)
		backoff *= 2
	}
	log.Printf("segment: compact shard %d: commit lease busy, postponed", sh)
}

// compactCompletedShard merges a shard only after HEAD has passed its end and
// only under the commit lease: the lease serialises the index read-modify-write
// with concurrent commits, so a merge can never drop a newly published batch
// (the old code merged inline for the same reason).
func (r *Repo) compactCompletedShard(ctx context.Context, sh uint64) error {
	cur, _, err := r.readCurrentStrict()
	if err != nil {
		return err
	}
	// A shard spans revisions sh*shardSz .. (sh+1)*shardSz-1, so it is complete
	// once HEAD reached its last revision.
	forShard := (sh+1)*r.shardSz - 1
	if cur.Head < forShard {
		return nil
	}
	lease, err := r.acquireCommitLease(ctx)
	if err != nil {
		return err
	}
	defer lease.Release(r.store)
	if cur, _, cerr := r.readCurrentStrict(); cerr != nil || cur.Head < forShard {
		return cerr
	}
	r.journalMu.Lock()
	defer r.journalMu.Unlock()
	return r.compactSegmentShard(ctx, sh)
}

// appendSegmentRef inserts a ref keeping the index ordered and deduplicated.
func appendSegmentRef(refs []segmentRef, ref segmentRef) []segmentRef {
	for i := range refs {
		if refs[i].First == ref.First {
			refs[i] = ref
			return refs
		}
		if refs[i].First > ref.First {
			refs = append(refs, segmentRef{})
			copy(refs[i+1:], refs[i:])
			refs[i] = ref
			return refs
		}
	}
	return append(refs, ref)
}

// segmentOwns reports whether the published segment record still belongs to
// this batch (same lease token, segment key and parent).
func (r *Repo) segmentOwns(ctx context.Context, ref segmentRef, token string, parent uint64) bool {
	if !r.journal {
		return true
	}
	b, err := r.store.Get(ctx, r.keys.Journal(ref.First), nil)
	if err != nil {
		return false
	}
	rec, derr := DecodeCommitRecord(b)
	if derr != nil || rec.Version != 2 || rec.Lease != token ||
		rec.First != ref.First || rec.Count != ref.Count ||
		rec.SegmentSHA != ref.SHA256 || rec.Parent != parent {
		return false
	}
	return true
}

// segmentEntry loads rev's block and props from its segment. fresh bypasses
// the index cache so a revision committed by another process is still found.
func (r *Repo) segmentEntry(rev uint64, fresh bool) ([]byte, []byte, error) {
	sh := r.shard(rev)
	refs, err := r.segmentIndex(sh, fresh)
	if err != nil {
		return nil, nil, err
	}
	ref, ok := findSegment(refs, rev)
	if !ok {
		if fresh {
			return nil, nil, fmt.Errorf("fs: revision %d not in shard %d segment index", rev, sh)
		}
		refs, err = r.segmentIndex(sh, true)
		if err != nil {
			return nil, nil, err
		}
		if ref, ok = findSegment(refs, rev); !ok {
			// The revision is below HEAD but missing from the index: the index
			// (mutable) lost an entry while the segment objects (immutable,
			// written first) are intact. Rebuild that shard once and retry.
			if head, herr := r.HeadFresh(); herr == nil && rev >= r.segmentFrom && rev <= head {
				r.repairSegmentIndexOnce(sh)
				if refs, err = r.segmentIndex(sh, true); err == nil {
					ref, ok = findSegment(refs, rev)
				}
			}
		}
		if !ok {
			return nil, nil, fmt.Errorf("fs: revision %d not in shard %d segment index", rev, sh)
		}
	}
	objKey := r.segmentKeyFor(ref.First, ref.Count)
	if r.fullReads.Load() {
		// Verify path: read the whole object once per segment so the index
		// SHA-256 is checked, then serve its revisions from memory.
		if seg := r.segDecoded.get(objKey); seg != nil {
			return segmentEntryAt(seg, rev, ref)
		}
		seg, err := r.verifySegmentFull(*ref)
		if err != nil {
			return nil, nil, err
		}
		r.segDecoded.put(objKey, seg)
		return segmentEntryAt(seg, rev, ref)
	}
	// Sequential scans (log, blame, dump) walk a segment revision by revision.
	// Detecting that turns the second read into one whole-segment GET that then
	// serves the rest from memory; isolated reads stay ranged.
	sequential := r.noteSegmentRead(objKey, rev)
	if seg := r.segDecoded.get(objKey); seg != nil {
		return segmentEntryAt(seg, rev, ref)
	}
	if !sequential {
		block, props, err := r.readSegmentEntry(context.Background(), *ref, rev-ref.First)
		if err == nil {
			return block, props, nil
		}
		if !errors.Is(err, errSegmentV1) {
			return nil, nil, err
		}
	}
	seg, err := r.loadSegment(*ref)
	if err != nil {
		return nil, nil, err
	}
	r.segDecoded.put(objKey, seg)
	return segmentEntryAt(seg, rev, ref)
}

// noteSegmentRead reports whether rev continues the previous read of this
// segment, and records it as the new tail.
func (r *Repo) noteSegmentRead(objKey string, rev uint64) bool {
	r.seqMu.Lock()
	defer r.seqMu.Unlock()
	seq := r.seqKey == objKey && r.seqNext == rev
	r.seqKey, r.seqNext = objKey, rev+1
	return seq
}

// segmentEntryAt extracts one revision from a decoded segment.
func segmentEntryAt(seg *segmentData, rev uint64, ref *segmentRef) ([]byte, []byte, error) {
	i := rev - seg.First
	if i >= uint64(len(seg.Blocks)) {
		return nil, nil, fmt.Errorf("fs: revision %d outside segment %d+%d", rev, ref.First, ref.Count)
	}
	// Copy: a subslice keeps the whole (possibly multi-megabyte) segment
	// backing array alive, and the block cache would then hold one segment per
	// cached revision (verify of a 20k-revision repository OOMs).
	block := append([]byte(nil), seg.Blocks[i]...)
	props := append([]byte(nil), seg.Props[i]...)
	return block, props, nil
}

func (r *Repo) segmentIndex(sh uint64, fresh bool) ([]segmentRef, error) {
	key := cache.Key(segmentIndexCacheKey(sh))
	if !fresh {
		if b, ok := r.cache.Get(key); ok {
			return decodeSegmentIndex(b)
		}
	}
	b, err := r.store.Get(context.Background(), r.keys.SegmentIndex(sh), nil)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	cachePut(r.cache, key, b)
	return decodeSegmentIndex(b)
}

// segmentKeyFor derives a segment's object key from its range, so indexes and
// WAL records stay relative to the repository prefix (a scoped store may add
// its own prefix; storing absolute keys double-prefixes them).
func (r *Repo) segmentKeyFor(first, count uint64) string {
	return r.keys.Segment(r.shard(first), first, count)
}

func (r *Repo) loadSegment(ref segmentRef) (*segmentData, error) {
	objKey := r.segmentKeyFor(ref.First, ref.Count)
	key := cache.Key("segd/" + objKey)
	if b, ok := r.cache.Get(key); ok {
		return decodeSegment(b)
	}
	b, err := r.store.Get(context.Background(), objKey, nil)
	if err != nil {
		return nil, err
	}
	if ref.SHA256 != "" {
		sum := sha256.Sum256(b)
		if hex.EncodeToString(sum[:]) != ref.SHA256 {
			return nil, fmt.Errorf("fs: segment %s checksum mismatch", objKey)
		}
	}
	cachePut(r.cache, key, b)
	return decodeSegment(b)
}

// compactSegmentShard merges every segment of a completed shard into one.
// Old segment keys are deleted after a grace period; readers resolving them
// through an older index still succeed during that window.
func (r *Repo) compactSegmentShard(ctx context.Context, sh uint64) error {
	for attempt := 0; attempt < 3; attempt++ {
		err := r.compactSegmentShardOnce(ctx, sh)
		if errors.Is(err, ErrSegmentIndexConflict) {
			continue
		}
		return err
	}
	return fmt.Errorf("%w: compact shard %d", ErrSegmentIndexConflict, sh)
}

// compactSegmentShardOnce merges the shard once. The index swap is a CAS: a
// concurrent commit that appended an entry between the read and the write makes
// the swap fail instead of dropping that commit's segment from the index.
func (r *Repo) compactSegmentShardOnce(ctx context.Context, sh uint64) error {
	refs, etag, existed, err := r.loadSegmentIndexETag(ctx, sh)
	if err != nil || len(refs) <= 1 {
		return err
	}
	var first, last uint64
	var total uint64
	for i, ref := range refs {
		if i == 0 {
			first = ref.First
		}
		if end := ref.First + ref.Count - 1; end > last {
			last = end
		}
		total += ref.Count
	}
	// A contiguous index is the invariant every commit maintains. Refusing a
	// gapped one protects against merging holes into a segment that no reader
	// could decode; the hole itself must be repaired from the WAL instead.
	if last-first+1 != total {
		return fmt.Errorf("segment: shard %d index has a gap (%d entries, span %d-%d)", sh, total, first, last)
	}
	merged := &segmentData{First: first, Count: last - first + 1, Parent: first - 1}
	for _, ref := range refs {
		seg, serr := r.loadSegment(ref)
		if serr != nil {
			return serr
		}
		for i := uint64(0); i < seg.Count; i++ {
			idx := ref.First + i - first
			for uint64(len(merged.Blocks)) <= idx {
				merged.Blocks = append(merged.Blocks, nil)
				merged.Props = append(merged.Props, nil)
			}
			merged.Blocks[idx] = seg.Blocks[i]
			merged.Props[idx] = seg.Props[i]
		}
	}
	data := encodeSegment(merged)
	sum := sha256.Sum256(data)
	key := r.keys.Segment(sh, first, merged.Count)
	if err := r.store.Put(ctx, key, data); err != nil {
		return err
	}
	ref := segmentRef{First: first, Count: merged.Count, SHA256: hex.EncodeToString(sum[:])}
	if err := r.writeSegmentIndexCAS(ctx, sh, []segmentRef{ref}, etag, existed); err != nil {
		return err
	}
	old := make([]string, 0, len(refs))
	for _, ref := range refs {
		old = append(old, r.segmentKeyFor(ref.First, ref.Count))
	}
	go func() {
		time.Sleep(60 * time.Second)
		for _, k := range old {
			_ = r.store.Delete(context.Background(), []string{k})
		}
	}()
	return nil
}

// segmentJournalWrite is one prepared WAL record.
type segmentJournalWrite struct {
	first, count uint64
	key, sha     string
}

// ErrSegmentIndexConflict means another writer already owns the revision range
// in the shard index. The commit is refused rather than overwriting it; with a
// healthy commit lease this should never surface.
var ErrSegmentIndexConflict = errors.New("fs: segment index entry owned by another writer")

// loadSegmentIndexETag reads a shard index together with its ETag so the next
// write can be a compare-and-set. existed=false means the object is absent and
// the write must create it.
func (r *Repo) loadSegmentIndexETag(ctx context.Context, sh uint64) (refs []segmentRef, etag string, existed bool, err error) {
	b, err := r.store.Get(ctx, r.keys.SegmentIndex(sh), nil)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, "", false, nil
		}
		return nil, "", false, err
	}
	refs, err = decodeSegmentIndex(b)
	if err != nil {
		return nil, "", false, err
	}
	// The expected ETag is the content digest: FileStore, MemoryStore and
	// single-part S3 objects all use it, so one GET is enough. A gateway with
	// opaque ETags is handled inside Store.CAS, which falls back to hashing the
	// body it finds.
	return refs, store.ContentETag(b), true, nil
}

// writeSegmentIndexCAS stores one shard index only if it still has the ETag the
// caller read (or is still absent). A lost race returns ErrSegmentIndexConflict.
func (r *Repo) writeSegmentIndexCAS(ctx context.Context, sh uint64, refs []segmentRef, etag string, existed bool) error {
	data := encodeSegmentIndex(refs)
	key := r.keys.SegmentIndex(sh)
	var err error
	if existed {
		_, err = r.store.CAS(ctx, key, etag, data)
	} else {
		err = r.store.CreateIfAbsent(ctx, key, data)
	}
	if err != nil {
		if errors.Is(err, store.ErrPreconditionFailed) || errors.Is(err, store.ErrNotFound) {
			return ErrSegmentIndexConflict
		}
		return err
	}
	cachePut(r.cache, cache.Key(segmentIndexCacheKey(sh)), data)
	return nil
}

// exactSegment returns the entry that starts at first, if any.
func exactSegment(refs []segmentRef, first uint64) (segmentRef, bool) {
	for _, ref := range refs {
		if ref.First == first {
			return ref, true
		}
	}
	return segmentRef{}, false
}

// appendSegmentIndex inserts one ref with CAS retries. It refuses to write a
// ref whose range is already covered by a published entry, and refuses to write
// a ref that would leave the shard's covered range non-contiguous -- writing a
// broken index would turn a harmless race into unreadable revisions. Entries
// entirely above HEAD belong to a publish that never happened; the shard is
// rebuilt from its immutable objects (which drops them) and the append retried.
func (r *Repo) appendSegmentIndex(ctx context.Context, ref segmentRef) error {
	sh := r.shard(ref.First)
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		refs, etag, existed, err := r.loadSegmentIndexETag(ctx, sh)
		if err != nil {
			return err
		}
		if cur, ok := overlapSegment(refs, ref); ok {
			if cur == ref {
				return nil // our own earlier attempt
			}
			if head, herr := r.HeadFresh(); herr == nil && cur.First > head {
				if r.rebuildIndexInline(ctx, sh, head) {
					continue
				}
			}
			return fmt.Errorf("%w: revisions %d+%d overlap %d+%d", ErrSegmentIndexConflict,
				ref.First, ref.Count, cur.First, cur.Count)
		}
		newRefs := appendSegmentRef(refs, ref)
		if !segmentIndexContiguous(newRefs, r.segmentStart(sh)) {
			if head, herr := r.HeadFresh(); herr == nil {
				if r.rebuildIndexInline(ctx, sh, head) {
					continue
				}
			}
			return fmt.Errorf("%w: shard %d would become non-contiguous", ErrSegmentIndexConflict, sh)
		}
		if err := r.writeSegmentIndexCAS(ctx, sh, newRefs, etag, existed); err != nil {
			if errors.Is(err, ErrSegmentIndexConflict) {
				lastErr = err
				time.Sleep(time.Duration(attempt+1) * 20 * time.Millisecond)
				continue
			}
			return err
		}
		return nil
	}
	if lastErr == nil {
		lastErr = ErrSegmentIndexConflict
	}
	return fmt.Errorf("%w: shard %d", lastErr, sh)
}

// overlapSegment returns an entry whose range intersects ref.
func overlapSegment(refs []segmentRef, ref segmentRef) (segmentRef, bool) {
	end := ref.First + ref.Count
	for _, cur := range refs {
		if cur.First < end && ref.First < cur.First+cur.Count {
			return cur, true
		}
	}
	return segmentRef{}, false
}

// segmentStart is the first revision a shard's index must cover.
func (r *Repo) segmentStart(sh uint64) uint64 {
	if sh == 0 {
		return r.segmentFrom
	}
	return sh * r.shardSz
}

// segmentIndexContiguous reports whether refs cover [start, ...) without gaps
// or overlaps.
func segmentIndexContiguous(refs []segmentRef, start uint64) bool {
	next := start
	for _, ref := range refs {
		if ref.First != next {
			return false
		}
		next = ref.First + ref.Count
	}
	return true
}

// rebuildIndexInline rebuilds one shard from its objects and replacing the
// index, without acquiring the commit lease. Callers must already hold it (or
// be the only writer), which the publish path does.
func (r *Repo) rebuildIndexInline(ctx context.Context, sh, head uint64) bool {
	rebuilt, err := r.rebuildSegmentIndex(ctx, sh, head)
	if err != nil {
		log.Printf("segment: inline rebuild shard %d: %v", sh, err)
		return false
	}
	_, etag, existed, err := r.loadSegmentIndexETag(ctx, sh)
	if err != nil {
		return false
	}
	if err := r.writeSegmentIndexCAS(ctx, sh, rebuilt, etag, existed); err != nil {
		log.Printf("segment: inline rebuild shard %d: write: %v", sh, err)
		return false
	}
	return true
}

// rollbackSegmentIndex removes our ref after a publish aborted, so a retry at
// the same revision range is not blocked by an unpublished entry.
func (r *Repo) rollbackSegmentIndex(ctx context.Context, ref segmentRef) {
	sh := r.shard(ref.First)
	for attempt := 0; attempt < 3; attempt++ {
		refs, etag, existed, err := r.loadSegmentIndexETag(ctx, sh)
		if err != nil || !existed {
			return
		}
		kept := refs[:0]
		removed := false
		for _, cur := range refs {
			if cur.First == ref.First && cur.SHA256 == ref.SHA256 {
				removed = true
				continue
			}
			kept = append(kept, cur)
		}
		if !removed {
			return
		}
		if err := r.writeSegmentIndexCAS(ctx, sh, kept, etag, existed); err == nil {
			return
		} else if !errors.Is(err, ErrSegmentIndexConflict) {
			log.Printf("segment: roll back index %d: %v", sh, err)
			return
		}
	}
}

// ensureIndexRefs re-checks published entries after the HEAD CAS. If a lease
// violator rewrote the index in the meantime, the published HEAD still names
// our batch as the truth, so our entries are restored (and the event logged).
func (r *Repo) ensureIndexRefs(refs []segmentRef) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, ref := range refs {
		sh := r.shard(ref.First)
		// One GET: if our entry is there, nothing else to do. The restore path
		// (never taken in practice) pays for its own re-read.
		cur, err := r.loadSegmentIndex(sh)
		if err != nil {
			log.Printf("segment: post-publish check shard %d: %v", sh, err)
			continue
		}
		if ex, ok := exactSegment(cur, ref.First); ok && ex.SHA256 == ref.SHA256 {
			continue
		}
		if err := r.appendSegmentIndex(ctx, ref); err != nil {
			log.Printf("segment: restore index entry %d+%d: %v", ref.First, ref.Count, err)
			continue
		}
		log.Printf("segment: index entry %d+%d was overwritten during publish; restored", ref.First, ref.Count)
	}
}

// writeSegmentJournalBatch appends one WAL record describing every shard
// segment of the batch. A per-group record would let replay publish one shard
// of a two-shard batch after a crash, exposing a partially applied commit; one
// record makes the batch all-or-nothing.
func (r *Repo) writeSegmentJournalBatch(ctx context.Context, first, count uint64,
	groups []segmentJournalWrite, leaseToken string, parent uint64, created int64) error {
	if !r.journal || len(groups) == 0 {
		return nil
	}
	rec := &CommitRecord{
		Version: 2, Rev: first, First: first, Count: count,
		Parent: parent, Lease: leaseToken, Created: created,
		SegmentKey: groups[0].key, SegmentSHA: groups[0].sha,
		Quorum: r.writeQuorum,
	}
	for _, g := range groups {
		rec.Groups = append(rec.Groups, CommitGroup{
			First: g.first, Count: g.count, SegmentKey: g.key, SegmentSHA: g.sha,
		})
	}
	if err := r.store.Put(ctx, r.keys.Journal(first), EncodeCommitRecord(rec)); err != nil {
		return fmt.Errorf("journal: %w", err)
	}
	return nil
}
