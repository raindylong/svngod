package fs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
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
	// Index and per-batch WAL. The commit lease serialises writers, so a plain
	// read-modify-write of the small index is safe.
	for _, g := range groups {
		refs, err := r.loadSegmentIndex(g.shard)
		if err != nil {
			for _, p := range plans {
				errs[p.idx] = err
			}
			return revs, errs, err
		}
		refs = appendSegmentRef(refs, g.ref)
		if err := r.store.Put(ctx, r.keys.SegmentIndex(g.shard), encodeSegmentIndex(refs)); err != nil {
			for _, p := range plans {
				errs[p.idx] = err
			}
			return revs, errs, err
		}
		if err := r.writeSegmentJournal(ctx, g.first, g.count, g.parent, lease.token, created, r.segmentKeyFor(g.first, g.count), g.sha); err != nil {
			for _, p := range plans {
				errs[p.idx] = err
			}
			return revs, errs, err
		}
	}
	last := plans[len(plans)-1].rev
	nc := &Current{Head: last, Created: created}
	if _, cerr := r.store.CAS(ctx, r.keys.Current(), etag, nc.Marshal()); cerr != nil {
		if !errors.Is(cerr, store.ErrPreconditionFailed) {
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
	}
	r.setHead(last)
	r.cache.Clear()

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

// writeSegmentJournal records one batch in the WAL (version 2).
func (r *Repo) writeSegmentJournal(ctx context.Context, first, count, parent uint64, lease string,
	created int64, key, sha string) error {
	if !r.journal {
		return nil
	}
	rec := &CommitRecord{
		Version:    2,
		Rev:        first,
		Parent:     parent,
		Lease:      lease,
		Created:    created,
		SegmentKey: key,
		SegmentSHA: sha,
		First:      first,
		Count:      count,
		Quorum:     r.writeQuorum,
	}
	if err := r.store.Put(ctx, r.keys.Journal(first), EncodeCommitRecord(rec)); err != nil {
		return fmt.Errorf("journal: %w", err)
	}
	return nil
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
	key := cache.Key(fmt.Sprintf("segi/%d", sh))
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
	refs, err := r.loadSegmentIndex(sh)
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
	if err := r.store.Put(ctx, r.keys.SegmentIndex(sh), encodeSegmentIndex([]segmentRef{ref})); err != nil {
		return err
	}
	cachePut(r.cache, cache.Key(fmt.Sprintf("segi/%d", sh)), encodeSegmentIndex([]segmentRef{ref}))
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
