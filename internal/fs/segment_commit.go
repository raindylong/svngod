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
		g.ref = segmentRef{First: g.first, Count: g.count, Key: r.keys.Segment(g.shard, g.first, g.count), SHA256: g.sha}
		if err := r.store.Put(ctx, g.ref.Key, data); err != nil {
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
		if err := r.writeSegmentJournal(ctx, g.first, g.count, g.parent, lease.token, created, g.ref.Key, g.sha); err != nil {
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

	// Compact a shard once its last revision is published.
	for _, g := range groups {
		if (g.first+g.count)%r.shardSz == 0 {
			if cerr := r.compactSegmentShard(ctx, g.shard); cerr != nil {
				log.Printf("segment: compact shard %d: %v", g.shard, cerr)
			}
		}
	}
	return revs, errs, nil
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
		Version:     2,
		Rev:         first,
		Parent:      parent,
		Lease:       lease,
		Created:     created,
		SegmentKey:  key,
		SegmentSHA:  sha,
		First:       first,
		Count:       count,
		Quorum:      r.writeQuorum,
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
	if derr != nil || rec.Version != 2 || rec.Lease != token || rec.SegmentKey != ref.Key ||
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
	seg, err := r.loadSegment(*ref)
	if err != nil {
		return nil, nil, err
	}
	i := rev - seg.First
	if i >= uint64(len(seg.Blocks)) {
		return nil, nil, fmt.Errorf("fs: revision %d outside segment %s", rev, ref.Key)
	}
	return seg.Blocks[i], seg.Props[i], nil
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

func (r *Repo) loadSegment(ref segmentRef) (*segmentData, error) {
	key := cache.Key("segd/" + ref.Key)
	if b, ok := r.cache.Get(key); ok {
		return decodeSegment(b)
	}
	b, err := r.store.Get(context.Background(), ref.Key, nil)
	if err != nil {
		return nil, err
	}
	if ref.SHA256 != "" {
		sum := sha256.Sum256(b)
		if hex.EncodeToString(sum[:]) != ref.SHA256 {
			return nil, fmt.Errorf("fs: segment %s checksum mismatch", ref.Key)
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
	for i, ref := range refs {
		if i == 0 {
			first = ref.First
		}
		if end := ref.First + ref.Count - 1; end > last {
			last = end
		}
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
	ref := segmentRef{First: first, Count: merged.Count, Key: key, SHA256: hex.EncodeToString(sum[:])}
	if err := r.store.Put(ctx, r.keys.SegmentIndex(sh), encodeSegmentIndex([]segmentRef{ref})); err != nil {
		return err
	}
	cachePut(r.cache, cache.Key(fmt.Sprintf("segi/%d", sh)), encodeSegmentIndex([]segmentRef{ref}))
	old := make([]string, 0, len(refs))
	for _, ref := range refs {
		old = append(old, ref.Key)
	}
	go func() {
		time.Sleep(60 * time.Second)
		for _, k := range old {
			_ = r.store.Delete(context.Background(), []string{k})
		}
	}()
	return nil
}