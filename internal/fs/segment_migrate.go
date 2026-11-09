package fs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// MigrateToSegments rewrites a v1 repository's revisions into segment objects
// (format v2) and updates the repository format. It is designed for an
// offline/maintenance window: it aborts if HEAD moves during the pass, and the
// format is switched only after every shard has been written, so a crash leaves
// the repository readable in v1 (orphaned segments are garbage).
//
// purge additionally deletes the v1 loose/pack objects after the format switch.
func (r *Repo) MigrateToSegments(ctx context.Context, purge bool) (int, error) {
	if r.segments {
		return 0, nil
	}
	head, err := r.HeadFresh()
	if err != nil {
		return 0, err
	}
	if head == 0 {
		if err := r.switchFormat(); err != nil {
			return 0, err
		}
		return 0, nil
	}
	shardSz := r.shardSz
	lastShard := head / shardSz
	migrated := 0
	for sh := uint64(0); sh <= lastShard; sh++ {
		first := sh * shardSz
		last := first + shardSz - 1
		if sh == 0 {
			first = 1 // revision 0 stays loose
		}
		if last > head {
			last = head
		}
		if first > last {
			continue
		}
		// Resume: skip a shard whose index already covers the range.
		if refs, ierr := r.loadSegmentIndex(sh); ierr == nil && len(refs) == 1 &&
			refs[0].First == first && refs[0].Count == last-first+1 {
			migrated += int(last - first + 1)
			continue
		}
		seg := &segmentData{First: first, Count: last - first + 1, Parent: first - 1}
		for rev := first; rev <= last; rev++ {
			rb, rerr := r.readBlock(rev)
			if rerr != nil {
				return migrated, fmt.Errorf("read block %d: %w", rev, rerr)
			}
			rp, perr := r.loadRevProps(rev)
			if perr != nil {
				return migrated, fmt.Errorf("read props %d: %w", rev, perr)
			}
			seg.Blocks = append(seg.Blocks, rb.Marshal())
			seg.Props = append(seg.Props, rp.Marshal())
		}
		data := encodeSegment(seg)
		sum := sha256.Sum256(data)
		key := r.keys.Segment(sh, first, seg.Count)
		if perr := r.store.Put(ctx, key, data); perr != nil {
			return migrated, perr
		}
		ref := segmentRef{First: first, Count: seg.Count, SHA256: hex.EncodeToString(sum[:])}
		if perr := r.store.Put(ctx, r.keys.SegmentIndex(sh), encodeSegmentIndex([]segmentRef{ref})); perr != nil {
			return migrated, perr
		}
		migrated += len(seg.Blocks)
	}
	// Abort if a writer slipped in: the segments would be stale.
	if now, err := r.HeadFresh(); err != nil || now != head {
		return migrated, fmt.Errorf("head moved during migration (%d -> %d); retry with writers stopped", head, now)
	}
	if err := r.switchFormat(); err != nil {
		return migrated, err
	}
	if purge {
		r.purgeV1Objects(ctx, head)
	}
	return migrated, nil
}

// switchFormat flips the repository format to segment mode.
func (r *Repo) switchFormat() error {
	ctx := context.Background()
	b, err := r.store.Get(ctx, r.keys.Format(), nil)
	if err != nil {
		return err
	}
	f, err := UnmarshalFormat(b)
	if err != nil {
		return err
	}
	f.Segments = true
	f.SegmentFrom = 1
	if f.SegmentTarget == 0 {
		f.SegmentTarget = r.shardSz
	}
	if err := r.store.Put(ctx, r.keys.Format(), f.Marshal()); err != nil {
		return err
	}
	r.segments = true
	r.segmentFrom = f.SegmentFrom
	r.segmentTarget = f.SegmentTarget
	r.initCompaction()
	return nil
}

// purgeV1Objects deletes the per-revision objects now superseded by segments.
// Best effort: leftovers are harmless and can be removed by delete-repo/gc.
func (r *Repo) purgeV1Objects(ctx context.Context, head uint64) {
	shardSz := r.shardSz
	var batch []string
	flush := func() {
		if len(batch) == 0 {
			return
		}
		_ = r.deleteLoose(ctx, batch)
		batch = nil
	}
	for sh := uint64(0); sh <= head/shardSz; sh++ {
		first := sh * shardSz
		if sh == 0 {
			first = 1
		}
		last := first + shardSz - 1
		if last > head {
			last = head
		}
		for rev := first; rev <= last; rev++ {
			batch = append(batch, r.keys.LooseRev(sh, rev), r.keys.LooseRevProps(sh, rev))
			if len(batch) >= 1000 {
				flush()
			}
		}
		batch = append(batch, r.keys.PackedRev(sh), r.keys.PackedIdx(sh), r.keys.PackedDone(sh), r.keys.PackedRevProps(sh))
		flush()
	}
	flush()
}
