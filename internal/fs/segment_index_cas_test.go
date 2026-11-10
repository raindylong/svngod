package fs

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"svngod/internal/cdc"
	"svngod/internal/fs/store"
)

// TestSegmentIndexCASMergesConcurrentWriters pins the lost-update protection:
// two writers that read the same index must not be able to have the second one
// drop the first one's entry.
func TestSegmentIndexCASMergesConcurrentWriters(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepoSegments(st, "repos/cas", 64, 64, cdc.DefaultParams(), false, 64)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	refA := segmentRef{First: 1, Count: 2, SHA256: "aa"}
	refB := segmentRef{First: 3, Count: 2, SHA256: "bb"}

	// Both writers observe an absent index.
	_, etag, existed, err := r.loadSegmentIndexETag(ctx, 0)
	if err != nil || existed || etag != "" {
		t.Fatalf("initial index: etag=%q existed=%v err=%v", etag, existed, err)
	}
	if err := r.writeSegmentIndexCAS(ctx, 0, []segmentRef{refA}, etag, existed); err != nil {
		t.Fatal(err)
	}
	// The second writer's stale CAS must fail rather than overwrite.
	if err := r.writeSegmentIndexCAS(ctx, 0, []segmentRef{refB}, etag, existed); !errors.Is(err, ErrSegmentIndexConflict) {
		t.Fatalf("stale CAS err=%v, want conflict", err)
	}
	// Retrying through the helper re-reads and merges.
	if _, err := r.appendSegmentIndex(ctx, refB); err != nil {
		t.Fatal(err)
	}
	refs, err := r.loadSegmentIndex(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 2 || refs[0] != refA || refs[1] != refB {
		t.Fatalf("merged index=%+v", refs)
	}
}

// TestAppendRefusesPublishedRange: a ref covering revisions that are already
// published must never replace the entry that serves them.
func TestAppendRefusesPublishedRange(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepoSegments(st, "repos/foreign", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	commitBatches(t, r, 1, 4) // head=4, index 1+4
	ctx := context.Background()
	_, err = r.appendSegmentIndex(ctx, segmentRef{First: 1, Count: 4, SHA256: "deadbeef"})
	if !errors.Is(err, ErrSegmentIndexConflict) {
		t.Fatalf("err=%v, want conflict", err)
	}
	refs, lerr := r.loadSegmentIndex(0)
	if lerr != nil {
		t.Fatal(lerr)
	}
	if len(refs) != 1 || refs[0].SHA256 == "deadbeef" {
		t.Fatalf("published index was modified: %+v", refs)
	}
	if h, _ := r.HeadFresh(); h != 4 {
		t.Fatalf("head=%d, want 4", h)
	}
}

// TestAppendReclaimsUnpublishedEntry: an entry above HEAD is an aborted
// publish; the shard is rebuilt from objects (dropping it) and the append
// succeeds, so the revision range is not blocked forever.
func TestAppendReclaimsUnpublishedEntry(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepoSegments(st, "repos/reclaim", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	commitBatches(t, r, 1, 4) // head=4, index 1+4
	ctx := context.Background()
	// Simulate a crashed writer: index entry at 5+2 with no publication.
	refs, err := r.loadSegmentIndex(0)
	if err != nil {
		t.Fatal(err)
	}
	orphan := segmentRef{First: 5, Count: 2, SHA256: "cafe"}
	if err := st.Put(ctx, r.keys.SegmentIndex(0), encodeSegmentIndex(append(refs, orphan))); err != nil {
		t.Fatal(err)
	}
	if _, err := r.appendSegmentIndex(ctx, segmentRef{First: 5, Count: 2, SHA256: "beef"}); err != nil {
		t.Fatalf("append over unpublished entry: %v", err)
	}
	got, err := r.loadSegmentIndex(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].First != 1 || got[1].First != 5 || got[1].SHA256 != "beef" {
		t.Fatalf("index=%+v", got)
	}
}

// failingCASStore fails CAS on the HEAD pointer so a publish aborts after its
// index and WAL writes, exercising rollback.
type failingCASStore struct {
	store.Store
	failHead bool
}

func (s *failingCASStore) CAS(ctx context.Context, key, expect string, data []byte) (string, error) {
	if s.failHead && key == "repos/roll/current" {
		return "", errors.New("injected head CAS failure")
	}
	return s.Store.CAS(ctx, key, expect, data)
}

func TestPublishRollsBackIndexOnFailure(t *testing.T) {
	base := store.NewMemoryStore()
	st := &failingCASStore{Store: base, failHead: true}
	r, err := CreateRepoSegments(st, "repos/roll", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	_, errs, err := r.CommitBatch(batchAdds("/a", "/b"))
	if err == nil && (len(errs) == 0 || errs[0] == nil) {
		t.Fatal("commit succeeded despite injected CAS failure")
	}
	refs, lerr := r.loadSegmentIndex(0)
	if lerr != nil {
		t.Fatal(lerr)
	}
	if len(refs) != 0 {
		t.Fatalf("unpublished index entry left behind: %+v", refs)
	}
	if h, _ := r.HeadFresh(); h != 0 {
		t.Fatalf("head=%d, want 0", h)
	}
	// With the fault cleared the same commit must go through.
	st.failHead = false
	revs, errs, err := r.CommitBatch(batchAdds("/a", "/b"))
	if err != nil || errs[0] != nil {
		t.Fatalf("retry: revs=%v errs=%v err=%v", revs, errs, err)
	}
	if revs[0] != 1 {
		t.Fatalf("retry landed at rev %d, want 1", revs[0])
	}
}

// TestRepairDropsUnpublishedSegments makes sure a crashed writer's segment
// (written, never published) does not block the revision range forever.
func TestRepairDropsUnpublishedSegments(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepoSegments(st, "repos/orphan", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	commitBatches(t, r, 1, 4) // head=4
	head, _ := r.HeadFresh()
	seg := &segmentData{First: head + 1, Count: 1, Parent: head,
		Blocks: [][]byte{[]byte("x")}, Props: [][]byte{[]byte("y")}}
	if err := st.Put(context.Background(), r.keys.Segment(0, head+1, 1), encodeSegment(seg)); err != nil {
		t.Fatal(err)
	}
	refs, err := r.rebuildSegmentIndex(context.Background(), 0, head)
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range refs {
		if ref.First > head {
			t.Fatalf("unpublished segment %d+%d kept in rebuilt index", ref.First, ref.Count)
		}
	}
	// The published revisions must still be covered.
	var covered uint64
	for _, ref := range refs {
		if end := ref.First + ref.Count - 1; end > covered {
			covered = end
		}
	}
	if covered < head {
		t.Fatalf("rebuilt index covers only to %d of %d", covered, head)
	}
}

func TestSegmentIndexConflictErrorText(t *testing.T) {
	err := fmt.Errorf("%w: revisions 1+2", ErrSegmentIndexConflict)
	if !errors.Is(err, ErrSegmentIndexConflict) {
		t.Fatal("wrapped conflict not detectable")
	}
}
