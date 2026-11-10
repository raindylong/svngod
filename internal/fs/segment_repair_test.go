package fs

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"svngod/internal/cdc"
	"svngod/internal/fs/store"
)

func commitBatches(t *testing.T, r *Repo, batches, per int) {
	t.Helper()
	for b := 0; b < batches; b++ {
		reqs := make([]BatchCommit, per)
		for i := range reqs {
			reqs[i] = BatchCommit{Changes: []Change{{
				Path:   fmt.Sprintf("/p%d-%d", b, i),
				Action: ActionAdd,
				Kind:   KindFile,
				Data:   []byte(fmt.Sprintf("v%d-%d", b, i)),
			}}}
		}
		if _, errs, err := r.CommitBatch(reqs); err != nil {
			t.Fatal(err)
		} else {
			for _, e := range errs {
				if e != nil {
					t.Fatal(e)
				}
			}
		}
	}
}

// TestReadRepairsLostSegmentIndex deletes a shard index and expects the next
// read of an indexed revision to rebuild it from the segment objects.
func TestReadRepairsLostSegmentIndex(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepoSegments(st, "repos/repi", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	commitBatches(t, r, 2, 4) // shard 0: refs 1+4, 5+4
	if refs, err := r.loadSegmentIndex(0); err != nil || len(refs) != 2 {
		t.Fatalf("pre index=%+v err=%v", refs, err)
	}
	if err := st.Delete(context.Background(), []string{r.keys.SegmentIndex(0)}); err != nil {
		t.Fatal(err)
	}
	// Reopen so no cached index or block hides the missing object.
	r2, err := OpenRepo(st, "repos/repi")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r2.readBlock(3); err != nil {
		t.Fatalf("read after index loss: %v", err)
	}
	refs, err := r2.loadSegmentIndex(0)
	if err != nil {
		t.Fatal(err)
	}
	// The rebuild keeps the widest segment covering each range, so a merged
	// shard may come back as one entry even though two were committed.
	if len(refs) == 0 || refs[0].First != 1 || refs[len(refs)-1].First+refs[len(refs)-1].Count <= 7 {
		t.Fatalf("index not rebuilt: %+v", refs)
	}
	for rev := uint64(1); rev <= 8; rev++ {
		if _, err := r2.readBlock(rev); err != nil {
			t.Fatalf("read %d after repair: %v", rev, err)
		}
	}
	issues, err := r2.VerifyRange(1, 8, false)
	if err != nil || len(issues) != 0 {
		t.Fatalf("verify after repair: issues=%v err=%v", issues, err)
	}
}

// TestRepairSegmentIndexesRebuildsAll rebuilds every shard in one pass.
func TestRepairSegmentIndexesRebuildsAll(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepoSegments(st, "repos/repall", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	commitBatches(t, r, 4, 4) // 16 revisions, shards 0 and 1
	var keys []string
	for sh := uint64(0); sh <= 1; sh++ {
		keys = append(keys, r.keys.SegmentIndex(sh))
	}
	if err := st.Delete(context.Background(), keys); err != nil {
		t.Fatal(err)
	}
	r2, err := OpenRepo(st, "repos/repall")
	if err != nil {
		t.Fatal(err)
	}
	n, err := r2.RepairSegmentIndexes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("rebuilt=%d, want 2", n)
	}
	issues, err := r2.VerifyRange(1, 16, false)
	if err != nil || len(issues) != 0 {
		t.Fatalf("verify after full repair: issues=%v err=%v", issues, err)
	}
	n, err = r2.RepairSegmentIndexes(context.Background())
	if err != nil || n != 0 {
		t.Fatalf("second repair rebuilt=%d err=%v, want 0", n, err)
	}
}

// TestRebuildRejectsOverlap refuses to guess when two segment objects claim
// overlapping revisions.
func TestRebuildRejectsOverlap(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepoSegments(st, "repos/repov", 16, 64, cdc.DefaultParams(), false, 16)
	if err != nil {
		t.Fatal(err)
	}
	commitBatches(t, r, 1, 4)
	refs, err := r.loadSegmentIndex(0)
	if err != nil || len(refs) != 1 {
		t.Fatalf("index=%+v err=%v", refs, err)
	}
	seg, err := r.loadSegment(refs[0])
	if err != nil {
		t.Fatal(err)
	}
	// A second object claiming revisions 3..6 overlaps 1..4.
	fake := &segmentData{First: 3, Count: 4, Parent: 2,
		Blocks: seg.Blocks[:4], Props: seg.Props[:4]}
	if err := st.Put(context.Background(), r.keys.Segment(0, 3, 4), encodeSegment(fake)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.rebuildSegmentIndex(context.Background(), 0, 16); err == nil {
		t.Fatal("rebuild accepted overlapping segment ranges")
	}
}

// TestRepairRefusesShortIndex deletes a segment object and expects the rebuild
// to refuse writing a shorter index: the loss must stay visible.
func TestRepairRefusesShortIndex(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepoSegments(st, "repos/short", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	commitBatches(t, r, 2, 4) // revs 1..8: shard 0 = 1..7, shard 1 = 8
	refs, err := r.loadSegmentIndex(0)
	if err != nil || len(refs) == 0 {
		t.Fatalf("index=%+v err=%v", refs, err)
	}
	if err := st.Delete(context.Background(), []string{r.segmentKeyFor(refs[0].First, refs[0].Count)}); err != nil {
		t.Fatal(err)
	}
	r2, err := OpenRepo(st, "repos/short")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r2.RepairSegmentIndexes(context.Background()); err == nil {
		t.Fatal("repair accepted an index that does not cover HEAD")
	} else if !strings.Contains(err.Error(), "cover") {
		t.Fatalf("error does not explain the gap: %v", err)
	}
	// The original index is preserved (it names the missing object).
	got, err := r2.loadSegmentIndex(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(refs) || got[0] != refs[0] {
		t.Fatalf("index was narrowed to %+v", got)
	}
}

// TestRepairDropsDeadOverride recovers the "listed but missing" state: reads
// refuse to silently revert the edit, so repair removes the dead list entry and
// the committed value is served again.
func TestRepairDropsDeadOverride(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepoSegments(st, "repos/deadov", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	commitBatches(t, r, 1, 2)
	committed, err := r.GetRevPropsFresh(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.UpdateRevProps(1, map[string]string{"svn:log": "edited"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := st.Delete(context.Background(), []string{r.keys.SegRevPropsOverride(0, 1)}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.GetRevPropsFresh(1); err == nil {
		t.Fatal("missing override did not error before repair")
	}
	n, err := r.RepairRevPropsOverrides(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("dropped=%d, want 1", n)
	}
	rp, err := r.GetRevPropsFresh(1)
	if err != nil {
		t.Fatal(err)
	}
	if rp.Props["svn:log"] != committed.Props["svn:log"] {
		t.Fatalf("after repair log=%q, want committed %q", rp.Props["svn:log"], committed.Props["svn:log"])
	}
	if n, _ := r.RepairRevPropsOverrides(context.Background()); n != 0 {
		t.Fatalf("second repair dropped %d", n)
	}
	issues, err := r.VerifyRange(1, 2, false)
	if err != nil || len(issues) != 0 {
		t.Fatalf("verify: issues=%v err=%v", issues, err)
	}
}
