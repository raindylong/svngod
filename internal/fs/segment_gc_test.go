package fs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"svngod/internal/cdc"
	"svngod/internal/fs/store"
)

func TestCompactSegmentsOnPartialShard(t *testing.T) {
	st := store.NewMemoryStore()
	r := openSegRepo(t, st, "segpart", 64, 64)
	for b := 0; b < 2; b++ {
		reqs := make([]BatchCommit, 5)
		for i := range reqs {
			reqs[i] = BatchCommit{Changes: []Change{{Path: fmt.Sprintf("/p%d-%d", b, i), Action: ActionAdd, Kind: KindFile, Data: []byte("v")}}}
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
	refs, err := r.loadSegmentIndex(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 2 {
		t.Fatalf("pre-compaction refs=%+v", refs)
	}
	n, err := r.CompactSegments(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("merged=%d err=%v", n, err)
	}
	refs, err = r.loadSegmentIndex(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 || refs[0].First != 1 || refs[0].Count != 10 {
		t.Fatalf("post-compaction refs=%+v", refs)
	}
	for rev := uint64(1); rev <= 10; rev++ {
		if _, err := r.readBlock(rev); err != nil {
			t.Fatalf("read %d: %v", rev, err)
		}
	}
	// Commits after the merge must append to the merged index without loss.
	if _, errs, err := r.CommitBatch(batchAdds("/after")); err != nil || errs[0] != nil {
		t.Fatalf("commit after compaction: %v %v", err, errs)
	}
	if _, err := r.Resolve(11, "/after"); err != nil {
		t.Fatalf("resolve after: %v", err)
	}
	if _, err := r.readBlock(1); err != nil {
		t.Fatalf("read 1 after append: %v", err)
	}
}

func TestCollectOrphanSegments(t *testing.T) {
	dir := t.TempDir()
	st, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	r, err := CreateRepoSegments(st, "repos/r", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	if _, errs, err := r.CommitBatch(batchAdds("/keep")); err != nil || errs[0] != nil {
		t.Fatalf("commit: %v %v", err, errs)
	}
	orphan := r.keys.Segment(0, 1, 999)
	if err := st.Put(context.Background(), orphan, []byte("junk")); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, filepath.FromSlash(orphan))
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(p, old, old); err != nil {
		t.Fatal(err)
	}
	n, err := r.CollectOrphanSegments(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("deleted=%d, want 1", n)
	}
	if ok, _ := st.Exists(context.Background(), orphan); ok {
		t.Fatal("orphan still present")
	}
	refs, err := r.loadSegmentIndex(0)
	if err != nil || len(refs) == 0 {
		t.Fatalf("referenced segment lost: refs=%+v err=%v", refs, err)
	}
	if _, err := r.readBlock(1); err != nil {
		t.Fatalf("read after sweep: %v", err)
	}
}

// TestCollectOrphanSegmentsKeepsYoung checks the age filter: an unreferenced
// object written moments ago is left alone (it may belong to a compaction that
// is between writing the merged segment and swapping the index).
func TestCollectOrphanSegmentsKeepsYoung(t *testing.T) {
	dir := t.TempDir()
	st, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	r, err := CreateRepoSegments(st, "repos/young", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	if _, errs, err := r.CommitBatch(batchAdds("/keep")); err != nil || errs[0] != nil {
		t.Fatalf("commit: %v %v", err, errs)
	}
	orphan := r.keys.Segment(0, 1, 999)
	if err := st.Put(context.Background(), orphan, []byte("fresh")); err != nil {
		t.Fatal(err)
	}
	n, err := r.CollectOrphanSegments(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("deleted %d young objects, want 0", n)
	}
	if ok, _ := st.Exists(context.Background(), orphan); !ok {
		t.Fatal("young orphan was deleted")
	}
}

// TestCollectOrphanChangeFragments reaps staged change fragments whose activity
// manifest is gone, and keeps those of a live session.
func TestCollectOrphanChangeFragments(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepoSegments(st, "repos/frags", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	put := func(id, path string) {
		key := r.keys.Prefix + "/dav-txns/changes/" + id + "/" + path
		if err := st.Put(ctx, key, []byte(`{"path":"`+path+`"}`)); err != nil {
			t.Fatal(err)
		}
	}
	// A finished session whose manifest is gone (async cleanup or a crash).
	put("dead", "aaa")
	put("dead", "bbb")
	// A live session: manifest present.
	if err := st.Put(ctx, r.keys.Prefix+"/dav-txns/activity-live.json", []byte(`{"ID":"live","Created":9999999999}`)); err != nil {
		t.Fatal(err)
	}
	put("live", "ccc")

	n, err := r.CollectTxnDataGrace(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("collected %d fragments, want 2", n)
	}
	alive := func(k string) bool { ok, _ := st.Exists(ctx, k); return ok }
	if !alive(r.keys.Prefix + "/dav-txns/changes/live/ccc") {
		t.Fatal("live session's fragment was deleted")
	}
	if alive(r.keys.Prefix + "/dav-txns/changes/dead/aaa") {
		t.Fatal("orphan fragment survived")
	}
}

// TestReplicaDigest: the digest is stable, covers revprop overrides, and changes
// when a shard index is lost. Cross-replica equality is exercised end to end by
// `svnadmin replicas`; this pins the function itself.
func TestReplicaDigest(t *testing.T) {
	r := segRepo(t, "repos/dig")
	commitBatches(t, r, 1, 4)
	d1, err := r.ReplicaDigest()
	if err != nil || d1 == "" {
		t.Fatalf("digest=%q err=%v", d1, err)
	}
	if d2, _ := r.ReplicaDigest(); d2 != d1 {
		t.Fatalf("digest not stable: %q != %q", d1, d2)
	}
	// A revprop override changes the digest (the mutable side must be covered).
	if err := r.UpdateRevProps(2, map[string]string{"svn:log": "edited"}, nil); err != nil {
		t.Fatal(err)
	}
	d2, err := r.ReplicaDigest()
	if err != nil {
		t.Fatal(err)
	}
	if d2 == d1 {
		t.Fatal("digest unchanged after a revprop edit")
	}
	// Changing the override's content (not just its presence) must show up too.
	if err := r.UpdateRevProps(2, map[string]string{"svn:log": "edited again"}, nil); err != nil {
		t.Fatal(err)
	}
	d3, err := r.ReplicaDigest()
	if err != nil {
		t.Fatal(err)
	}
	if d3 == d2 {
		t.Fatal("digest unchanged after editing the override content")
	}
	// A lost shard index must change it as well.
	if err := r.store.Delete(context.Background(), []string{r.keys.SegmentIndex(0)}); err != nil {
		t.Fatal(err)
	}
	if d4, _ := r.ReplicaDigest(); d4 == d3 {
		t.Fatal("digest unchanged after deleting an index")
	}
}

// TestCompactionHonoursSegmentTarget: merging a shard must produce segments of
// at most SEGMENT_TARGET revisions, not one segment per shard.
func TestCompactionHonoursSegmentTarget(t *testing.T) {
	st := store.NewMemoryStore()
	if _, err := CreateRepoSegments(st, "repos/tgt", 64, 64, cdc.DefaultParams(), false, 8); err != nil {
		t.Fatal(err)
	}
	r, err := OpenRepo(st, "repos/tgt")
	if err != nil {
		t.Fatal(err)
	}
	commitBatches(t, r, 32, 1) // 32 revisions, one segment each
	if _, err := r.CompactSegments(context.Background()); err != nil {
		t.Fatal(err)
	}
	refs, err := r.loadSegmentIndex(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 4 {
		t.Fatalf("refs=%d, want 4 (32 revisions at target 8): %+v", len(refs), refs)
	}
	var next uint64 = 1
	for _, ref := range refs {
		if ref.Count > 8 {
			t.Fatalf("segment %d+%d exceeds target 8", ref.First, ref.Count)
		}
		if ref.First != next {
			t.Fatalf("gap: %d+%d, want first %d", ref.First, ref.Count, next)
		}
		next += ref.Count
	}
	if next != 33 {
		t.Fatalf("coverage ends at %d, want 33", next)
	}
	for rev := uint64(1); rev <= 32; rev++ {
		if _, err := r.readBlock(rev); err != nil {
			t.Fatalf("read %d: %v", rev, err)
		}
	}
	issues, err := r.VerifyRange(1, 32, false)
	if err != nil || len(issues) != 0 {
		t.Fatalf("verify: issues=%v err=%v", issues, err)
	}
	// Compacting again must be a no-op.
	if _, err := r.CompactSegments(context.Background()); err != nil {
		t.Fatal(err)
	}
	again, _ := r.loadSegmentIndex(0)
	if len(again) != 4 {
		t.Fatalf("second compaction changed the index: %+v", again)
	}
}

// TestActiveShardCompactionBoundsIndex: a shard that never fills accumulates one
// index entry per commit. The background compactor must merge it once the index
// reaches the threshold, so a busy repository's index does not grow without
// bound (which made every commit rewrite an ever-larger object).
func TestActiveShardCompactionBoundsIndex(t *testing.T) {
	st := store.NewMemoryStore()
	if _, err := CreateRepoSegments(st, "repos/active", 4096, 64, cdc.DefaultParams(), false, 64); err != nil {
		t.Fatal(err)
	}
	r, err := OpenRepo(st, "repos/active")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 140; i++ {
		if _, errs, cerr := r.CommitBatch([]BatchCommit{{Changes: []Change{{
			Path: fmt.Sprintf("/f%03d.txt", i), Action: ActionAdd, Kind: KindFile, Data: []byte("x"),
		}}}}); cerr != nil || errs[0] != nil {
			t.Fatalf("commit %d: %v %v", i, cerr, errs)
		}
	}
	// The compactor is asynchronous: wait for it to bring the index down.
	deadline := time.Now().Add(10 * time.Second)
	var refs []segmentRef
	for {
		refs, err = r.loadSegmentIndex(0)
		if err != nil {
			t.Fatal(err)
		}
		if len(refs) < segmentCompactionThreshold || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(refs) >= segmentCompactionThreshold {
		t.Fatalf("active shard index still has %d entries (threshold %d)", len(refs), segmentCompactionThreshold)
	}
	for rev := uint64(1); rev <= 140; rev++ {
		if _, err := r.readBlock(rev); err != nil {
			t.Fatalf("read %d: %v", rev, err)
		}
	}
	issues, err := r.VerifyRange(1, 140, false)
	if err != nil || len(issues) != 0 {
		t.Fatalf("verify: issues=%v err=%v", issues, err)
	}
}
