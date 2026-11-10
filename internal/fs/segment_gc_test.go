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
