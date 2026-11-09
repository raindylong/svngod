package fs

import (
	"context"
	"fmt"
	"testing"

	"svngod/internal/cdc"
	"svngod/internal/fs/store"
)

func openSegRepo(t *testing.T, st store.Store, name string, shard, target uint64) *Repo {
	t.Helper()
	if _, err := CreateRepoSegments(st, name, shard, 64, cdc.DefaultParams(), false, target); err != nil {
		t.Fatal(err)
	}
	r, err := OpenRepo(st, name)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func batchAdds(paths ...string) []BatchCommit {
	out := make([]BatchCommit, len(paths))
	for i, p := range paths {
		out[i] = BatchCommit{Changes: []Change{{Path: p, Action: ActionAdd, Kind: KindFile, Data: []byte("v")}}}
	}
	return out
}

func TestSegmentRepoCommitRead(t *testing.T) {
	st := store.NewMemoryStore()
	r := openSegRepo(t, st, "seg", 8, 8)
	reqs := batchAdds("/f1", "/f2", "/f3", "/f4", "/f5", "/f6", "/f7", "/f8", "/f9", "/f10")
	revs, errs, err := r.CommitBatch(reqs)
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range errs {
		if e != nil {
			t.Fatalf("request %d: %v", i, e)
		}
	}
	for i, rev := range revs {
		if rev != uint64(i+1) {
			t.Fatalf("rev[%d]=%d", i, rev)
		}
	}
	for i := 1; i <= 10; i++ {
		if _, err := r.readBlock(uint64(i)); err != nil {
			t.Fatalf("read block %d: %v", i, err)
		}
		if _, err := r.loadRevProps(uint64(i)); err != nil {
			t.Fatalf("read props %d: %v", i, err)
		}
	}
	if h, _ := r.HeadFresh(); h != 10 {
		t.Fatalf("head=%d", h)
	}
	// Reopen from the same store: the read path must resolve segments.
	r2, err := OpenRepo(st, "seg")
	if err != nil {
		t.Fatal(err)
	}
	nr, err := r2.Resolve(5, "/f5")
	if err != nil {
		t.Fatal(err)
	}
	if txt, err := r2.GetText(nr.Node); err != nil || string(txt) != "v" {
		t.Fatalf("content=%q err=%v", txt, err)
	}
	if issues, err := r2.VerifyRange(1, 10, false); err != nil || len(issues) != 0 {
		t.Fatalf("verify issues=%v err=%v", issues, err)
	}
}

func TestSegmentCompaction(t *testing.T) {
	st := store.NewMemoryStore()
	r := openSegRepo(t, st, "segc", 8, 8)
	// Shard 0 covers revisions 0..7; revision 8 starts shard 1.
	for i := 0; i < 3; i++ {
		if _, errs, err := r.CommitBatch(batchAdds(fmt.Sprintf("/a%d", i*2), fmt.Sprintf("/b%d", i*2))); err != nil {
			t.Fatal(err)
		} else {
			for _, e := range errs {
				if e != nil {
					t.Fatal(e)
				}
			}
		}
	}
	rev4, errs, err := r.CommitBatch(batchAdds("/c6"))
	if err != nil || errs[0] != nil {
		t.Fatalf("rev4=%v errs=%v err=%v", rev4, errs, err)
	}
	refs, err := r.loadSegmentIndex(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 || refs[0].First != 1 || refs[0].Count != 7 {
		t.Fatalf("compaction refs=%+v", refs)
	}
	for i := 1; i <= 7; i++ {
		if _, err := r.readBlock(uint64(i)); err != nil {
			t.Fatalf("read %d after compaction: %v", i, err)
		}
	}
}

func TestSegmentReplayRollsForward(t *testing.T) {
	base := store.NewMemoryStore()
	ws := &walProbeStore{countingStore: &countingStore{Store: base}}
	r := openSegRepo(t, ws, "segr", 8, 8)
	if _, errs, err := r.CommitBatch(batchAdds("/a", "/b", "/c", "/d")); err != nil {
		t.Fatal(err)
	} else {
		for _, e := range errs {
			if e != nil {
				t.Fatal(e)
			}
		}
	}
	cur, etag, err := r.readCurrent()
	if err != nil {
		t.Fatal(err)
	}
	rewound := (&Current{Head: 0, Created: cur.Created}).Marshal()
	if _, err := ws.CAS(context.Background(), r.keys.Current(), etag, rewound); err != nil {
		t.Fatal(err)
	}
	r2, err := OpenRepo(ws, "segr")
	if err != nil {
		t.Fatal(err)
	}
	if h, err := r2.HeadFresh(); err != nil || h != 4 {
		t.Fatalf("head after replay=%d err=%v, want 4", h, err)
	}
	if _, err := r2.readBlock(3); err != nil {
		t.Fatalf("read replayed rev: %v", err)
	}
}
