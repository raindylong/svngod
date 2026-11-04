package fs

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"svngod/internal/fs/store"
)

func addFile(path, data string) BatchCommit {
	return BatchCommit{Changes: []Change{{Path: path, Action: ActionAdd, Kind: KindFile, Data: []byte(data)}}}
}

func TestCommitBatchPublishesConsecutiveRevisions(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepo(st, "batch", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	var reqs []BatchCommit
	for i := 0; i < 5; i++ {
		reqs = append(reqs, addFile(fmt.Sprintf("/f%d", i), fmt.Sprintf("v%d", i)))
	}
	revs, errs, err := r.CommitBatch(reqs)
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	for i, e := range errs {
		if e != nil {
			t.Fatalf("request %d: %v", i, e)
		}
	}
	for i, rev := range revs {
		if rev != uint64(i+1) {
			t.Fatalf("rev[%d]=%d, want %d", i, rev, i+1)
		}
	}
	if h, _ := r.HeadFresh(); h != 5 {
		t.Fatalf("head=%d, want 5", h)
	}
	nr, err := r.Resolve(5, "/f3")
	if err != nil {
		t.Fatal(err)
	}
	txt, err := r.GetText(nr.Node)
	if err != nil || string(txt) != "v3" {
		t.Fatalf("content=%q err=%v", txt, err)
	}
}

func TestCommitBatchConflictIsolated(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepo(st, "batchconflict", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	reqs := []BatchCommit{addFile("/a", "1"), addFile("/a", "2"), addFile("/b", "3")}
	revs, errs, err := r.CommitBatch(reqs)
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if errs[0] != nil || errs[2] != nil {
		t.Fatalf("unexpected errors: %v %v", errs[0], errs[2])
	}
	if !errors.Is(errs[1], ErrOutOfDate) {
		t.Fatalf("duplicate add err=%v, want out-of-date", errs[1])
	}
	if revs[0] != 1 || revs[2] != 2 {
		t.Fatalf("revs=%v, want 1 and 2", revs)
	}
	if h, _ := r.HeadFresh(); h != 2 {
		t.Fatalf("head=%d, want 2", h)
	}
	// The failed request does not consume a revision number.
	rev, e, err := r.CommitBatch([]BatchCommit{addFile("/c", "4")})
	if err != nil || e[0] != nil || rev[0] != 3 {
		t.Fatalf("next batch rev=%v err=%v/%v, want 3", rev, e[0], err)
	}
}

func TestCommitBatchModifyWithinBatch(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepo(st, "batchmod", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	reqs := []BatchCommit{
		addFile("/f", "one"),
		{Changes: []Change{{Path: "/f", Action: ActionModify, Kind: KindFile, Data: []byte("two")}}},
	}
	revs, errs, err := r.CommitBatch(reqs)
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if errs[0] != nil || errs[1] != nil || revs[1] != 2 {
		t.Fatalf("revs=%v errs=%v", revs, errs)
	}
	for rev, want := range map[uint64]string{1: "one", 2: "two"} {
		nr, rerr := r.Resolve(rev, "/f")
		if rerr != nil {
			t.Fatal(rerr)
		}
		txt, terr := r.GetText(nr.Node)
		if terr != nil || string(txt) != want {
			t.Fatalf("rev %d content=%q err=%v, want %q", rev, txt, terr, want)
		}
	}
}

func TestCommitBatchReplayRollsForwardWindow(t *testing.T) {
	base := store.NewMemoryStore()
	ws := &walProbeStore{countingStore: &countingStore{Store: base}}
	r, err := CreateRepo(ws, "batchreplay", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	var reqs []BatchCommit
	for i := 0; i < 12; i++ {
		reqs = append(reqs, addFile(fmt.Sprintf("/r%d", i), fmt.Sprintf("v%d", i)))
	}
	if _, errs, err := r.CommitBatch(reqs); err != nil {
		t.Fatalf("batch: %v", err)
	} else {
		for i, e := range errs {
			if e != nil {
				t.Fatalf("request %d: %v", i, e)
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
	r2, err := OpenRepo(ws, "batchreplay")
	if err != nil {
		t.Fatal(err)
	}
	if h, err := r2.HeadFresh(); err != nil || h != 12 {
		t.Fatalf("head after replay=%d err=%v, want 12", h, err)
	}
}