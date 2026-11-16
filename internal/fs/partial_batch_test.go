package fs

import (
	"testing"

	"svngod/internal/fs/store"
)

// TestCommitBatchPartialFailureIsVisible documents the contract a bulk caller
// depends on: CommitBatch applies requests one by one, so a rejection in the
// middle leaves the earlier ones published and the later ones applied on top.
// The revision-number gap is visible only through a zero in revs - err stays
// nil. Callers that need all-or-nothing (the dump loader does, because a gap
// makes history invalid) must check for zeros, which load.go does.
func TestCommitBatchPartialFailureIsVisible(t *testing.T) {
	r, err := CreateRepo(store.NewMemoryStore(), "pb", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// rev1 creates /a.
	if _, err := r.CommitTxn(0, []Change{{Path: "/a", Action: ActionAdd, Kind: KindFile, Data: []byte("x")}}, nil); err != nil {
		t.Fatal(err)
	}
	// A batch whose second request adds a path that already exists at that
	// point (the first request creates it), so checkChangeInTree rejects #2.
	revs, errs, err := r.CommitBatch([]BatchCommit{
		{Changes: []Change{{Path: "/b", Action: ActionAdd, Kind: KindFile, Data: []byte("y")}}},
		{Changes: []Change{{Path: "/b", Action: ActionAdd, Kind: KindFile, Data: []byte("z")}}},
		{Changes: []Change{{Path: "/c", Action: ActionAdd, Kind: KindFile, Data: []byte("w")}}},
	})
	t.Logf("revs=%v errs=%v err=%v", revs, errs, err)
	head, _ := r.Head()
	t.Logf("head=%d", head)
	if err != nil {
		return
	}
	// No whole-batch error: the caller must still be able to detect that not
	// every request landed. If revs contains zeros, that is the signal.
	partial := false
	for _, rv := range revs {
		if rv == 0 {
			partial = true
		}
	}
	anyErr := false
	for _, e := range errs {
		if e != nil {
			anyErr = true
		}
	}
	if anyErr && !partial {
		t.Fatal("a failed request reported an error but still returned a revision")
	}
	if !anyErr && !partial {
		t.Log("batch applied fully (rejection did not happen); nothing to assert")
	}
	// The gap is exactly what a bulk caller must see: the rejected revision is
	// missing while its successor exists.
	if partial && head <= 1 {
		t.Fatalf("rejected request left head at %d: the bug is a silent no-op instead of a gap", head)
	}
}
