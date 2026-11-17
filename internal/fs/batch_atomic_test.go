package fs

import (
	"testing"

	"svngod/internal/fs/store"
)

// TestCommitBatchAllOrNothingLeavesNoGap is the guarantee a bulk importer
// needs: when any request in the batch is rejected, nothing is published and
// the revision sequence stays contiguous.
//
// Without it, a rejection in the middle publishes the requests around it
// (revs=[2 0 3]) and the missing number corrupts every later revision's parent
// chain - the repository can no longer be resumed, only rebuilt.
func TestCommitBatchAllOrNothingLeavesNoGap(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepo(st, "aon", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.CommitTxn(0, []Change{{Path: "/a", Action: ActionAdd, Kind: KindFile, Data: []byte("x")}}, nil); err != nil {
		t.Fatal(err)
	}
	before, err := r.Head()
	if err != nil {
		t.Fatal(err)
	}

	// Request 1 would succeed on its own; request 2 re-adds the path request 1
	// creates, so the tree check rejects it.
	revs, errs, err := r.CommitBatch([]BatchCommit{
		{Changes: []Change{{Path: "/b", Action: ActionAdd, Kind: KindFile, Data: []byte("y")}}},
		{Changes: []Change{{Path: "/b", Action: ActionAdd, Kind: KindFile, Data: []byte("z")}}},
		{Changes: []Change{{Path: "/c", Action: ActionAdd, Kind: KindFile, Data: []byte("w")}}},
	}, AllOrNothing())

	if err == nil {
		t.Fatal("AllOrNothing batch with a rejected request reported success")
	}
	for i, rv := range revs {
		if rv != 0 {
			t.Fatalf("request %d reported revision %d in an abandoned batch", i, rv)
		}
	}
	for i, e := range errs {
		if e == nil {
			t.Fatalf("request %d reported no error in an abandoned batch", i)
		}
	}
	after, err := r.Head()
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("head moved from %d to %d: part of the batch was published", before, after)
	}
	// Nothing from the batch is visible.
	if _, rerr := r.Resolve(after, "/b"); rerr == nil {
		t.Fatal("/b from an abandoned batch is readable")
	}
	if _, rerr := r.Resolve(after, "/c"); rerr == nil {
		t.Fatal("/c from an abandoned batch is readable")
	}
}

// TestCommitBatchAllOrNothingSucceedsWhenClean makes sure the option does not
// turn a healthy batch into a no-op.
func TestCommitBatchAllOrNothingSucceedsWhenClean(t *testing.T) {
	r, err := CreateRepo(store.NewMemoryStore(), "aon2", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	revs, errs, err := r.CommitBatch([]BatchCommit{
		{Changes: []Change{{Path: "/a", Action: ActionAdd, Kind: KindFile, Data: []byte("1")}}},
		{Changes: []Change{{Path: "/b", Action: ActionAdd, Kind: KindFile, Data: []byte("2")}}},
		{Changes: []Change{{Path: "/c", Action: ActionAdd, Kind: KindFile, Data: []byte("3")}}},
	}, AllOrNothing())
	if err != nil {
		t.Fatalf("clean batch failed: %v", err)
	}
	for i, rv := range revs {
		if rv != uint64(i+1) {
			t.Fatalf("request %d got revision %d, want %d", i, rv, i+1)
		}
		if errs[i] != nil {
			t.Fatalf("request %d reported %v", i, errs[i])
		}
	}
	head, _ := r.Head()
	if head != 3 {
		t.Fatalf("head=%d, want 3", head)
	}
	// Contiguity is the property the loader relies on.
	for rev := uint64(1); rev <= head; rev++ {
		if _, err := r.readBlock(rev); err != nil {
			t.Fatalf("revision %d unreadable: %v", rev, err)
		}
	}
}

// TestCommitBatchDefaultStillPartial pins that the server's semantics did not
// change: without the option, disjoint-path commits still merge and a middle
// rejection still leaves the surrounding ones published.
func TestCommitBatchDefaultStillPartial(t *testing.T) {
	r, err := CreateRepo(store.NewMemoryStore(), "aon3", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.CommitTxn(0, []Change{{Path: "/a", Action: ActionAdd, Kind: KindFile, Data: []byte("x")}}, nil); err != nil {
		t.Fatal(err)
	}
	revs, _, _ := r.CommitBatch([]BatchCommit{
		{Changes: []Change{{Path: "/b", Action: ActionAdd, Kind: KindFile, Data: []byte("y")}}},
		{Changes: []Change{{Path: "/b", Action: ActionAdd, Kind: KindFile, Data: []byte("z")}}},
		{Changes: []Change{{Path: "/c", Action: ActionAdd, Kind: KindFile, Data: []byte("w")}}},
	})
	if revs[0] == 0 || revs[2] == 0 {
		t.Fatalf("default batch stopped publishing around a rejection: %v", revs)
	}
	if revs[1] != 0 {
		t.Fatalf("rejected request got a revision: %v", revs)
	}
	head, _ := r.Head()
	if head != 3 {
		t.Fatalf("head=%d, want 3 (default semantics must not change)", head)
	}
}
