package fs

import (
	"fmt"
	"sync"
	"testing"

	"svngod/internal/fs/store"
)

// TestConcurrentSamePathCommit verifies that, under the repo.mu serial lock and
// path-level optimistic concurrency control, N concurrent commits to the SAME
// path yield exactly one winner, the losers get ErrOutOfDate (not a silent
// lost update), and the committed content is the winner's — no block pollution.
func TestConcurrentSamePathCommit(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepo(st, "repos/test", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	base, err := r.CommitTxn(0, []Change{
		{Path: "/f.txt", Action: ActionAdd, Kind: KindFile, Data: []byte("base content for f.txt seed revision")},
	}, nil)
	if err != nil || base != 1 {
		t.Fatalf("seed commit: rev=%d err=%v", base, err)
	}

	const n = 3
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = r.CommitTxn(base, []Change{
				{Path: "/f.txt", Action: ActionModify, Kind: KindFile, Data: []byte(fmt.Sprintf("v%d", i))},
			}, nil)
		}(i)
	}
	wg.Wait()

	succ, ood := 0, 0
	var winnerErr error
	for i, e := range errs {
		switch {
		case e == nil:
			succ++
		case e == ErrOutOfDate:
			ood++
		default:
			winnerErr = fmt.Errorf("unexpected error from goroutine %d: %v", i, e)
		}
	}
	if winnerErr != nil {
		t.Fatal(winnerErr)
	}
	if succ != 1 {
		t.Fatalf("expected exactly 1 winner, got %d (out-of-date=%d)", succ, ood)
	}
	if ood != n-1 {
		t.Fatalf("expected %d out-of-date losers, got %d", n-1, ood)
	}

	head, err := r.Head()
	if err != nil {
		t.Fatal(err)
	}
	if head != base+1 {
		t.Fatalf("head=%d, want %d (only one new revision, no block pollution)", head, base+1)
	}

	// Read back the winner's committed content — must not be polluted by a loser.
	nr, err := r.Resolve(head, "/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	if nr.Kind != KindFile {
		t.Fatalf("f.txt kind=%v", nr.Kind)
	}
	got, err := r.GetText(nr.Node)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "v0" && string(got) != "v1" && string(got) != "v2" {
		t.Fatalf("committed f.txt content is polluted/foreign: %q", got)
	}
}
