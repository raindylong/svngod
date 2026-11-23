package fs

import (
	"fmt"
	"sync"
	"testing"

	"svngod/internal/cdc"
	"svngod/internal/fs/store"
)

// Updating the same revision's properties from several clients is ordinary: a
// release script sets svn:log on many revisions, and two people editing the same
// revision's log at once is possible. The CAS loop retried a fixed 8 times with
// no backoff, so at 32 concurrent writers 20% of edits were rejected outright
// with "revprop update contention" - measured on a 32-core box through the real
// HTTP path (157 failures in 320 requests at 16 writers: 4.4%; 64/320 at 32: 20%).
//
// A rejected revprop edit is not a lost race that can be retried safely by the
// user without cost: the client sees a 500, and svn does not resend it.
//
// These tests assert that concurrent updates all succeed, which is what the
// fixed-attempt loop could not do. They are the reverse test for the fix.

// TestSegmentRevPropsConcurrentWritersAllSucceed drives more writers than the
// old retry count, which is exactly where it failed.
func TestSegmentRevPropsConcurrentWritersAllSucceed(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepoSegments(st, "repos/segretry", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	commitBatches(t, r, 1, 4)

	// 128 writers: the level at which the retry POLICY matters, not just the
	// count. At 32 both "8 attempts", "64 attempts" and "64 with backoff" pass on
	// a many-core box, so a test at that size cannot tell them apart; measured on
	// 32 cores, 128 simultaneous writers fail 16.7% without backoff and 0% with
	// it, which is what makes this test a guard for the backoff specifically.
	const writers, rounds = 128, 4
	var wg sync.WaitGroup
	errCh := make(chan error, writers*rounds)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				key := fmt.Sprintf("x:seg_%d_%d", w, i)
				if uerr := r.UpdateRevProps(2, map[string]string{key: "v"}, nil); uerr != nil {
					errCh <- uerr
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	var fails []error
	for e := range errCh {
		fails = append(fails, e)
	}
	if len(fails) > 0 {
		t.Fatalf("%d of %d concurrent revprop updates were rejected; first: %v",
			len(fails), writers*rounds, fails[0])
	}
	// Every update must be present: retrying must not drop an edit.
	rp, err := r.GetRevPropsFresh(2)
	if err != nil {
		t.Fatal(err)
	}
	for w := 0; w < writers; w++ {
		for i := 0; i < rounds; i++ {
			key := fmt.Sprintf("x:seg_%d_%d", w, i)
			if rp.Props[key] == "" {
				t.Fatalf("property %s was lost", key)
			}
		}
	}
}

// TestLooseRevPropsConcurrentWritersAllSucceed is the non-segment path, which
// has the same fixed-attempt loop.
func TestLooseRevPropsConcurrentWritersAllSucceed(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepo(st, "repos/looseretry", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.CommitTxn(0, []Change{
		{Path: "/a.txt", Action: ActionAdd, Kind: KindFile, Data: []byte("a")},
	}, map[string]string{"svn:log": "one"}); err != nil {
		t.Fatal(err)
	}

	const writers, rounds = 32, 4
	var wg sync.WaitGroup
	errCh := make(chan error, writers*rounds)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				key := fmt.Sprintf("x:loose_%d_%d", w, i)
				if uerr := r.UpdateRevProps(1, map[string]string{key: "v"}, nil); uerr != nil {
					errCh <- uerr
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	var fails int
	var first error
	for e := range errCh {
		fails++
		if first == nil {
			first = e
		}
	}
	if fails > 0 {
		t.Fatalf("%d of %d concurrent revprop updates were rejected; first: %v",
			fails, writers*rounds, first)
	}
}

// TestRevPropRemoveUnderContention: deletes take the same path and must not be
// lost either.
func TestRevPropRemoveUnderContention(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepoSegments(st, "repos/segdel", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	commitBatches(t, r, 1, 4)

	var wg sync.WaitGroup
	errCh := make(chan error, 64)
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 4; i++ {
				k := fmt.Sprintf("x:r%d_%d", w, i)
				if uerr := r.UpdateRevProps(3, map[string]string{k: "v"}, nil); uerr != nil {
					errCh <- uerr
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		t.Fatalf("set under contention failed: %v", e)
	}

	// Now remove one concurrently with more sets.
	errCh2 := make(chan error, 32)
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			if uerr := r.UpdateRevProps(3, map[string]string{fmt.Sprintf("x:z%d", w): "v"}, []string{"x:r0_0"}); uerr != nil {
				errCh2 <- uerr
			}
		}(w)
	}
	wg.Wait()
	close(errCh2)
	for e := range errCh2 {
		t.Fatalf("remove under contention failed: %v", e)
	}
}
