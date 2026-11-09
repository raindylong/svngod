package fs

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"svngod/internal/cdc"
	"svngod/internal/fs/store"
)

// TestSegmentRevPropsOverride edits a revision property on a segment
// repository and checks that every read path (cached, fresh, reopened) sees it.
func TestSegmentRevPropsOverride(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepoSegments(st, "repos/segrev", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	commitBatches(t, r, 1, 4)
	if err := r.UpdateRevProps(2, map[string]string{"svn:log": "edited", "x:test": "v"}, nil); err != nil {
		t.Fatal(err)
	}
	for _, get := range []func(uint64) (*RevProps, error){r.GetRevProps, r.GetRevPropsFresh} {
		rp, err := get(2)
		if err != nil {
			t.Fatal(err)
		}
		if rp.Props["svn:log"] != "edited" || rp.Props["x:test"] != "v" {
			t.Fatalf("props=%+v", rp.Props)
		}
	}
	// Other revisions in the same segment are untouched.
	if rp, err := r.GetRevProps(3); err != nil {
		t.Fatal(err)
	} else if rp.Props["svn:log"] == "edited" {
		t.Fatalf("override leaked to rev 3: %+v", rp.Props)
	}
	// A new process (fresh Repo) must see the override too.
	r2, err := OpenRepo(st, "repos/segrev")
	if err != nil {
		t.Fatal(err)
	}
	if rp, err := r2.GetRevProps(2); err != nil || rp.Props["svn:log"] != "edited" {
		t.Fatalf("reopened props=%+v err=%v", rp, err)
	}
	// Removal works and does not resurrect the committed value.
	if err := r.UpdateRevProps(2, nil, []string{"x:test"}); err != nil {
		t.Fatal(err)
	}
	if rp, err := r.GetRevPropsFresh(2); err != nil || rp.Props["x:test"] != "" {
		t.Fatalf("remove failed: %+v err=%v", rp, err)
	}
	if rp, err := r.GetRevPropsFresh(2); err != nil || rp.Props["svn:log"] != "edited" {
		t.Fatalf("other prop lost: %+v err=%v", rp, err)
	}
}

// TestSegmentRevPropsConcurrent updates two different properties of the same
// revision through two Repo instances (separate caches, shared store). Both
// updates must survive.
func TestSegmentRevPropsConcurrent(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepoSegments(st, "repos/segrevc", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	commitBatches(t, r, 1, 4)
	r2, err := OpenRepo(st, "repos/segrevc")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errCh := make(chan error, 20)
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			if err := r.UpdateRevProps(2, map[string]string{"x:a": fmt.Sprintf("a%d", i)}, nil); err != nil {
				errCh <- err
			}
		}(i)
		go func(i int) {
			defer wg.Done()
			if err := r2.UpdateRevProps(2, map[string]string{"x:b": fmt.Sprintf("b%d", i)}, nil); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent update: %v", err)
	}
	rp, err := r2.GetRevPropsFresh(2)
	if err != nil {
		t.Fatal(err)
	}
	if rp.Props["x:a"] == "" || rp.Props["x:b"] == "" {
		t.Fatalf("one writer's update was lost: %+v", rp.Props)
	}
}

// TestSegmentRevPropsNoExtraRead checks the fast path: revisions without an
// override must cost no extra object request once the shard's override list is
// cached.
func TestSegmentRevPropsNoExtraRead(t *testing.T) {
	base := store.NewMemoryStore()
	rs := &readSizer{Store: base}
	r, err := CreateRepoSegments(rs, "repos/segrevr", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	commitBatches(t, r, 2, 4) // shard 0: revs 1..7
	r2, err := OpenRepo(rs, "repos/segrevr")
	if err != nil {
		t.Fatal(err)
	}
	idxKey := r2.keys.SegRevPropsIndex(0)
	rs.reset()
	for i := 0; i < 5; i++ {
		if _, err := r2.GetRevProps(3); err != nil {
			t.Fatal(err)
		}
	}
	if _, calls := rs.transferred(idxKey); calls > 1 {
		t.Fatalf("override index read %d times for 5 reads", calls)
	}
	overrideKey := r2.keys.SegRevPropsOverride(0, 3)
	if _, calls := rs.transferred(overrideKey); calls != 0 {
		t.Fatalf("override object fetched %d times for an unoverridden revision", calls)
	}
}

// TestSegmentRevPropsRevZero keeps the loose path for revision 0.
func TestSegmentRevPropsRevZero(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepoSegments(st, "repos/segrev0", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	commitBatches(t, r, 1, 2)
	if err := r.UpdateRevProps(0, map[string]string{"x:zero": "z"}, nil); err != nil {
		t.Fatal(err)
	}
	rp, err := r.GetRevPropsFresh(0)
	if err != nil {
		t.Fatal(err)
	}
	if rp.Props["x:zero"] != "z" {
		t.Fatalf("rev 0 props=%+v", rp.Props)
	}
	// No segment override object should have been created for rev 0.
	if ok, _ := st.Exists(context.Background(), r.keys.SegRevPropsOverride(0, 0)); ok {
		t.Fatal("rev 0 override stored in the segment overlay")
	}
}
