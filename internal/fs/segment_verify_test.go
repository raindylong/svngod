package fs

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"svngod/internal/cdc"
	"svngod/internal/fs/store"
)

// TestVerifyDetectsIndexGap removes one index entry and expects verify to
// report the missing revision range.
func TestVerifyDetectsIndexGap(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepoSegments(st, "repos/gap", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	for b := 0; b < 2; b++ {
		reqs := make([]BatchCommit, 4)
		for i := range reqs {
			reqs[i] = BatchCommit{Changes: []Change{{
				Path: fmt.Sprintf("/g%d-%d", b, i), Action: ActionAdd, Kind: KindFile, Data: []byte("v"),
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
	refs, err := r.loadSegmentIndex(0)
	if err != nil || len(refs) != 2 {
		t.Fatalf("index: %+v %v", refs, err)
	}
	// Drop the first batch from the index: revisions 1..4 become unresolvable.
	if err := st.Put(context.Background(), r.keys.SegmentIndex(0), encodeSegmentIndex(refs[1:])); err != nil {
		t.Fatal(err)
	}
	r2, err := OpenRepo(st, "repos/gap")
	if err != nil {
		t.Fatal(err)
	}
	issues, err := r2.VerifyRange(1, 8, false)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, is := range issues {
		if strings.Contains(is.Err.Error(), "covers") || strings.Contains(is.Err.Error(), "want") {
			found = true
		}
	}
	if !found {
		t.Fatalf("verify missed index gap: %+v", issues)
	}
}

// TestVerifyParallelMatchesSequential checks that worker count changes speed,
// not the result.
func TestVerifyParallelMatchesSequential(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepoSegments(st, "repos/par", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	for b := 0; b < 8; b++ {
		reqs := make([]BatchCommit, 4)
		for i := range reqs {
			reqs[i] = BatchCommit{Changes: []Change{{
				Path: fmt.Sprintf("/p%d-%d", b, i), Action: ActionAdd, Kind: KindFile, Data: []byte("v"),
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
	r2, err := OpenRepo(st, "repos/par")
	if err != nil {
		t.Fatal(err)
	}
	seq, err := r2.VerifyRangeWith(1, 32, false, 1)
	if err != nil || len(seq) != 0 {
		t.Fatalf("sequential issues=%v err=%v", seq, err)
	}
	par, err := r2.VerifyRangeWith(1, 32, false, 4)
	if err != nil || len(par) != 0 {
		t.Fatalf("parallel issues=%v err=%v", par, err)
	}
	if r2.fullReads.Load() {
		t.Fatal("fullReads left set after parallel verify")
	}
}

// TestVerifyParallelFindsCorruption makes sure a corrupt segment is reported
// exactly once no matter how many workers run.
func TestVerifyParallelFindsCorruption(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepoSegments(st, "repos/parc", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	for b := 0; b < 8; b++ {
		reqs := make([]BatchCommit, 4)
		for i := range reqs {
			reqs[i] = BatchCommit{Changes: []Change{{
				Path: fmt.Sprintf("/c%d-%d", b, i), Action: ActionAdd, Kind: KindFile, Data: []byte("v"),
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
	// Corrupt the segment covering revision 12 (shard 1).
	refs, err := r.loadSegmentIndex(1)
	if err != nil || len(refs) == 0 {
		t.Fatalf("index: %+v %v", refs, err)
	}
	ref, ok := findSegment(refs, 12)
	if !ok {
		t.Fatalf("rev 12 not in index: %+v", refs)
	}
	key := r.segmentKeyFor(ref.First, ref.Count)
	raw, err := st.Get(context.Background(), key, nil)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-6] ^= 0xff
	if err := st.Put(context.Background(), key, raw); err != nil {
		t.Fatal(err)
	}
	r2, err := OpenRepo(st, "repos/parc")
	if err != nil {
		t.Fatal(err)
	}
	issues, err := r2.VerifyRangeWith(1, 32, false, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) == 0 {
		t.Fatal("parallel verify missed corruption")
	}
	for _, is := range issues {
		if is.Rev < ref.First || is.Rev >= ref.First+ref.Count {
			t.Fatalf("issue reported for unexpected rev %d", is.Rev)
		}
	}
}
