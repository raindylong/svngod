package fs

import (
	"context"
	"testing"

	"svngod/internal/cdc"
	"svngod/internal/fs/store"
)

// TestSegmentKeepSetRefusesLostIndex pins a data-safety property of the orphan
// sweep: a shard whose index object is missing must abort the sweep, not look
// like "this shard has no segments".
//
// loadSegmentIndex maps ErrNotFound to an empty list, which is indistinguishable
// from a shard that legitimately has no segments. Left unchecked, one lost
// index object would make every segment of that shard look unreferenced and the
// sweep would delete a whole shard of history.
func TestSegmentKeepSetRefusesLostIndex(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepoSegments(st, "keepset", 4, 64, cdc.DefaultParams(), false, 4)
	if err != nil {
		t.Fatal(err)
	}
	commitN(t, r, 3, 2) // several revisions across two shards
	head, err := r.Head()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.segmentKeepSet(head); err != nil {
		t.Fatalf("healthy repository must produce a keep set: %v", err)
	}

	// Remove the index of shard 0 while its segments stay in place.
	if err := st.Delete(context.Background(), []string{r.keys.SegmentIndex(0)}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.segmentKeepSet(head); err == nil {
		t.Fatal("keep set accepted a shard with a lost index: the orphan sweep would delete its segments")
	}
}
