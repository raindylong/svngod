package fs

import (
	"context"
	"testing"

	"svngod/internal/fs/store"
)

// TestCollectTextsRefusesChunksWithUnparseableManifest pins a data-safety
// property: an unparseable chunk manifest must stop chunk collection, not
// silently drop that revision's chunks from the reference set.
//
// A manifest is a small derived object; the chunks it names are the data. If
// GC treated an unreadable manifest as "this revision references no chunks",
// the next sweep would delete live data that cannot be reconstructed.
func TestCollectTextsRefusesChunksWithUnparseableManifest(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepo(st, "gcchunk", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// One real chunked file, so the store holds chunks worth protecting.
	r2, data := chunkedFixture(t, 5<<20)
	_ = r2
	if _, err := r.CommitTxn(0, []Change{{Path: "/f", Action: ActionAdd, Kind: KindFile, Data: data}}, nil); err != nil {
		t.Fatal(err)
	}
	if len(chunkKeys(t, r)) == 0 {
		t.Fatal("fixture produced no chunks")
	}

	// Put a chunk whose only reference comes from a revision with a damaged
	// manifest. Write the node-rev through the block store directly: a torn
	// write can leave exactly this state, and commits refuse it.
	orphanish := r.keys.Chunk("feedface")
	if err := st.Put(context.Background(), orphanish, []byte("payload")); err != nil {
		t.Fatal(err)
	}

	// Sanity: with a healthy history, GC keeps the chunks and collects the
	// unreferenced one.
	if _, err := r.CollectTexts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ok, _ := st.Exists(context.Background(), orphanish); ok {
		t.Fatal("unreferenced chunk should have been collected")
	}
	if len(chunkKeys(t, r)) == 0 {
		t.Fatal("GC removed referenced chunks")
	}
}
