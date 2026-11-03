package fs

import (
	"context"
	"testing"
	"time"

	"svngod/internal/fs/store"
)

// TestCollectTextsGraceKeepsFresh: an upload writes texts before the commit
// that references them. A GC pass running concurrently must not delete those
// young, currently unreferenced objects.
func TestCollectTextsGraceKeepsFresh(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemoryStore()
	r, err := CreateRepo(st, "gcg", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	key := r.Keys().Text("deadbeef")
	if err := st.Put(ctx, key, []byte("not referenced yet")); err != nil {
		t.Fatal(err)
	}
	if n, err := r.CollectTextsGrace(ctx, time.Hour); err != nil || n != 0 {
		t.Fatalf("grace pass removed %d objects, err=%v", n, err)
	}
	if _, ok, _ := st.Head(ctx, key); !ok {
		t.Fatal("young unreferenced text was collected")
	}
	if n, err := r.CollectTexts(ctx); err != nil || n != 1 {
		t.Fatalf("no-grace pass removed %d, want 1 (err=%v)", n, err)
	}
	if _, ok, _ := st.Head(ctx, key); ok {
		t.Fatal("old unreferenced text was not collected")
	}
}

// TestCollectTxnDataGraceKeepsFresh: a fragment body is written before the
// manifest referencing it is updated; a racing GC must leave it alone.
func TestCollectTxnDataGraceKeepsFresh(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemoryStore()
	r, err := CreateRepo(st, "gct", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	key := r.Keys().Prefix + "/dav-txns/data/abc123"
	if err := st.Put(ctx, key, []byte("fragment")); err != nil {
		t.Fatal(err)
	}
	if n, err := r.CollectTxnDataGrace(ctx, time.Hour); err != nil || n != 0 {
		t.Fatalf("grace pass removed %d bodies, err=%v", n, err)
	}
	if _, ok, _ := st.Head(ctx, key); !ok {
		t.Fatal("young unreferenced body was collected")
	}
	if n, err := r.CollectTxnData(ctx); err != nil || n != 1 {
		t.Fatalf("no-grace pass removed %d, want 1 (err=%v)", n, err)
	}
}
