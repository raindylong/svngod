package store

import (
	"context"
	"fmt"
	"testing"
)

// TestFileStoreListPrefixPages checks that paging through a prefix with the
// continuation key terminates and yields every object exactly once. Without
// cursor support the same first page came back forever, which hung session
// restore for repositories with more objects than the page size.
func TestFileStoreListPrefixPages(t *testing.T) {
	ctx := context.Background()
	st, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const n = 25
	for i := 0; i < n; i++ {
		if err := st.Put(ctx, fmt.Sprintf("repos/r1/dav-txns/activity-%03d.json", i), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	seen := make(map[string]bool, n)
	next := ""
	for pages := 0; ; pages++ {
		if pages > n {
			t.Fatalf("listing did not terminate after %d pages", pages)
		}
		objs, tok, err := st.ListPrefix(ctx, "repos/r1/dav-txns/", next, 4)
		if err != nil {
			t.Fatal(err)
		}
		if len(objs) > 4 {
			t.Fatalf("page too large: %d", len(objs))
		}
		for _, o := range objs {
			if seen[o.Key] {
				t.Fatalf("key repeated across pages: %s", o.Key)
			}
			seen[o.Key] = true
		}
		if tok == "" {
			break
		}
		next = tok
	}
	if len(seen) != n {
		t.Fatalf("saw %d objects, want %d", len(seen), n)
	}
}

// TestFileStoreDeletePrefix removes a whole session subtree in one call.
func TestFileStoreDeletePrefix(t *testing.T) {
	ctx := context.Background()
	st, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := st.Put(ctx, fmt.Sprintf("repos/r/dav-txns/changes/a/%d", i), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Put(ctx, "repos/r/keep", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := st.DeletePrefix(ctx, "repos/r/dav-txns/changes/a/"); err != nil {
		t.Fatal(err)
	}
	objs, _, err := st.ListPrefix(ctx, "repos/r/dav-txns/changes/a/", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 0 {
		t.Fatalf("prefix not empty: %v", objs)
	}
	if ok, err := st.Exists(ctx, "repos/r/keep"); err != nil || !ok {
		t.Fatalf("unrelated object removed (ok=%v err=%v)", ok, err)
	}
	if err := st.DeletePrefix(ctx, "../escape"); err == nil {
		t.Fatal("expected escape rejection")
	}
}
