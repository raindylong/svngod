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

func TestFileStoreRangedGet(t *testing.T) {
	dir := t.TempDir()
	st, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	data := make([]byte, 3<<20)
	for i := range data {
		data[i] = byte(i)
	}
	if err := st.Put(ctx, "obj", data); err != nil {
		t.Fatal(err)
	}
	cases := []struct{ off, length int64 }{
		{0, 1 << 20},
		{1<<20 + 123, 4096},
		{int64(len(data)) - 10, 10},
		{int64(len(data)) - 10, 100}, // clamped to the end
	}
	for _, c := range cases {
		got, gerr := st.Get(ctx, "obj", &Range{Offset: c.off, Length: c.length})
		if gerr != nil {
			t.Fatalf("range %d+%d: %v", c.off, c.length, gerr)
		}
		end := c.off + c.length
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		if want := data[c.off:end]; len(got) != len(want) || string(got) != string(want) {
			t.Fatalf("range %d+%d: got %d bytes, want %d", c.off, c.length, len(got), len(want))
		}
	}
	if _, gerr := st.Get(ctx, "obj", &Range{Offset: int64(len(data)) + 1}); gerr == nil {
		t.Fatal("expected out-of-bounds error")
	}
}

func TestFileStoreSyncsWrites(t *testing.T) {
	dir := t.TempDir()
	st, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	syncHook = func() { calls++ }
	defer func() { syncHook = nil }()

	ctx := context.Background()
	if err := st.Put(ctx, "a", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if calls < 2 {
		t.Fatalf("Put fsync calls=%d, want file+dir", calls)
	}
	before := calls
	if _, err := st.CAS(ctx, "a", ContentETag([]byte("x")), []byte("y")); err != nil {
		t.Fatal(err)
	}
	if calls <= before {
		t.Fatal("CAS did not fsync")
	}
}

func TestFileStoreNoSync(t *testing.T) {
	dir := t.TempDir()
	st, err := NewFileStoreWithSync(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	syncHook = func() { calls++ }
	defer func() { syncHook = nil }()
	if err := st.Put(context.Background(), "a", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("no-sync store fsynced %d times", calls)
	}
}

func TestFileStoreReconstructibleSkipsSync(t *testing.T) {
	dir := t.TempDir()
	st, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	syncHook = func() { calls++ }
	defer func() { syncHook = nil }()
	if err := PutReconstructible(context.Background(), st, "txn/frag", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("reconstructible write fsynced %d times", calls)
	}
	if err := st.Put(context.Background(), "rev", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if calls == 0 {
		t.Fatal("durable Put did not fsync")
	}
}
