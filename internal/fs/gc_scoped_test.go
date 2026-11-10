package fs

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"svngod/internal/cdc"
	"svngod/internal/fs/store"
)

// scopeStore models a store whose repository prefix lives inside the backend
// (svnadmin --endpoint, S3Store): keys handed in carry no repository prefix and
// ListPrefix returns them relative to the store, without a leading slash.
type scopeStore struct{ store.Store }

func tkey(k string) string { return strings.TrimPrefix(k, "/") }

func (s *scopeStore) Put(ctx context.Context, key string, data []byte) error {
	return s.Store.Put(ctx, tkey(key), data)
}

func (s *scopeStore) PutFrom(ctx context.Context, key string, r io.Reader, size int64) error {
	return s.Store.PutFrom(ctx, tkey(key), r, size)
}

func (s *scopeStore) Get(ctx context.Context, key string, rng *store.Range) ([]byte, error) {
	return s.Store.Get(ctx, tkey(key), rng)
}

func (s *scopeStore) GetTo(ctx context.Context, key string, w io.Writer) (int64, error) {
	return s.Store.GetTo(ctx, tkey(key), w)
}

func (s *scopeStore) Exists(ctx context.Context, key string) (bool, error) {
	return s.Store.Exists(ctx, tkey(key))
}

func (s *scopeStore) Head(ctx context.Context, key string) (string, bool, error) {
	return s.Store.Head(ctx, tkey(key))
}

func (s *scopeStore) CreateIfAbsent(ctx context.Context, key string, data []byte) error {
	return s.Store.CreateIfAbsent(ctx, tkey(key), data)
}

func (s *scopeStore) CAS(ctx context.Context, key, expect string, data []byte) (string, error) {
	return s.Store.CAS(ctx, tkey(key), expect, data)
}

func (s *scopeStore) Delete(ctx context.Context, keys []string) error {
	cp := make([]string, len(keys))
	for i, k := range keys {
		cp[i] = tkey(k)
	}
	return s.Store.Delete(ctx, cp)
}

func (s *scopeStore) ListPrefix(ctx context.Context, prefix, cont string, max int) ([]store.ListedObj, string, error) {
	return s.Store.ListPrefix(ctx, tkey(prefix), cont, max)
}

// listAll returns every object key under a repo-relative prefix.
func listAllKeys(t *testing.T, st store.Store, prefix string) []string {
	t.Helper()
	var out []string
	next := ""
	for {
		objs, cont, err := st.ListPrefix(context.Background(), prefix, next, 1000)
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range objs {
			out = append(out, o.Key)
		}
		if cont == "" || cont == next {
			return out
		}
		next = cont
	}
}

func plantOld(t *testing.T, dir, key string, data []byte) {
	t.Helper()
	p := filepath.Join(dir, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(p, old, old); err != nil {
		t.Fatal(err)
	}
}

// TestScopedStoreGCKeepsLiveObjects is a regression guard for key forms: when
// the repository prefix is empty (scoped store), GC used to compare listed keys
// ("texts/<digest>") with reference keys ("/texts/<digest>"), match nothing and
// delete live data. It must keep every live text/chunk/dir/segment object and
// delete only the planted orphans.
func TestScopedStoreGCKeepsLiveObjects(t *testing.T) {
	dir := t.TempDir()
	inner, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	st := &scopeStore{Store: inner}
	r, err := CreateRepoSegments(st, "", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	big := make([]byte, 2<<20)
	for i := range big {
		big[i] = byte(i * 31)
	}
	reqs := []BatchCommit{{Changes: []Change{
		{Path: "/small.txt", Action: ActionAdd, Kind: KindFile, Data: []byte("hello")},
		{Path: "/big.bin", Action: ActionAdd, Kind: KindFile, Data: big},
	}}}
	if _, errs, err := r.CommitBatch(reqs); err != nil || errs[0] != nil {
		t.Fatalf("commit: %v %v", err, errs)
	}

	live := map[string][]string{}
	for _, prefix := range []string{"texts/", "chunks/", "dirs/", "segs/"} {
		live[prefix] = listAllKeys(t, st, prefix)
	}
	if len(live["chunks/"])+len(live["texts/"]) == 0 {
		t.Fatalf("commit produced no text/chunk objects: %v", live)
	}

	// Plant an unreferenced object under each prefix, old enough to collect.
	plantOld(t, dir, "texts/"+strings.Repeat("ab", 32), []byte("orphan"))
	plantOld(t, dir, "chunks/"+strings.Repeat("cd", 32), []byte("orphan"))
	plantOld(t, dir, "segs/0000000000/000000000009-000000000001.seg", []byte("orphan"))

	if n, err := r.CollectTextsGrace(context.Background(), 0); err != nil || n != 2 {
		t.Fatalf("text GC deleted %d (err=%v), want 2", n, err)
	}
	if n, err := r.CollectOrphanSegments(context.Background(), 0); err != nil || n != 1 {
		t.Fatalf("segment GC deleted %d (err=%v), want 1", n, err)
	}
	for prefix, keys := range live {
		for _, k := range keys {
			ok, err := st.Exists(context.Background(), k)
			if err != nil || !ok {
				t.Fatalf("live object %s under %s was deleted (err=%v)", k, prefix, err)
			}
		}
	}
	for _, k := range []string{"texts/" + strings.Repeat("ab", 32), "chunks/" + strings.Repeat("cd", 32)} {
		if ok, _ := st.Exists(context.Background(), k); ok {
			t.Fatalf("orphan %s survived", k)
		}
	}
}
