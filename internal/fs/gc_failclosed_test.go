package fs

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"svngod/internal/cdc"
	"svngod/internal/fs/store"
)

// readFaultStore fails Get for keys matching a predicate, leaving everything
// else working. It is used to prove that every deletion path refuses to run
// when a reference it needs cannot be read.
type readFaultStore struct {
	store.Store
	mu    sync.Mutex
	match func(key string) bool
}

func (s *readFaultStore) Get(ctx context.Context, key string, rng *store.Range) ([]byte, error) {
	s.mu.Lock()
	m := s.match
	s.mu.Unlock()
	if m != nil && rng == nil && m(key) {
		return nil, fmt.Errorf("injected read failure at %s", key)
	}
	return s.Store.Get(ctx, key, rng)
}

func (s *readFaultStore) setFault(m func(string) bool) {
	s.mu.Lock()
	s.match = m
	s.mu.Unlock()
}

// countPrefix reports how many live objects exist under prefix.
func countPrefix(t *testing.T, st store.Store, prefix string) int {
	t.Helper()
	n := 0
	next := ""
	for {
		objs, cont, err := st.ListPrefix(context.Background(), prefix, next, 500)
		if err != nil {
			t.Fatalf("list %s: %v", prefix, err)
		}
		n += len(objs)
		if cont == "" || cont == next {
			break
		}
		next = cont
	}
	return n
}

// TestGCDeletionPathsRefuseOnReadFailure is the guard for the class of bug that
// made gc delete live data three times: a reference scan that skips what it
// cannot read ends up with an incomplete reference set, and the next sweep
// deletes everything the missing entry pointed at.
//
// Each case builds a repository with something worth protecting, fails the read
// that a scan depends on, and asserts both that the sweep reports an error and
// that nothing was deleted.
//
// Note on coverage: the txn case fails on pre-fix code (that sweep used to
// `continue`), so it guards a real regression. The texts/segments cases pass
// before and after because those paths already propagated a block-level read
// error; they are here to keep it that way. The lost-index variant - where the
// index is absent rather than unreadable - is guarded by
// TestSegmentKeepSetRefusesLostIndex, which does fail on pre-fix code.
func TestGCDeletionPathsRefuseOnReadFailure(t *testing.T) {
	t.Run("texts: unreadable revision block", func(t *testing.T) {
		mem := store.NewMemoryStore()
		fs := &readFaultStore{Store: mem}
		r, err := CreateRepo(fs, "gc1", 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, data := chunkedFixture(t, 5<<20)
		if _, err := r.CommitTxn(0, []Change{{Path: "/f", Action: ActionAdd, Kind: KindFile, Data: data}}, nil); err != nil {
			t.Fatal(err)
		}
		before := countPrefix(t, mem, r.keys.Prefix+"/chunks/")
		if before == 0 {
			t.Fatal("fixture produced no chunks")
		}
		// A block that cannot be read hides every reference it holds. (A
		// *parseable-but-corrupt* manifest is covered separately by
		// TestCollectTextsKeepsChunksOfCorruptManifest; a block-level failure
		// must abort before the scan even starts.)
		fs.setFault(func(k string) bool { return strings.Contains(k, "/revs") || strings.Contains(k, "/segs/") })
		if _, err := r.CollectTexts(context.Background()); err == nil {
			t.Fatal("CollectTexts ran with an unreadable revision block")
		}
		if after := countPrefix(t, mem, r.keys.Prefix+"/chunks/"); after != before {
			t.Fatalf("chunks deleted despite a failed scan: %d -> %d", before, after)
		}
	})

	t.Run("segments: unreadable shard index", func(t *testing.T) {
		mem := store.NewMemoryStore()
		fs := &readFaultStore{Store: mem}
		r, err := CreateRepoSegments(fs, "gc2", 4, 64, cdc.DefaultParams(), false, 4)
		if err != nil {
			t.Fatal(err)
		}
		commitN(t, r, 3, 2)
		before := countPrefix(t, mem, r.keys.ListingPrefix("segs"))
		if before == 0 {
			t.Fatal("fixture produced no segments")
		}
		// MemoryStore stamps ModTime at write time, so nothing is old enough to
		// be an orphan candidate here; the sweep returns before reading an
		// index. Exercise the keep-set contract directly instead: it is the
		// gate that stops a lost index from looking like "no segments".
		fs.setFault(func(k string) bool { return strings.HasSuffix(k, ".idx") })
		head, herr := r.Head()
		if herr != nil {
			t.Fatal(herr)
		}
		if _, kerr := r.segmentKeepSet(head); kerr == nil {
			t.Fatal("keep set built from an unreadable shard index")
		}
		if after := countPrefix(t, mem, r.keys.ListingPrefix("segs")); after != before {
			t.Fatalf("segments deleted despite a failed index read: %d -> %d", before, after)
		}
	})

	t.Run("txn data: unreadable activity manifest", func(t *testing.T) {
		mem := store.NewMemoryStore()
		fs := &readFaultStore{Store: mem}
		r, err := CreateRepo(fs, "gc3", 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		base := r.keys.ListingPrefix("dav-txns")
		// A staged body plus the manifest that references it.
		body := base + "data/abc123"
		if err := mem.Put(ctx, body, []byte("upload body")); err != nil {
			t.Fatal(err)
		}
		manifest := base + "activity-1.json"
		if err := mem.Put(ctx, manifest, []byte(`{"dataRef":"abc123"}`)); err != nil {
			t.Fatal(err)
		}
		fs.setFault(func(k string) bool { return strings.HasSuffix(k, ".json") })
		if _, err := r.CollectTxnData(ctx); err == nil {
			t.Fatal("txn sweep ran with an unreadable activity manifest")
		}
		if ok, _ := mem.Exists(ctx, body); !ok {
			t.Fatal("staged upload body deleted despite an unreadable manifest")
		}
	})
}
