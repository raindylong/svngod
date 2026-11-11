package fs

import (
	"context"
	"sync"
	"testing"

	"svngod/internal/fs/store"
)

// indexRaceStore fires an injected callback right after the first Get of one
// key returns - deterministic read/write interleaving between two writers of
// the same segment index.
type indexRaceStore struct {
	store.Store
	mu   sync.Mutex
	key  string
	once sync.Once
	fire func()
}

func (s *indexRaceStore) Get(ctx context.Context, key string, rng *store.Range) ([]byte, error) {
	b, err := s.Store.Get(ctx, key, rng)
	s.mu.Lock()
	match := key == s.key
	s.mu.Unlock()
	if match {
		s.once.Do(s.fire)
	}
	return b, err
}

// TestSegmentIndexCASSurvivesConcurrentCreate: WAL replay appends its batch's
// ref while a peer creates the same shard's index in the read/write gap. The
// old replay path was load+Put and silently dropped the peer's ref; the CAS
// retry must end with BOTH refs. This is the lost-update primitive behind the
// replay fix.
func TestSegmentIndexCASSurvivesConcurrentCreate(t *testing.T) {
	base := store.NewMemoryStore()
	st := &indexRaceStore{Store: base}
	r := openSegRepo(t, st, "ir", 8, 8)
	ctx := context.Background()

	sh := r.shard(r.segmentFrom)
	key := r.keys.SegmentIndex(sh)
	refA := segmentRef{First: r.segmentFrom, Count: 2, SHA256: "aaaaaaaa"}
	refB := segmentRef{First: r.segmentFrom + 2, Count: 2, SHA256: "bbbbbbbb"}

	st.mu.Lock()
	st.key = key
	st.mu.Unlock()
	st.fire = func() {
		// Peer writes its own index after replay read the (missing) key.
		if err := base.CreateIfAbsent(ctx, key, encodeSegmentIndex([]segmentRef{refB})); err != nil {
			t.Errorf("peer create: %v", err)
		}
	}

	changed, err := r.appendSegmentIndexCAS(ctx, refA)
	if err != nil {
		t.Fatalf("CAS append: %v", err)
	}
	if !changed {
		t.Fatal("append reported no change, but the ref was added")
	}

	b, err := base.Get(ctx, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	refs, err := decodeSegmentIndex(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 2 || refs[0] != refA || refs[1] != refB {
		t.Fatalf("index lost an update: got %+v, want [%+v %+v]", refs, refA, refB)
	}

	// Idempotent: appending again changes nothing.
	if changed, err := r.appendSegmentIndexCAS(ctx, refA); err != nil || changed {
		t.Fatalf("re-append changed=%v err=%v, want false/nil", changed, err)
	}
}
