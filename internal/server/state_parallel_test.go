package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"svngod/internal/fs"
	"svngod/internal/fs/store"
)

// concStore records how many Gets overlap and adds a little latency so
// concurrency can actually build up.
type concStore struct {
	store.Store
	mu  sync.Mutex
	cur int
	max int
}

func (s *concStore) Get(ctx context.Context, key string, rng *store.Range) ([]byte, error) {
	s.mu.Lock()
	s.cur++
	if s.cur > s.max {
		s.max = s.cur
	}
	s.mu.Unlock()
	b, err := s.Store.Get(ctx, key, rng)
	time.Sleep(2 * time.Millisecond)
	s.mu.Lock()
	s.cur--
	s.mu.Unlock()
	return b, err
}

func (s *concStore) maxConcurrent() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.max
}

// TestLoadChangeFragmentsParallel stages many change fragments and checks that
// they are fetched concurrently and all come back intact.
func TestLoadChangeFragmentsParallel(t *testing.T) {
	cs := &concStore{Store: store.NewMemoryStore()}
	repo, err := fs.CreateRepo(cs, "repos/txnp", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	const n = 40
	actID := "act-parallel"
	for i := 0; i < n; i++ {
		pc := persistedChange{Path: fmt.Sprintf("/dir/f%02d.txt", i)}
		data, merr := json.Marshal(pc)
		if merr != nil {
			t.Fatal(merr)
		}
		key := activityChangeKey(repo, actID, pc.Path)
		if err := store.PutReconstructible(ctx, cs, key, data); err != nil {
			t.Fatal(err)
		}
	}
	frags := loadChangeFragments(ctx, repo, actID)
	if len(frags) != n {
		t.Fatalf("loaded %d fragments, want %d", len(frags), n)
	}
	seen := map[string]bool{}
	for _, pc := range frags {
		seen[pc.Path] = true
	}
	for i := 0; i < n; i++ {
		if p := fmt.Sprintf("/dir/f%02d.txt", i); !seen[p] {
			t.Fatalf("fragment %s missing", p)
		}
	}
	if got := cs.maxConcurrent(); got < 8 {
		t.Fatalf("fragments were read with max concurrency %d, want >= 8", got)
	}
}
