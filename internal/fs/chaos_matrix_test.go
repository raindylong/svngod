package fs

// Chaos matrix: the institutionalised fault drill. Every crash/fault scenario
// the production code defends against is one row in a table; the same
// invariant checker judges every row. This file exists so a new failure mode
// is one more row, not another bespoke test somewhere else, and so `go test`
// (and thus scripts/ci.sh) runs the whole drill on every push.
//
// Fault dimensions already covered elsewhere are reused, not duplicated:
//   - write-fault atomicity  -> atomicity_test.go (faultStore, spans)
//   - WAL crash windows      -> journal_test.go (rewound HEAD fixtures) and
//                               SVNGOD_CHAOS_AFTER_WAL (process exit, used by
//                               the lab scripts)
//   - replica loss / repair  -> journal_test.go switchStore quorum fixtures
// This matrix adds the *cross-product drill*: each fault applied to the
// *system-level* invariants below.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"svngod/internal/fs/store"
)

// chaosCheck is the pass/fail contract for every scenario in the matrix.
// A scenario recovers (restart/reopen) after the fault, then must show:
//  1. HEAD is all-or-nothing for the in-flight commit (no partial batch);
//  2. everything at or below HEAD verifies clean (no corrupt objects);
//  3. a subsequent commit succeeds (the repo is not wedged);
//  4. any published revision of the in-flight path is readable.
func chaosCheck(t *testing.T, r *Repo, baseHead uint64, inFlight string, span int) {
	t.Helper()
	head, err := r.HeadFresh()
	if err != nil {
		t.Fatalf("head unreadable after recovery: %v", err)
	}
	if head != baseHead && head != baseHead+uint64(span) {
		t.Fatalf("head=%d after fault, want %d (aborted) or %d (all published): partial batch",
			head, baseHead, baseHead+uint64(span))
	}
	issues, err := r.VerifyRange(1, head, true)
	if err != nil || len(issues) > 0 {
		t.Fatalf("verify after recovery: err=%v issues=%v", err, issues)
	}
	if _, err := r.CommitTxn(head, []Change{{
		Path: "/chaos-next", Action: ActionAdd, Kind: KindFile, Data: []byte("post-fault commit\n"),
	}}, nil); err != nil {
		t.Fatalf("repo wedged after fault: next commit failed: %v", err)
	}
	if head > baseHead { // the in-flight batch was published: its path must read
		nr, err := r.Resolve(head, inFlight)
		if err != nil || nr == nil {
			t.Fatalf("published %s at rev %d unreadable: %v", inFlight, head, err)
		}
	}
}

// chaosScenario is one fault drill: a name, a setup that commits `span`
// changes of `path` under a fault, and the reopen that models a restart.
type chaosScenario struct {
	name string
	run  func(t *testing.T) (*Repo, uint64, string, int)
}

// chaosMirrorRepo builds the standard drill target: a 3-replica mirror,
// quorum 2, one switchable replica. span commits create the in-flight batch.
func chaosMirrorRepo(t *testing.T, span int) (*Repo, *switchStore, *switchStore, *store.MirrorStore) {
	t.Helper()
	a, b, c := store.NewMemoryStore(), &switchStore{Store: store.NewMemoryStore()}, &switchStore{Store: store.NewMemoryStore()}
	ms := store.NewMirror([]store.Store{a, b, c}, store.WithWriteQuorum(2))
	r, err := CreateRepo(ms, "ch", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	return r, b, c, ms
}

func chaosCommit(r *Repo, base uint64, span int) ([]uint64, error) {
	reqs := make([]BatchCommit, span)
	for i := range reqs {
		reqs[i] = BatchCommit{BaseRev: base, Changes: []Change{{
			Path: fmt.Sprintf("/cf%d", i+1), Action: ActionAdd, Kind: KindFile, Data: []byte("x"),
		}}}
	}
	revs, _, err := r.CommitBatch(reqs)
	return revs, err
}

// tornStore corrupts writes to a key pattern: the write succeeds but the
// stored bytes differ (a partial write that reached the replica).
type tornStore struct {
	store.Store
	pattern string
	mu      sync.Mutex
	torn    map[string]bool
}

func (s *tornStore) Put(ctx context.Context, key string, data []byte) error {
	if strings.Contains(key, s.pattern) {
		s.mu.Lock()
		s.torn[key] = true
		s.mu.Unlock()
		return s.Store.Put(ctx, key, append(data, []byte("TORN")...))
	}
	return s.Store.Put(ctx, key, data)
}

// TestChaosMatrix runs every institutionalised fault drill.
func TestChaosMatrix(t *testing.T) {
	const span = 4
	scenarios := []chaosScenario{
		{
			name: "replica-loss-during-commit",
			run: func(t *testing.T) (*Repo, uint64, string, int) {
				r, b, _, _ := chaosMirrorRepo(t, span)
				base, _ := r.Head()
				b.down.Store(true) // one of two quorum-able replicas disappears mid-flight
				_, _ = chaosCommit(r, base, span)
				b.down.Store(false)
				r2, err := OpenRepo(r.Store(), "ch")
				if err != nil {
					t.Fatal(err)
				}
				return r2, base, "/cf1", span
			},
		},
		{
			name: "both-switchable-replicas-lost",
			run: func(t *testing.T) (*Repo, uint64, string, int) {
				r, b, c, ms := chaosMirrorRepo(t, span)
				base, _ := r.Head()
				b.down.Store(true)
				c.down.Store(true)
				_, _ = chaosCommit(r, base, span) // must fail or abort; nothing published
				// Bring the replicas back and let the mirror's health loop
				// re-admit them (probe interval is seconds; poll for it).
				b.down.Store(false)
				c.down.Store(false)
				deadline := time.Now().Add(20 * time.Second)
				for time.Now().Before(deadline) {
					healthy := 0
					for _, st := range ms.Status() {
						if st.Healthy {
							healthy++
						}
					}
					if healthy == 3 {
						break
					}
					time.Sleep(200 * time.Millisecond)
				}
				r2, err := OpenRepo(ms, "ch")
				if err != nil {
					t.Fatal(err)
				}
				return r2, base, "/cf1", span
			},
		},
		{
			name: "torn-revblock-write",
			run: func(t *testing.T) (*Repo, uint64, string, int) {
				ts := &tornStore{Store: store.NewMemoryStore(), pattern: "/revs/", torn: map[string]bool{}}
				r, err := CreateRepo(ts, "tn", 4096, 64)
				if err != nil {
					t.Fatal(err)
				}
				base, _ := r.Head()
				_, _ = chaosCommit(r, base, span) // torn block; WAL should refuse to publish
				r2, err := OpenRepo(ts, "tn")
				if err != nil {
					t.Fatal(err)
				}
				return r2, base, "/cf1", span
			},
		},
	}
	for _, sc := range scenarios {
		sc := sc
		t.Run(sc.name, func(t *testing.T) {
			r, base, path, sp := sc.run(t)
			chaosCheck(t, r, base, path, sp)
		})
	}
}
