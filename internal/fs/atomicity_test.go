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

// faultStore fails the Nth commit-path write (segment, shard index, WAL record
// or HEAD CAS). hard=true keeps failing after the first fault, modelling a
// process that died: nothing it does after the fault reaches the store.
//
// It reports Journaling so the WAL roll-forward path is exercised, as it is on
// the mirror stores used in production.
type faultStore struct {
	store.Store
	mu     sync.Mutex
	failAt int
	count  int
	hard   bool
	dead   bool
}

func (s *faultStore) Journaling() bool { return true }

func commitWrite(key string) bool {
	return strings.Contains(key, "/segs/") || strings.Contains(key, "/journal/") ||
		strings.HasSuffix(key, "/current")
}

func (s *faultStore) shouldFail(key string) bool {
	if !commitWrite(key) {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.count++
	if s.dead {
		return true
	}
	if s.failAt > 0 && s.count == s.failAt {
		if s.hard {
			s.dead = true
		}
		return true
	}
	return false
}

func (s *faultStore) Put(ctx context.Context, key string, data []byte) error {
	if s.shouldFail(key) {
		return fmt.Errorf("injected write failure at %s", key)
	}
	return s.Store.Put(ctx, key, data)
}

func (s *faultStore) CreateIfAbsent(ctx context.Context, key string, data []byte) error {
	if s.shouldFail(key) {
		return fmt.Errorf("injected write failure at %s", key)
	}
	return s.Store.CreateIfAbsent(ctx, key, data)
}

func (s *faultStore) CAS(ctx context.Context, key, expect string, data []byte) (string, error) {
	if s.shouldFail(key) {
		return "", fmt.Errorf("injected write failure at %s", key)
	}
	return s.Store.CAS(ctx, key, expect, data)
}

func (s *faultStore) writes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

func (s *faultStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.count = 0
	s.dead = false
}

// heal models a restarted process with a healthy store.
func (s *faultStore) heal() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failAt = 0
	s.hard = false
	s.dead = false
}

// TestCommitAtomicityUnderWriteFailures fails each commit write in turn and
// checks the repository never exposes a half-committed revision: HEAD is either
// the old or the new value, everything below HEAD is readable, and a following
// commit succeeds.
func TestCommitAtomicityUnderWriteFailures(t *testing.T) {
	for _, hard := range []bool{false, true} {
		mode := "soft"
		if hard {
			mode = "hard"
		}
		// span=2 stays inside one shard; span=10 crosses the shard boundary and
		// therefore writes two segments, two indexes and two WAL records with
		// the index updates running concurrently.
		for _, span := range []int{2, 10} {
			for k := 1; k <= 32; k++ {
				hit := false
				t.Run(fmt.Sprintf("%s-span%d-k%d", mode, span, k), func(t *testing.T) {
					hit = testOneFault(t, k, hard, span)
				})
				if !hit {
					break
				}
			}
		}
	}
}

func testOneFault(t *testing.T, failAt int, hard bool, span int) bool {
	st := &faultStore{Store: store.NewMemoryStore(), failAt: 0, hard: hard}
	r, err := CreateRepoSegments(st, "repos/atomic", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	commit := func(paths ...string) ([]uint64, []error, error) {
		reqs := make([]BatchCommit, len(paths))
		for i, p := range paths {
			reqs[i] = BatchCommit{Changes: []Change{{Path: p, Action: ActionAdd, Kind: KindFile, Data: []byte(p)}}}
		}
		return r.CommitBatch(reqs)
	}
	if _, errs, err := commit("/a1", "/a2"); err != nil || errs[0] != nil {
		t.Fatalf("baseline commit: %v %v", err, errs)
	}
	baseHead, _ := r.HeadFresh()
	if baseHead != 2 {
		t.Fatalf("base head=%d", baseHead)
	}

	st.reset()
	bPaths := make([]string, span)
	for i := range bPaths {
		bPaths[i] = fmt.Sprintf("/b%d", i+1)
	}
	st.failAt = failAt
	_, _, berr := commit(bPaths...)
	st.heal()
	hit := st.writes() >= failAt
	if !hit {
		return false // this write point does not exist for the attempt
	}

	// Model recovery: the crashed writer's lease is gone.
	ctx := context.Background()
	_ = st.Delete(ctx, []string{r.keys.CommitLock()})
	r2, err := OpenRepo(st, "repos/atomic")
	if err != nil {
		t.Fatal(err)
	}
	head, err := r2.HeadFresh()
	if err != nil {
		t.Fatal(err)
	}
	if head != baseHead && head != baseHead+uint64(span) {
		t.Fatalf("HEAD=%d after fault at write %d: commit is partially visible (berr=%v)", head, failAt, berr)
	}
	// Everything below HEAD must be readable and consistent.
	issues, err := r2.VerifyRange(1, head, false)
	if err != nil || len(issues) != 0 {
		t.Fatalf("verify after fault at %d: issues=%v err=%v", failAt, issues, err)
	}
	if head == baseHead+uint64(span) {
		for _, p := range bPaths {
			nr, rerr := r2.Resolve(head, p)
			if rerr != nil {
				t.Fatalf("published revision unreadable at %s: %v", p, rerr)
			}
			if txt, terr := r2.GetText(nr.Node); terr != nil || string(txt) != p {
				t.Fatalf("published %s content=%q err=%v", p, txt, terr)
			}
		}
		if berr == nil {
			t.Fatalf("commit reported success but write %d was injected", failAt)
		}
	} else {
		// Not published: the client saw an error and the paths are absent.
		if berr == nil {
			t.Fatalf("commit reported success but nothing was published (fault at %d)", failAt)
		}
		if _, rerr := r2.Resolve(head, "/b1"); rerr == nil {
			t.Fatalf("unpublished change is visible at HEAD=%d", head)
		}
	}

	// A following commit must succeed and land exactly at HEAD+1, proving the
	// failed attempt left no blocking residue.
	commit3 := func() (uint64, error) {
		reqs := []BatchCommit{{Changes: []Change{{Path: "/c1", Action: ActionAdd, Kind: KindFile, Data: []byte("c1")}}}}
		revs, errs, cerr := r2.CommitBatch(reqs)
		if cerr != nil {
			return 0, cerr
		}
		if errs[0] != nil {
			return 0, errs[0]
		}
		return revs[0], nil
	}
	rev, cerr := commit3()
	if cerr != nil {
		t.Fatalf("commit after fault at %d failed: %v", failAt, cerr)
	}
	if rev != head+1 {
		t.Fatalf("commit after fault landed at %d, want %d", rev, head+1)
	}
	issues, err = r2.VerifyRange(1, rev, false)
	if err != nil || len(issues) != 0 {
		t.Fatalf("verify after recovery commit: issues=%v err=%v", issues, err)
	}
	return true
}
