package server

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"svngod/internal/fs"
	"svngod/internal/fs/store"
)

// Two request paths here wait on a group of goroutines with no bound of their
// own, and both use context.Background() for the store calls inside them:
//
//   - loadChangeFragments (state.go) reads a transaction's change fragments in
//     parallel then wg.Wait()s.
//   - the bundle chunk paths (bundle.go) run a worker pool over chunks then
//     wg.Wait()s.
//
// http.Server sets ReadHeaderTimeout and IdleTimeout but deliberately no
// ReadTimeout/WriteTimeout (multi-gigabyte uploads are expected), so a request
// context carries no deadline. A store call that blocks - a wedged gateway, an
// SDK that ignores ctx - therefore parks the handler forever, holding every
// goroutine it spawned.
//
// The tests model the worst case: a store that ignores cancellation.

// stallingStore blocks selected operations until released, ignoring ctx.
type stallingStore struct {
	store.Store
	release chan struct{}
	hit     chan struct{}
	once    sync.Once
}

func newStallingStore(inner store.Store) *stallingStore {
	return &stallingStore{
		Store:   inner,
		release: make(chan struct{}),
		hit:     make(chan struct{}, 16),
	}
}

func (s *stallingStore) block(ctx context.Context) {
	select {
	case s.hit <- struct{}{}:
	default:
	}
	<-s.release // deliberately does not watch ctx
}

func (s *stallingStore) Get(ctx context.Context, key string, rng *store.Range) ([]byte, error) {
	s.block(ctx)
	return s.Store.Get(context.Background(), key, rng)
}

func (s *stallingStore) Exists(ctx context.Context, key string) (bool, error) {
	s.block(ctx)
	return s.Store.Exists(context.Background(), key)
}

func (s *stallingStore) ListPrefix(ctx context.Context, prefix, cont string, max int) ([]store.ListedObj, string, error) {
	// Listing is used to discover fragment keys before the parallel reads; keep
	// it working so the test reaches the read phase.
	return s.Store.ListPrefix(ctx, prefix, cont, max)
}

func (s *stallingStore) unblock() { s.once.Do(func() { close(s.release) }) }

// TestLoadChangeFragmentsDoesNotHangOnStalledStore fails on the current code:
// the fragment reads block, wg.Wait() has no bound, and the call never returns.
func TestLoadChangeFragmentsDoesNotHangOnStalledStore(t *testing.T) {
	mem := store.NewMemoryStore()
	st := newStallingStore(mem)
	defer st.unblock()

	repo, err := fs.CreateRepo(st, "stall", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	// Seed a couple of fragment objects so the loader has work to do.
	ctx := context.Background()
	if err := st.Store.Put(ctx, repo.Keys().Prefix+"/dav-txns/changes/act-1/aa", []byte(`{"Path":"/a","Action":"add","Kind":"file"}`)); err != nil {
		t.Fatal(err)
	}
	if err := st.Store.Put(ctx, repo.Keys().Prefix+"/dav-txns/changes/act-1/bb", []byte(`{"Path":"/b","Action":"add","Kind":"file"}`)); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = loadChangeFragments(context.Background(), repo, "act-1")
	}()

	select {
	case <-st.hit:
	case <-time.After(2 * time.Second):
		t.Fatal("fragment read never started")
	}
	select {
	case <-done:
	case <-time.After(fragmentReadTimeout + 10*time.Second):
		t.Fatal("loadChangeFragments never returned on a stalled store")
	}
}

// TestLoadChangeFragmentsHealthyStillWorks is the false-positive guard.
func TestLoadChangeFragmentsHealthyStillWorks(t *testing.T) {
	mem := store.NewMemoryStore()
	repo, err := fs.CreateRepo(mem, "ok", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	if err := mem.Put(context.Background(), repo.Keys().Prefix+"/dav-txns/changes/act-2/aa",
		[]byte(`{"Path":"/a","Action":"add","Kind":"file"}`)); err != nil {
		t.Fatal(err)
	}
	got := loadChangeFragments(context.Background(), repo, "act-2")
	if len(got) != 1 || got[0].Path != "/a" {
		t.Fatalf("healthy load returned %d fragments: %+v", len(got), got)
	}
}

// TestLoadChangeFragmentsDispatchIsBounded covers the subtler half: acquiring a
// semaphore slot happens in the dispatch loop, so with fragmentParallelism
// workers already stalled the loop itself parks on sem. Waiting on the group
// afterwards is then unreachable, and the timeout added for the workers never
// runs. Needs more fragments than there are slots, which is why it is separate.
func TestLoadChangeFragmentsDispatchIsBounded(t *testing.T) {
	mem := store.NewMemoryStore()
	st := newStallingStore(mem)
	defer st.unblock()

	repo, err := fs.CreateRepo(st, "stall2", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	// More fragments than fragmentParallelism so every slot fills and the
	// dispatch loop has to wait for one.
	n := fragmentParallelism * 3
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%s/dav-txns/changes/act-many/%03d", repo.Keys().Prefix, i)
		if err := mem.Put(ctx, key, []byte(`{"Path":"/x","Action":"add","Kind":"file"}`)); err != nil {
			t.Fatal(err)
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = loadChangeFragments(context.Background(), repo, "act-many")
	}()

	select {
	case <-st.hit:
	case <-time.After(2 * time.Second):
		t.Fatal("fragment read never started")
	}
	// Must return even though every worker is stuck and the dispatch loop is
	// waiting for a free slot.
	select {
	case <-done:
	case <-time.After(fragmentReadTimeout + 10*time.Second):
		t.Fatal("loadChangeFragments never returned: the dispatch loop was blocked on a full semaphore")
	}
}
