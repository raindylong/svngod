package store

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A repair pass bounds the work it hands to each mirror and to its copy workers,
// but the two aggregate waits at the end - the listing join and the copy join -
// had no upper bound. A store that ignores cancellation parks a worker forever,
// so the join parks the pass forever, and the pass is the only mechanism that
// brings a standby back.
//
// Same shape as the quorum write aggregate (ea9b8b0) and the fragment read batch
// (aa0bb20): the per-item timeout exists, nothing bounds the join.

// hangPhase says which store call should block.
type hangPhase int

const (
	hangNothing hangPhase = iota
	hangPut               // only writes block (copy phase)
	hangAll               // every call blocks (listing phase too)
)

// blockingStore blocks the selected calls until released, ignoring ctx.
type blockingStore struct {
	Store
	phase   atomic.Int32
	release chan struct{}
	hit     chan struct{}
	once    sync.Once
}

func newBlockingStore(inner Store) *blockingStore {
	return &blockingStore{
		Store:   inner,
		release: make(chan struct{}),
		hit:     make(chan struct{}, 16),
	}
}

func (b *blockingStore) setPhase(p hangPhase) { b.phase.Store(int32(p)) }

func (b *blockingStore) blockedFor(what hangPhase) bool {
	p := hangPhase(b.phase.Load())
	return p == hangAll || (p == hangPut && what == hangPut)
}

func (b *blockingStore) blockIf(what hangPhase) {
	if !b.blockedFor(what) {
		return
	}
	select {
	case b.hit <- struct{}{}:
	default:
	}
	<-b.release
}

func (b *blockingStore) unblock() { b.once.Do(func() { close(b.release) }) }

func (b *blockingStore) Get(ctx context.Context, key string, rng *Range) ([]byte, error) {
	b.blockIf(hangAll)
	return b.Store.Get(context.Background(), key, rng)
}

func (b *blockingStore) Put(ctx context.Context, key string, data []byte) error {
	b.blockIf(hangPut)
	return b.Store.Put(context.Background(), key, data)
}

func (b *blockingStore) ListPrefix(ctx context.Context, prefix, cont string, max int) ([]ListedObj, string, error) {
	b.blockIf(hangAll)
	return b.Store.ListPrefix(context.Background(), prefix, cont, max)
}

func (b *blockingStore) Head(ctx context.Context, key string) (string, bool, error) {
	b.blockIf(hangAll)
	return b.Store.Head(context.Background(), key)
}

// shortRepairTimeouts keeps the tests measuring the bound, not the wall clock.
//
// These are per-instance, not package variables: a background repair loop from
// another test may be reading the package defaults at the same moment, and
// mutating them is a data race under -race in the full suite.
func (m *MirrorStore) shortRepairTimeouts() {
	m.listTimeout = 2 * time.Second
	m.copyTimeout = 2 * time.Second
	m.joinGrace = 2 * time.Second
}

// TestRepairCopyJoinIsBounded covers the copy phase: listing succeeds, then the
// destination stops accepting writes. The pass must still return.
func TestRepairCopyJoinIsBounded(t *testing.T) {
	ctx := context.Background()

	a := NewMemoryStore()
	b := newBlockingStore(NewMemoryStore())
	defer b.unblock()

	m := NewMirror([]Store{a, b}, WithRepoPrefix("repos/x"))
	m.shortRepairTimeouts()
	defer m.Close()

	for i := 0; i < 200; i++ {
		key := "repos/x/texts/t" + itoaTemp(i)
		if err := a.Put(ctx, key, []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	// Block only writes, so the listing phase completes normally and the pass
	// actually reaches the copy join.
	b.setPhase(hangPut)

	done := make(chan struct{})
	go func() {
		defer close(done)
		m.RepairNow()
	}()

	select {
	case <-b.hit:
	case <-time.After(5 * time.Second):
		t.Fatal("repair never reached the copy phase")
	}
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("repair pass never returned: the copy join had no bound on a store that ignores ctx")
	}
}

// TestRepairListingJoinIsBounded covers the listing phase: one mirror answers a
// probe but then never finishes listing. The pass must still return.
func TestRepairListingJoinIsBounded(t *testing.T) {
	ctx := context.Background()

	a := NewMemoryStore()
	b := newBlockingStore(NewMemoryStore())
	defer b.unblock()

	m := NewMirror([]Store{a, b}, WithRepoPrefix("repos/x"))
	m.shortRepairTimeouts()
	defer m.Close()

	for i := 0; i < 50; i++ {
		key := "repos/x/texts/l" + itoaTemp(i)
		if err := a.Put(ctx, key, []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	// Every call blocks, so the listing goroutine for b parks in the store.
	b.setPhase(hangAll)

	done := make(chan struct{})
	go func() {
		defer close(done)
		m.RepairNow()
	}()

	select {
	case <-b.hit:
	case <-time.After(5 * time.Second):
		t.Fatal("repair never touched the stalling mirror")
	}
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("repair pass never returned: the listing join had no bound on a store that ignores ctx")
	}
}

// TestRepairHealthyStillCopies is the false-positive guard: bounding the joins
// must not stop a normal pass from doing its work.
func TestRepairHealthyStillCopies(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	m := NewMirror([]Store{a, b}, WithRepoPrefix("repos/x"))
	defer m.Close()

	const n = 100
	for i := 0; i < n; i++ {
		key := "repos/x/texts/h" + itoaTemp(i)
		if err := a.Put(ctx, key, []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	m.RepairNow()
	for i := 0; i < n; i++ {
		key := "repos/x/texts/h" + itoaTemp(i)
		if _, ok, _ := b.Head(ctx, key); !ok {
			t.Fatalf("repair did not copy %s", key)
		}
	}
}

func itoaTemp(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [12]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
