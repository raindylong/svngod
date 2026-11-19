package store

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"
)

// A replica that ignores cancellation must not be able to hold a commit open
// forever.
//
// quorumMirror gives each standby copy a context with quorumWriteTimeout and
// then blocks on wg.Wait(). The timeout only helps if fn actually returns when
// that context is cancelled. A backend that ignores ctx - a hung TCP read in a
// client library that does not poll ctx, a blocked syscall, an SDK bug - makes
// WithTimeout a no-op, and wg.Wait() then waits forever. In the commit path that
// means the caller never gets a response and the commit lease is never released:
// every other commit on the repository queues behind it.
//
// These tests use a Store that deliberately ignores ctx, which is the worst case
// a defensive layer must survive.

// deafStore never returns from Put until released, ignoring ctx entirely.
type deafStore struct {
	Store
	release chan struct{}
	entered chan struct{}
	once    sync.Once
}

func newDeafStore(t *testing.T) *deafStore {
	t.Helper()
	return &deafStore{
		Store:   NewMemoryStore(),
		release: make(chan struct{}),
		entered: make(chan struct{}, 8),
	}
}

func (d *deafStore) Put(ctx context.Context, key string, data []byte) error {
	select {
	case d.entered <- struct{}{}:
	default:
	}
	<-d.release // deliberately ignores ctx
	return nil
}

func (d *deafStore) Get(ctx context.Context, key string, rng *Range) ([]byte, error) {
	<-d.release
	return nil, ErrNotFound
}

func (d *deafStore) Head(ctx context.Context, key string) (string, bool, error) {
	<-d.release
	return "", false, nil
}

func (d *deafStore) GetTo(ctx context.Context, key string, w io.Writer) (int64, error) {
	<-d.release
	return 0, nil
}

func (d *deafStore) unblock() { close(d.release) }

// TestQuorumWriteDoesNotHangOnDeafReplica fails on the current code: with
// writeQuorum=2 the second copy blocks in Put, the per-copy context expires, but
// the call never returns because the store ignores ctx and wg.Wait() has no
// bound of its own.
func TestQuorumWriteDoesNotHangOnDeafReplica(t *testing.T) {
	primary := NewMemoryStore()
	deaf := newDeafStore(t)
	defer deaf.unblock()

	m := NewMirror([]Store{primary, deaf}, WithWriteQuorum(2))

	done := make(chan error, 1)
	go func() {
		done <- m.Put(context.Background(), "repos/demo/current", []byte("x"))
	}()

	// Wait for the standby copy to be attempted, then require the whole call to
	// give up on its own.
	select {
	case <-deaf.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("standby Put was never attempted")
	}

	select {
	case <-done:
		// Returning is the point; the error value is checked below.
	case <-time.After(quorumWriteTimeout + 20*time.Second):
		t.Fatal("Put never returned although the replica ignored cancellation; " +
			"the commit path would hold the lease forever")
	}
}

// TestQuorumWriteReportsUnmetWhenReplicaDeaf pins the outcome: a replica that
// never acknowledges means the quorum was not reached, so the caller must see
// ErrQuorumUnmet rather than a silent success.
func TestQuorumWriteReportsUnmetWhenReplicaDeaf(t *testing.T) {
	primary := NewMemoryStore()
	deaf := newDeafStore(t)
	defer deaf.unblock()

	m := NewMirror([]Store{primary, deaf}, WithWriteQuorum(2))

	done := make(chan error, 1)
	go func() {
		done <- m.Put(context.Background(), "repos/demo/current", []byte("x"))
	}()
	select {
	case <-deaf.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("standby Put was never attempted")
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Put reported success without a quorum acknowledgement")
		}
	case <-time.After(quorumWriteTimeout + 20*time.Second):
		t.Fatal("Put never returned")
	}
}

// TestQuorumWriteHealthyStillSucceeds is the false-positive guard: the bound
// must not reject a normal two-replica write.
func TestQuorumWriteHealthyStillSucceeds(t *testing.T) {
	a, b := NewMemoryStore(), NewMemoryStore()
	m := NewMirror([]Store{a, b}, WithWriteQuorum(2))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.Put(ctx, "repos/demo/current", []byte("ok")); err != nil {
		t.Fatalf("healthy quorum write failed: %v", err)
	}
}
