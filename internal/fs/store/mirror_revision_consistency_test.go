package store

import (
	"context"
	"errors"
	"testing"
)

// putFailStore fails Put while leaving reads and CAS intact, simulating a
// replica that accepts connections but refuses writes.
type putFailStore struct {
	Store
	fail bool
}

var errPutDown = errors.New("replica read-only")

func (s *putFailStore) Put(ctx context.Context, key string, data []byte) error {
	if s.fail {
		return errPutDown
	}
	return s.Store.Put(ctx, key, data)
}

// A revision object must never become visible on a standby when the arbiter
// refused it: a retry could then publish different bytes under the same
// revision number on each replica.
func TestRevisionPutRequiresArbiter(t *testing.T) {
	ctx := context.Background()
	a := &putFailStore{Store: NewMemoryStore()}
	b := NewMemoryStore()
	m := NewMirror([]Store{a, b})
	defer m.Close()
	a.fail = true

	key := "repos/x/revs-loose/0000000000/0000000001"
	if err := m.Put(ctx, key, []byte("rb")); !errors.Is(err, ErrQuorumUnmet) {
		t.Fatalf("Put revision with a down arbiter = %v, want ErrQuorumUnmet", err)
	}
	if _, err := b.Get(ctx, key, nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("revision leaked to the standby: %v", err)
	}
}

// Even when this process marked the arbiter down, a revision write still
// targets it first; otherwise the standby could serve a revision the arbiter
// (the HEAD authority) never saw.
func TestRevisionPutAlwaysTargetsArbiter(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	m := NewMirror([]Store{a, b})
	defer m.Close()
	m.mu.Lock()
	m.healthy[0] = false
	m.mu.Unlock()

	key := "repos/x/revs-loose/0000000000/0000000002"
	if err := m.Put(ctx, key, []byte("rb2")); err != nil {
		t.Fatal(err)
	}
	got, err := a.Get(ctx, key, nil)
	if err != nil || string(got) != "rb2" {
		t.Fatalf("arbiter copy = %q, %v", got, err)
	}
	if got, err := b.Get(ctx, key, nil); err != nil || string(got) != "rb2" {
		t.Fatalf("standby copy = %q, %v", got, err)
	}
}

// A standby that holds an orphan block from a failed attempt must be healed
// from the arbiter, not left divergent.
func TestCopyKeyToOverwritesDivergentRevision(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	m := NewMirror([]Store{a, b})
	defer m.Close()

	key := "repos/x/revs-loose/0000000000/0000000003"
	if err := a.Put(ctx, key, []byte("arbiter-bytes")); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(ctx, key, []byte("orphan-bytes")); err != nil {
		t.Fatal(err)
	}
	if err := m.copyKeyTo(1, key); err != nil {
		t.Fatal(err)
	}
	if got, _ := b.Get(ctx, key, nil); string(got) != "arbiter-bytes" {
		t.Fatalf("standby not healed: %q", got)
	}
	if got, _ := a.Get(ctx, key, nil); string(got) != "arbiter-bytes" {
		t.Fatalf("arbiter changed: %q", got)
	}
}

// Repair must never copy a standby orphan onto the arbiter.
func TestCopyKeyToNeverOverwritesArbiter(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	m := NewMirror([]Store{a, b})
	defer m.Close()

	key := "repos/x/revs-loose/0000000000/0000000004"
	if err := a.Put(ctx, key, []byte("arbiter-bytes")); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(ctx, key, []byte("orphan-bytes")); err != nil {
		t.Fatal(err)
	}
	if err := m.copyKeyTo(0, key); err != nil {
		t.Fatal(err)
	}
	if got, _ := a.Get(ctx, key, nil); string(got) != "arbiter-bytes" {
		t.Fatalf("arbiter overwritten by a standalone orphan: %q", got)
	}
}

// A CAS based on a stale standby value must be refused by the arbiter, and
// the stale writer must not overwrite the standby either.
func TestControlCASIsCheckedOnArbiter(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	m := NewMirror([]Store{a, b})
	defer m.Close()

	key := "repos/x/current"
	head0 := []byte("head-0")
	head1 := []byte("head-1")
	if err := a.Put(ctx, key, head1); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(ctx, key, head0); err != nil {
		t.Fatal(err)
	}
	if _, err := m.CAS(ctx, key, ContentETag(head0), []byte("head-2")); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("CAS with a stale standby value = %v, want ErrPreconditionFailed", err)
	}
	if got, _ := b.Get(ctx, key, nil); string(got) != "head-0" {
		t.Fatalf("failed CAS updated the standby: %q", got)
	}
	if got, _ := a.Get(ctx, key, nil); string(got) != "head-1" {
		t.Fatalf("arbiter changed by a failed CAS: %q", got)
	}
	// The correct base succeeds and is mirrored to the standby.
	if _, err := m.CAS(ctx, key, ContentETag(head1), []byte("head-2")); err != nil {
		t.Fatal(err)
	}
	if got, _ := a.Get(ctx, key, nil); string(got) != "head-2" {
		t.Fatalf("arbiter lost the successful CAS: %q", got)
	}
	if got, _ := b.Get(ctx, key, nil); string(got) != "head-2" {
		t.Fatalf("standby not mirrored: %q", got)
	}
}

// Repair must overwrite a standby's orphan revision block with the arbiter's
// bytes, including when their sizes match (node-id reordering keeps the total
// length but changes content).
func TestRepairReconcilesDivergentRevisionTail(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	m := NewMirror([]Store{a, b})
	defer m.Close()

	key := "repos/x/revs-loose/0000000000/0000000005"
	if err := a.Put(ctx, key, []byte("arbiter-A")); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(ctx, key, []byte("orphan-AB")); err != nil {
		t.Fatal(err)
	}
	// Equal lengths: only the content differs, which a size check cannot see.
	sets := []map[string]int64{{key: 9}, {key: 9}}
	m.reconcileRevisions(map[string]bool{key: true}, sets, []bool{true, true})
	if got, _ := b.Get(ctx, key, nil); string(got) != "arbiter-A" {
		t.Fatalf("standby revision not reconciled: %q", got)
	}
	if got, _ := a.Get(ctx, key, nil); string(got) != "arbiter-A" {
		t.Fatalf("arbiter changed during reconcile: %q", got)
	}
}
