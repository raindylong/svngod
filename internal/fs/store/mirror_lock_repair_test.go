package store

import (
	"context"
	"testing"
)

// TestRepairDropsReleasedLeaseOnStandby: in a two-node pair the lease is
// written and deleted on the arbiter only, so a standby can keep a copy from
// before an outage/release. A repair pass must delete that leftover instead of
// promoting it back onto the arbiter (which made live commits fail with
// "commit lease lost").
func TestRepairDropsReleasedLeaseOnStandby(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	m := NewMirror([]Store{a, b}, WithRepoPrefix("repos/x"))
	defer m.Close()

	key := "repos/x/locks/commit"
	if err := b.Put(ctx, key, []byte("stale 123")); err != nil {
		t.Fatal(err)
	}
	sets := []map[string]int64{{}, {key: 10}}
	reachable := []bool{true, true}
	m.reconcileMutable(ctx, key, sets, reachable)

	if _, ok, _ := a.Head(ctx, key); ok {
		t.Fatal("stale lease promoted onto the arbiter")
	}
	if _, ok, _ := b.Head(ctx, key); ok {
		t.Fatal("stale lease left on the standby")
	}
}

// TestRepairCopiesArbiterLeaseToStandby: when the arbiter holds the live lease
// it wins over whatever stale bytes the standby kept.
func TestRepairCopiesArbiterLeaseToStandby(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	m := NewMirror([]Store{a, b}, WithRepoPrefix("repos/x"))
	defer m.Close()

	key := "repos/x/locks/commit"
	if err := a.Put(ctx, key, []byte("live 999")); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(ctx, key, []byte("stale 123")); err != nil {
		t.Fatal(err)
	}
	sets := []map[string]int64{{key: 9}, {key: 10}}
	reachable := []bool{true, true}
	m.reconcileMutable(ctx, key, sets, reachable)

	got, err := b.Get(ctx, key, nil)
	if err != nil || string(got) != "live 999" {
		t.Fatalf("standby lease = %q, %v; want the arbiter copy", got, err)
	}
}

// TestRepairLeavesLockStateAloneWithoutArbiter: with the arbiter unreachable
// no repair may guess at lock state.
func TestRepairLeavesLockStateAloneWithoutArbiter(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	m := NewMirror([]Store{a, b}, WithRepoPrefix("repos/x"))
	defer m.Close()

	key := "repos/x/locks/commit"
	if err := b.Put(ctx, key, []byte("stale 123")); err != nil {
		t.Fatal(err)
	}
	sets := []map[string]int64{nil, {key: 10}}
	reachable := []bool{false, true}
	m.reconcileMutable(ctx, key, sets, reachable)

	if got, ok, _ := b.Head(ctx, key); !ok || got == "" {
		t.Fatal("lock state changed while the arbiter was unreachable")
	}
}
// TestDeletePrimaryOnlyTouchesArbiter: loose-object cleanup must not mirror
// its deletes; a standby's leftover copies are harmless, while mirrored
// thousand-key deletes used to take a live standby out of service.
func TestDeletePrimaryOnlyTouchesArbiter(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	key := "repos/x/revs-loose/0000000000/000000000001"
	for _, s := range []Store{a, b} {
		if err := s.Put(ctx, key, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	m := NewMirror([]Store{a, b}, WithRepoPrefix("repos/x"))
	defer m.Close()
	if err := m.DeletePrimary(ctx, []string{key}); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := a.Head(ctx, key); ok {
		t.Fatal("arbiter copy not deleted")
	}
	if _, ok, _ := b.Head(ctx, key); !ok {
		t.Fatal("standby copy was deleted (delete mirrored)")
	}
}
