package fs

import (
	"context"
	"testing"

	"svngod/internal/fs/store"
)

// TestMirrorRepairChoosesNewestCurrent checks the restart-after-failover case:
// the ex-primary is stale (older HEAD, missing the standby's objects), and a
// new MirrorStore must not let it overwrite the newer value. Repair is
// symmetric, so objects that only the other mirror has are copied back too.
func TestMirrorRepairChoosesNewestCurrent(t *testing.T) {
	ctx := context.Background()
	a, b := store.NewMemoryStore(), store.NewMemoryStore()
	putCurrent := func(s store.Store, head uint64) {
		if err := s.Put(ctx, "repos/x/current", (&Current{Head: head, Created: 100}).Marshal()); err != nil {
			t.Fatal(err)
		}
	}
	putCurrent(a, 1)
	putCurrent(b, 1)
	m1 := store.NewMirror([]store.Store{a, b}, store.WithMutableOrder(CompareMutable))
	m1.Close()

	// The standby advances while the old primary is down.
	if err := b.Put(ctx, "repos/x/revs-loose/0000000000/000000000002", []byte("rev2")); err != nil {
		t.Fatal(err)
	}
	putCurrent(b, 2)

	// Process restart: both backends are reachable again, but a is stale.
	m2 := store.NewMirror([]store.Store{a, b}, store.WithMutableOrder(CompareMutable))
	defer m2.Close()

	if _, ok, _ := a.Head(ctx, "repos/x/revs-loose/0000000000/000000000002"); !ok {
		t.Fatal("repair did not copy the newer revision object to the stale mirror")
	}
	curA, err := a.Get(ctx, "repos/x/current", nil)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := UnmarshalCurrent(curA)
	if err != nil || ca.Head != 2 {
		t.Fatalf("stale mirror current=%+v err=%v, want head 2", ca, err)
	}

	// An object that only the other mirror has must be copied the other way.
	if err := a.Put(ctx, "repos/x/texts/only-a", []byte("t")); err != nil {
		t.Fatal(err)
	}
	m2.RepairNow()
	if _, ok, _ := b.Head(ctx, "repos/x/texts/only-a"); !ok {
		t.Fatal("repair did not copy an object from the non-primary mirror")
	}
}
