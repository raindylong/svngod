package fs

import (
	"context"
	"testing"
	"time"

	"svngod/internal/fs/store"
)

// A standby with a divergent HEAD must not steer commits: the arbiter holds
// the lease and is the authority for the current pointer. Before this check a
// retry read the standby's older head and republished the same revision
// number, leaving the replicas with different revision blocks.
func TestHeadFreshPrefersArbiter(t *testing.T) {
	ctx := context.Background()
	a, b := store.NewMemoryStore(), store.NewMemoryStore()
	ms := store.NewMirror([]store.Store{a, b})
	defer ms.Close()
	r, err := CreateRepo(ms, "ha", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	if h, err := r.HeadFresh(); err != nil || h != 0 {
		t.Fatalf("initial HeadFresh = %d, %v", h, err)
	}

	divergent := &Current{Head: 7, Created: time.Now().Unix()}
	if err := b.Put(ctx, r.Keys().Current(), divergent.Marshal()); err != nil {
		t.Fatal(err)
	}
	if h, err := r.HeadFresh(); err != nil || h != 0 {
		t.Fatalf("HeadFresh = %d, %v; want the arbiter's head 0", h, err)
	}

	if _, err := r.CommitTxn(0, []Change{{
		Path: "/a.txt", Action: ActionAdd, Kind: KindFile, Data: []byte("x"),
	}}, nil); err != nil {
		t.Fatal(err)
	}
	if h, err := r.HeadFresh(); err != nil || h != 1 {
		t.Fatalf("HeadFresh after commit = %d, %v", h, err)
	}
	raw, err := b.Get(ctx, r.Keys().Current(), nil)
	if err != nil {
		t.Fatal(err)
	}
	cur, err := UnmarshalCurrent(raw)
	if err != nil || cur.Head != 1 {
		t.Fatalf("standby current = %+v, %v; want head 1", cur, err)
	}
}
