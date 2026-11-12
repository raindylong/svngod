package store

import (
	"context"
	"testing"
)

// routerFixture: meta + two shard owners, shards 0..3 and 4..∞.
func routerFixture(t *testing.T) (*ShardRouter, *MemoryStore, *MemoryStore, *MemoryStore) {
	t.Helper()
	meta := NewMemoryStore()
	shard0 := NewMemoryStore()
	shard1 := NewMemoryStore()
	r, err := NewShardRouter(ShardRouterConfig{
		Prefix:    "repos/sr",
		Meta:      meta,
		ShardSize: 4096,
		Ranges: []ShardRange{
			{From: 0, To: 3, Store: shard0},
			{From: 4, To: ^uint64(0), Store: shard1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return r, meta, shard0, shard1
}

// TestShardRouterRoutesByFamily: revision-keyed keys land on the shard owner;
// control and content keys land on meta.
func TestShardRouterRoutesByFamily(t *testing.T) {
	r, meta, shard0, shard1 := routerFixture(t)
	ctx := context.Background()
	k := NewKeys("repos/sr")
	cases := []struct {
		key   string
		where *MemoryStore
	}{
		{k.LooseRev(0, 1), shard0},
		{k.LooseRev(2, 9000), shard0},
		{k.LooseRev(4, 20000), shard1},
		{k.PackedRev(3), shard0},
		{k.PackedRev(4), shard1},
		{k.PackedRevProps(1), shard0},
		{k.LooseRevProps(4, 18000), shard1},
		{k.Journal(1), shard0},          // rev 1 -> shard 0
		{k.Journal(4096), shard0},       // rev 4096 -> shard 1, owned by range 0..3
		{k.Journal(16384), shard1},      // rev 16384 -> shard 4, owned by range 4..
		{k.SegmentIndex(0), shard0},     // "segs/0000000000.idx"
		{k.Current(), meta},             // control
		{k.CommitLock(), meta},          // control
		{k.Format(), meta},              // control
		{k.Text("abc123"), meta},        // content
		{k.Chunk("deadbeef"), meta},     // content
		{k.TxnPrefix("t1") + "x", meta}, // txn staging
	}
	for _, c := range cases {
		if err := r.Put(ctx, c.key, []byte("v")); err != nil {
			t.Fatalf("put %s: %v", c.key, err)
		}
		for _, st := range []*MemoryStore{meta, shard0, shard1} {
			_, err := st.Get(ctx, c.key, nil)
			has := err == nil
			if has != (st == c.where) {
				t.Fatalf("key %s: present on wrong backend", c.key)
			}
		}
	}
}

// TestShardRouterValidatesRanges: gaps and overlaps are rejected.
func TestShardRouterValidatesRanges(t *testing.T) {
	if _, err := NewShardRouter(ShardRouterConfig{Meta: NewMemoryStore(), ShardSize: 64,
		Ranges: []ShardRange{{From: 1, To: 4, Store: NewMemoryStore()}}}); err == nil {
		t.Fatal("ranges must start at 0")
	}
	if _, err := NewShardRouter(ShardRouterConfig{Meta: NewMemoryStore(), ShardSize: 64,
		Ranges: []ShardRange{
			{From: 0, To: 2, Store: NewMemoryStore()},
			{From: 4, To: 9, Store: NewMemoryStore()},
		}}); err == nil {
		t.Fatal("gap must be rejected")
	}
	if _, err := NewShardRouter(ShardRouterConfig{Meta: NewMemoryStore(), ShardSize: 0,
		Ranges: []ShardRange{{From: 0, To: 2, Store: NewMemoryStore()}}}); err == nil {
		t.Fatal("zero shard size must be rejected")
	}
}

// TestShardRouterListPrefixMerges: a family prefix lists across backends.
func TestShardRouterListPrefixMerges(t *testing.T) {
	r, _, _, _ := routerFixture(t)
	ctx := context.Background()
	k := NewKeys("repos/sr")
	if err := r.Put(ctx, k.Journal(1), []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := r.Put(ctx, k.Journal(4096), []byte("b")); err != nil {
		t.Fatal(err)
	}
	objs, _, err := r.ListPrefix(ctx, k.JournalPrefix(), "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 2 {
		t.Fatalf("listed %d objects, want 2", len(objs))
	}
}

// TestShardRouterRepoEndToEnd lives in the fs package (needs CreateRepo);
// here we only assert Store-level CAS/Exists/Delete routing.
func TestShardRouterOpsRoute(t *testing.T) {
	r, meta, shard0, _ := routerFixture(t)
	ctx := context.Background()
	k := NewKeys("repos/sr")
	// CAS on current goes to meta (seed first, then advance).
	if err := r.CreateIfAbsent(ctx, k.Current(), []byte("h1")); err != nil {
		t.Fatal(err)
	}
	etag, _, herr := r.Head(ctx, k.Current())
	if herr != nil {
		t.Fatal(herr)
	}
	if _, err := r.CAS(ctx, k.Current(), etag, []byte("h2")); err != nil {
		t.Fatal(err)
	}
	if _, err := meta.Get(ctx, k.Current(), nil); err != nil {
		t.Fatalf("current not on meta: %v", err)
	}
	// CreateIfAbsent on a loose rev goes to its shard owner.
	if err := r.CreateIfAbsent(ctx, k.LooseRev(1, 5), []byte("r")); err != nil {
		t.Fatal(err)
	}
	if _, err := shard0.Get(ctx, k.LooseRev(1, 5), nil); err != nil {
		t.Fatalf("loose rev not on shard0: %v", err)
	}
	// Exists routes.
	ok, err := r.Exists(ctx, k.LooseRev(1, 5))
	if err != nil || !ok {
		t.Fatalf("exists: %v %v", ok, err)
	}
	// Delete batches per backend.
	if err := r.Delete(ctx, []string{k.LooseRev(1, 5), k.Current()}); err != nil {
		t.Fatal(err)
	}
	if ok, _ := r.Exists(ctx, k.LooseRev(1, 5)); ok {
		t.Fatal("loose rev not deleted")
	}
	if ok, _ := r.Exists(ctx, k.Current()); ok {
		t.Fatal("current not deleted")
	}
}
