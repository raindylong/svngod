package fs

import (
	"testing"

	"svngod/internal/fs/store"
)

// TestRepoOverShardRouter: a repository whose revision history is physically
// split across two backing stores by shard range, with control/content on a
// meta store. Commits cross the shard boundary (shardSz=4), the WAL journal
// records land on the shard owners with the WORM chain intact, and verify
// passes over the whole history.
func TestRepoOverShardRouter(t *testing.T) {
	const shardSz = 4 // tiny so 10 revisions cross two shard ranges
	meta := store.NewMirror([]store.Store{store.NewMemoryStore(), store.NewMemoryStore()},
		store.WithWriteQuorum(2), store.WithRepoPrefix("repos/split"))
	shard0 := store.NewMemoryStore()
	shard1 := store.NewMemoryStore()
	sr, err := store.NewShardRouter(store.ShardRouterConfig{
		Prefix:    "repos/split",
		Meta:      meta,
		ShardSize: shardSz,
		Ranges: []store.ShardRange{
			{From: 0, To: 1, Store: shard0}, // revs 0..7
			{From: 2, To: ^uint64(0), Store: shard1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	r, err := CreateRepo(sr, "repos/split", shardSz, 64)
	if err != nil {
		t.Fatal(err)
	}
	const n = 10
	for i := 0; i < n; i++ {
		if _, err := r.CommitTxn(uint64(i), []Change{{
			Path: "/f.txt", Action: ActionModify, Kind: KindFile, Data: []byte("v"),
		}}, nil); err != nil {
			t.Fatal(err)
		}
	}
	// The data plane is physically split: shard 0/1 revisions (packed) are
	// on shard0's backend, shard 2+ (loose) on shard1's, current on meta.
	objs0, _, err := shard0.ListPrefix(t.Context(), "repos/split/revs/", "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	objs1, _, err := shard1.ListPrefix(t.Context(), "repos/split/revs-loose/", "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(objs0) == 0 || len(objs1) == 0 {
		t.Fatalf("history not split: shard0 packed=%d shard1 loose=%d", len(objs0), len(objs1))
	}
	// Journal (WORM chain) records are also on the shard owners.
	j0, _, err := shard0.ListPrefix(t.Context(), "repos/split/journal/", "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	j1, _, err := shard1.ListPrefix(t.Context(), "repos/split/journal/", "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(j0)+len(j1) < n {
		t.Fatalf("journal records lost: %d+%d < %d", len(j0), len(j1), n)
	}
	// Control objects on meta.
	if _, err := meta.Get(t.Context(), "repos/split/current", nil); err != nil {
		t.Fatalf("current not on meta: %v", err)
	}
	// Whole-history invariants: chain, verify, reads.
	if err := r.VerifyJournalChain(1, n); err != nil {
		t.Fatalf("journal chain across shards: %v", err)
	}
	issues, err := r.VerifyRange(1, n, true)
	if err != nil || len(issues) > 0 {
		t.Fatalf("verify: err=%v issues=%v", err, issues)
	}
	head, _ := r.Head()
	nr, err := r.Resolve(head, "/f.txt")
	if err != nil || nr == nil {
		t.Fatalf("resolve at head: %v", err)
	}
	// Reopen through a fresh router wiring (simulates a new process): the
	// physical split is durable layout, not in-process state.
	sr2, err := store.NewShardRouter(store.ShardRouterConfig{
		Prefix:    "repos/split",
		Meta:      meta,
		ShardSize: shardSz,
		Ranges: []store.ShardRange{
			{From: 0, To: 1, Store: shard0},
			{From: 2, To: ^uint64(0), Store: shard1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	r2, err := OpenRepo(sr2, "repos/split")
	if err != nil {
		t.Fatal(err)
	}
	if h, _ := r2.Head(); h != n {
		t.Fatalf("reopened head=%d, want %d", h, n)
	}
	if _, err := r2.CommitTxn(n, []Change{{
		Path: "/g.txt", Action: ActionAdd, Kind: KindFile, Data: []byte("x"),
	}}, nil); err != nil {
		t.Fatalf("commit after reopen: %v", err)
	}
}
