package fs

import (
	"context"
	"strings"
	"testing"
	"time"

	"svngod/internal/cdc"
	"svngod/internal/fs/store"
)

// waitLooseStable waits until the loose-object count stops changing.
//
// PackShard reclaims a packed shard's loose copies in a background goroutine,
// so a test that inspects them immediately after packing races that cleanup.
// Waiting for the count to settle makes the fixture deterministic.
func waitLooseStable(t *testing.T, st store.Store, prefix string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	last := -1
	stable := 0
	for time.Now().Before(deadline) {
		n := countPrefix(t, st, prefix)
		if n == last {
			if stable++; stable >= 3 {
				return n
			}
		} else {
			stable = 0
			last = n
		}
		time.Sleep(10 * time.Millisecond)
	}
	return countPrefix(t, st, prefix)
}

// TestCollectLooseSkipsWhenPackedMarkerUnreadable covers the loose sweep's only
// gate. isPacked decides whether a shard's loose rev/revprops objects are
// redundant; if that probe fails and the code assumed "unpacked", the sweep
// would skip the shard (safe) - but if it were ever inverted to "assume
// packed", every loose object in the shard would be deleted while it was still
// the only copy. The sweep must treat an unreadable marker as "cannot decide".
func TestCollectLooseSkipsWhenPackedMarkerUnreadable(t *testing.T) {
	mem := store.NewMemoryStore()
	fs := &readFaultStore{Store: mem}
	// shardSize 4 so revs 1..4 fill shard 0 and 5..8 fill shard 1.
	r, err := CreateRepo(fs, "loose1", 4, 64)
	if err != nil {
		t.Fatal(err)
	}
	commitN(t, r, 4, 2) // 8 revisions across two shards
	if err := r.PackShard(0); err != nil {
		t.Fatal(err)
	}
	// PackShard reclaims the packed shard's loose copies in the background;
	// let that finish so the count below is not moving underneath us.
	loosePrefix := r.keys.ListingPrefix("revs-loose")
	before := waitLooseStable(t, mem, loosePrefix)
	if before == 0 {
		t.Fatal("fixture left no loose objects to protect")
	}
	// Everything still loose belongs to shard 1 (shard 0 was packed and its
	// copies reclaimed), so make the unreadable marker be shard 1's.
	r.cache.Clear()
	fs.setFault(func(k string) bool { return strings.HasSuffix(k, ".done") })
	deleted, cerr := r.CollectLoose(context.Background())
	fs.setFault(nil)
	if cerr != nil {
		t.Fatalf("CollectLoose must skip a shard it cannot classify, not fail: %v", cerr)
	}
	if after := countPrefix(t, mem, loosePrefix); after != before {
		t.Fatalf("loose objects deleted while the packed marker was unreadable: %d -> %d (deleted=%d)",
			before, after, deleted)
	}
	if deleted != 0 {
		t.Fatalf("sweep reported %d deletions with every marker unreadable", deleted)
	}
}

// TestCollectLooseCollectsAfterMarkerRecovers proves the guard is a skip, not a
// permanent block: once the marker is readable a packed shard's leftover loose
// copies are collected.
func TestCollectLooseCollectsAfterMarkerRecovers(t *testing.T) {
	mem := store.NewMemoryStore()
	r, err := CreateRepo(mem, "loose2", 4, 64)
	if err != nil {
		t.Fatal(err)
	}
	commitN(t, r, 4, 2)
	// Write loose copies for a packed shard by hand: PackShard's background
	// cleanup would otherwise remove them before the assertion.
	if err := r.PackShard(0); err != nil {
		t.Fatal(err)
	}
	loosePrefix := r.keys.ListingPrefix("revs-loose")
	waitLooseStable(t, mem, loosePrefix)
	stale := r.keys.LooseRev(0, 1)
	if err := mem.Put(context.Background(), stale, []byte("stale")); err != nil {
		t.Fatal(err)
	}
	n, err := r.CollectLoose(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("healthy sweep collected nothing from a packed shard")
	}
	if ok, _ := mem.Exists(context.Background(), stale); ok {
		t.Fatal("stale loose object survived a healthy sweep")
	}
}

// TestCompactSegmentsRefusesOnIndexReadFailure covers compaction, which deletes
// source segments once the merged object and index are in place. Merging from an
// index it could not read would risk dropping revisions: the merged object would
// not contain them and the old segments would then be garbage.
func TestCompactSegmentsRefusesOnIndexReadFailure(t *testing.T) {
	mem := store.NewMemoryStore()
	fs := &readFaultStore{Store: mem}
	r, err := CreateRepoSegments(fs, "comp1", 64, 64, cdc.DefaultParams(), false, 4)
	if err != nil {
		t.Fatal(err)
	}
	commitN(t, r, 4, 1) // four segments of one revision in shard 0
	refs, err := r.loadSegmentIndex(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) <= 1 {
		t.Fatalf("fixture produced %d segments; compaction needs more than one", len(refs))
	}
	segPrefix := r.keys.ListingPrefix("segs")
	before := countPrefix(t, mem, segPrefix)

	fs.setFault(func(k string) bool { return strings.HasSuffix(k, ".idx") })
	merged, cerr := r.CompactSegments(context.Background())
	if cerr == nil {
		t.Fatal("compaction ran with an unreadable shard index")
	}
	if merged != 0 {
		t.Fatalf("compaction reported %d merged shards despite the read failure", merged)
	}
	fs.setFault(nil)
	if after := countPrefix(t, mem, segPrefix); after != before {
		t.Fatalf("segments disappeared during a failed compaction: %d -> %d", before, after)
	}
}
