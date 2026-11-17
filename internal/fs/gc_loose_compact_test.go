package fs

import (
	"context"
	"strings"
	"testing"

	"svngod/internal/cdc"
	"svngod/internal/fs/store"
)

// TestCollectLooseSkipsWhenPackedMarkerUnreadable covers the loose sweep's only
// gate. isPacked decides whether a shard's loose rev/revprops objects are
// redundant; if that probe fails and the code assumed "unpacked", the shard
// would be skipped (safe) - but if it were ever inverted to "assume packed",
// every loose object in the shard would be deleted while it was still the only
// copy. The sweep must treat an unreadable marker as "cannot decide" and skip.
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
	// Loose revision blocks live under revs-loose/, not revs/ (which also holds
	// the packed .done markers and the pack object).
	loosePrefix := r.keys.ListingPrefix("revs-loose")
	before := countPrefix(t, mem, loosePrefix)
	if before == 0 {
		t.Fatal("fixture left no loose objects to protect")
	}

	// The packed-done marker for shard 0 cannot be read.
	fs.setFault(func(k string) bool { return strings.HasSuffix(k, ".done") })
	if _, err := r.CollectLoose(context.Background()); err != nil {
		t.Fatalf("CollectLoose must skip a shard it cannot classify, not fail: %v", err)
	}
	fs.setFault(nil)
	if after := countPrefix(t, mem, loosePrefix); after != before {
		t.Fatalf("loose objects deleted while the packed marker was unreadable: %d -> %d", before, after)
	}

	// With the fault cleared the same sweep may collect, proving the skip was
	// caused by the unreadable marker and not by a broken fixture.
	if _, err := r.CollectLoose(context.Background()); err != nil {
		t.Fatal(err)
	}
	if after := countPrefix(t, mem, loosePrefix); after >= before {
		t.Fatalf("healthy sweep removed nothing (%d -> %d): fixture does not exercise collection", before, after)
	}
}

// TestCompactSegmentsRefusesOnIndexReadFailure covers compaction, which deletes
// source segments once the merged object and index are in place. Merging from an
// index it could not read (or from a partially read one) would risk dropping
// revisions: the merged object would not contain them and the old segments would
// then be garbage.
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
