package fs

import (
	"context"
	"strings"
	"testing"

	"svngod/internal/cdc"
	"svngod/internal/fs/store"
)

// TestCollectTextsSkipsDirsWhenTableUnreadable covers the third reference scan
// that feeds a deletion: paged directories. A directory's children live in dir
// objects named by their content hash, so an unreadable table hides every
// bucket that directory references. Deleting those buckets would make the
// directory undecodable - the children are the data, the table is derived.
//
// Unlike the chunk scan, which turns the same condition into a hard error, the
// dir scan sets dirScanOK=false and skips the dir pass while still collecting
// texts and chunks (whose reference sets are unaffected). The test pins that
// behaviour so it cannot silently become "collect dirs from a partial set".
func TestCollectTextsSkipsDirsWhenTableUnreadable(t *testing.T) {
	mem := store.NewMemoryStore()
	fs := &readFaultStore{Store: mem}
	r, err := CreateRepoFull(fs, "dirgc", 0, 0, cdc.DefaultParams(), true)
	if err != nil {
		t.Fatal(err)
	}
	rev := commitBigDir(t, r, 1500) // > dirBucketThreshold, so the dir is paged
	nr, err := r.Resolve(rev, "/big")
	if err != nil {
		t.Fatal(err)
	}
	if nr.TextRep != TextRepExternal || nr.TextRef == "" {
		t.Fatalf("fixture did not page the directory: rep=%d ref=%q", nr.TextRep, nr.TextRef)
	}
	dirPrefix := r.keys.ListingPrefix("dirs")
	before := countPrefix(t, mem, dirPrefix)
	if before == 0 {
		t.Fatal("fixture produced no dir objects")
	}

	// The directory's table object cannot be read.
	fs.setFault(func(k string) bool { return strings.Contains(k, "/dirs/") })
	deleted, derr := r.CollectTexts(context.Background())
	fs.setFault(nil)
	if derr != nil {
		t.Fatalf("CollectTexts failed instead of skipping the dir pass: %v", derr)
	}
	if after := countPrefix(t, mem, dirPrefix); after != before {
		t.Fatalf("dir objects deleted from a partial reference set: %d -> %d (deleted=%d)", before, after, deleted)
	}

	// With the fault cleared the same sweep may collect: prove the fixture
	// really has collectable dir objects and the skip was caused by the fault.
	fs.setFault(nil)
	if _, err := r.CollectTexts(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Nothing should be collected now either - every dir object is referenced -
	// which is the healthy baseline for this fixture.
	if after := countPrefix(t, mem, dirPrefix); after != before {
		t.Fatalf("healthy sweep removed live dir objects: %d -> %d", before, after)
	}

	// An orphan dir object is collected when the scan is healthy.
	orphan := r.keys.Dir("deadbeefdeadbeefdeadbeefdeadbeef")
	if err := mem.Put(context.Background(), orphan, []byte("orphan dir")); err != nil {
		t.Fatal(err)
	}
	if _, err := r.CollectTexts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ok, _ := mem.Exists(context.Background(), orphan); ok {
		t.Fatal("orphan dir object not collected by a healthy sweep")
	}
}
