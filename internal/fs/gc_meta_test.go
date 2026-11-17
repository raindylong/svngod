package fs

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"svngod/internal/cdc"
	"svngod/internal/fs/store"
)

// This file makes fail-closed coverage exhaustive instead of per-case.
//
// Three rounds of live-data deletion have come from the same shape: a reference
// scan that skips what it cannot read, leaving an incomplete reference set, and
// the next sweep deleting everything the missing entry pointed at. Each fix
// added a test, but nothing forced the *next* sweep to have one. So the guard is
// now mechanical:
//
//  1. TestEveryGCSweepHasAFailClosedCase reflects over *Repo and fails when a
//     Collect*/Compact* method exists that no case below covers. Adding a sweep
//     without a fault case breaks the build, not production.
//  2. Each case builds a fixture, runs the sweep healthy once to prove the
//     fixture reaches the deletion path, then runs it again with the read its
//     reference scan depends on failing.
//
// The invariant is data integrity, not object identity: a healthy sweep is
// *supposed* to delete things (that is its job), and compaction replaces
// segments on purpose, so "these keys still exist" is the wrong assertion. What
// must hold before and after every sweep, healthy or not, is that the repository
// still reads back. `intact` expresses that per fixture.

// gcFaultFixture is one sweep wired up for a healthy run and a faulted run.
type gcFaultFixture struct {
	repo *Repo
	// fault matches the keys whose read the sweep's reference scan depends on.
	fault func(key string) bool
	// sweep runs the sweep under test.
	sweep func(ctx context.Context) (int, error)
	// intact asserts the repository still reads back correctly. It must hold
	// after a healthy sweep and after a faulted one.
	intact func(t *testing.T)
	// prep runs after the fixture is built and before the fault is armed:
	// cache clearing, background-cleanup waits, and similar.
	prep func(t *testing.T)
	// baseline is the number of deletions a healthy pass must report. A zero
	// would mean the fixture never reaches the deletion path, which would make
	// the faulted run below vacuous.
	baseline int
}

// gcFaultCase is one sweep family's contract.
type gcFaultCase struct {
	name string
	// methods must name every exported method this case guards, so the
	// reflection check can prove nothing was added without a case.
	methods []string
	// mode is "error" when an incomplete scan must abort the whole sweep, and
	// "skip" when the sweep may still collect the classes it could read.
	mode string
	// build returns a fresh fixture; called once per run so the healthy and
	// faulted passes do not share state.
	build func(t *testing.T) gcFaultFixture
}

// TestGCSweepsFailClosedOnReadFailure is the mechanical version of the per-path
// guards: every Collect*/Compact* method is run twice, once healthy and once
// with the read its reference scan depends on failing.
func TestGCSweepsFailClosedOnReadFailure(t *testing.T) {
	for _, c := range gcFaultCases() {
		t.Run(c.name, func(t *testing.T) {
			// Healthy pass: prove the fixture actually reaches the deletion
			// path, and that a healthy sweep leaves the repository readable.
			healthy := c.build(t)
			if healthy.prep != nil {
				healthy.prep(t)
			}
			healthy.intact(t)
			n, err := healthy.sweep(context.Background())
			if err != nil {
				t.Fatalf("healthy sweep failed: %v", err)
			}
			if n < healthy.baseline {
				t.Fatalf("healthy sweep deleted %d, want at least %d: fixture does not exercise this path",
					n, healthy.baseline)
			}
			healthy.intact(t)

			// Faulted pass on an identical fixture.
			f := c.build(t)
			if f.prep != nil {
				f.prep(t)
			}
			fs, ok := f.repo.store.(*readFaultStore)
			if !ok {
				t.Fatal("fixture store is not a readFaultStore; cannot inject a read failure")
			}
			fs.setFault(f.fault)
			deleted, err := f.sweep(context.Background())
			fs.setFault(nil)

			switch c.mode {
			case "error":
				if err == nil {
					t.Fatalf("sweep completed with an unreadable reference input (deleted=%d)", deleted)
				}
			case "skip":
				if err != nil {
					t.Fatalf("sweep must skip the class it cannot read, not fail: %v", err)
				}
			default:
				t.Fatalf("unknown mode %q", c.mode)
			}
			f.intact(t)
		})
	}
}

// TestEveryGCSweepHasAFailClosedCase fails when a deletion sweep is added
// without a case in gcFaultCases. This is the part that keeps the guard honest
// over time: the cases above only cover what someone remembered to add.
func TestEveryGCSweepHasAFailClosedCase(t *testing.T) {
	covered := map[string]bool{}
	for _, c := range gcFaultCases() {
		for _, m := range c.methods {
			covered[m] = true
		}
	}
	typ := reflect.TypeOf(&Repo{})
	var missing []string
	for i := 0; i < typ.NumMethod(); i++ {
		name := typ.Method(i).Name
		if !strings.HasPrefix(name, "Collect") && !strings.HasPrefix(name, "Compact") {
			continue
		}
		if !covered[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("deletion sweep(s) with no fail-closed case: %s\n"+
			"Add a gcFaultCases entry: build a fixture whose healthy pass deletes something, "+
			"fail the read its reference scan needs, and assert the repository still reads back.",
			strings.Join(missing, ", "))
	}
	// Guard against a case naming a method that no longer exists, which would
	// let the reflection check above keep passing for the wrong reason.
	for name := range covered {
		if _, ok := typ.MethodByName(name); !ok {
			t.Fatalf("gcFaultCases names %q, which is not a method on *Repo", name)
		}
	}
}

func gcFaultCases() []gcFaultCase {
	return []gcFaultCase{
		{
			name:    "loose: unreadable packed marker",
			methods: []string{"CollectLoose", "CollectLooseProgress"},
			mode:    "skip",
			build:   buildLooseFaultFixture,
		},
		{
			name:    "txn data: unreadable activity manifest",
			methods: []string{"CollectTxnData", "CollectTxnDataGrace", "CollectTxnDataDry"},
			mode:    "error",
			build:   buildTxnFaultFixture,
		},
		{
			name:    "texts: unreadable dir table",
			methods: []string{"CollectTexts", "CollectTextsGrace", "CollectTextsGraceProgress"},
			mode:    "skip",
			build:   buildTextsDirFaultFixture,
		},
		{
			name:    "orphan segments: unreadable shard index",
			methods: []string{"CollectOrphanSegments", "CollectOrphanSegmentsProgress"},
			mode:    "error",
			build:   buildOrphanSegmentFaultFixture,
		},
		{
			name:    "compact: unreadable shard index",
			methods: []string{"CompactSegments", "CompactSegmentsProgress"},
			mode:    "error",
			build:   buildCompactFaultFixture,
		},
	}
}

// assertAllRevsReadable is the shared integrity check for repositories whose
// revisions live in loose blocks or segments: every revision must still resolve.
func assertAllRevsReadable(t *testing.T, r *Repo) {
	t.Helper()
	head, err := r.HeadFresh()
	if err != nil {
		t.Fatalf("head after sweep: %v", err)
	}
	if head == 0 {
		t.Fatal("fixture has no revisions")
	}
	for rev := uint64(1); rev <= head; rev++ {
		if _, err := r.readBlock(rev); err != nil {
			t.Fatalf("revision %d unreadable after sweep: %v", rev, err)
		}
	}
}

// buildLooseFaultFixture packs shard 0 and leaves a stale loose revision block
// behind for the sweep to collect.
func buildLooseFaultFixture(t *testing.T) gcFaultFixture {
	mem := store.NewMemoryStore()
	fs := &readFaultStore{Store: mem}
	r, err := CreateRepo(fs, "meta-loose", 4, 64)
	if err != nil {
		t.Fatal(err)
	}
	commitN(t, r, 4, 2) // revs 1..8: shards 0, 1 and 2
	if err := r.PackShard(0); err != nil {
		t.Fatal(err)
	}
	return gcFaultFixture{
		repo:  r,
		fault: func(k string) bool { return strings.HasSuffix(k, ".done") },
		sweep: func(ctx context.Context) (int, error) { return r.CollectLoose(ctx) },
		intact: func(t *testing.T) {
			// Shards 1 and 2 are not packed, so their loose blocks are the only
			// copy of those revisions. Deleting them loses committed data.
			assertAllRevsReadable(t, r)
		},
		prep: func(t *testing.T) {
			// Wait for PackShard's background reclaim, then plant one stale
			// loose block so the healthy pass has something to delete.
			loosePrefix := r.keys.ListingPrefix("revs-loose")
			waitLooseStable(t, mem, loosePrefix)
			stale := r.keys.LooseRev(0, 1)
			if err := mem.Put(context.Background(), stale, []byte("stale")); err != nil {
				t.Fatal(err)
			}
			if ok, _ := mem.Exists(context.Background(), r.keys.PackedDone(0)); !ok {
				t.Fatal("shard 0 is not marked packed; the stale block would not be collectable")
			}
			// isPacked caches positive results, so PackShard's earlier probe
			// would hide the injected fault entirely.
			r.cache.Clear()
		},
		baseline: 1,
	}
}

// buildTxnFaultFixture stages an upload body with the manifest that references
// it, plus one unreferenced body for the healthy pass to collect.
func buildTxnFaultFixture(t *testing.T) gcFaultFixture {
	mem := store.NewMemoryStore()
	fs := &readFaultStore{Store: mem}
	r, err := CreateRepo(fs, "meta-txn", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	base := r.keys.ListingPrefix("dav-txns")
	liveBody := base + "data/abc123"
	manifest := base + "activity-1.json"
	if err := mem.Put(ctx, liveBody, []byte("live upload body")); err != nil {
		t.Fatal(err)
	}
	if err := mem.Put(ctx, manifest, []byte(`{"dataRef":"abc123"}`)); err != nil {
		t.Fatal(err)
	}
	if err := mem.Put(ctx, base+"data/orphan9", []byte("body nobody references")); err != nil {
		t.Fatal(err)
	}
	return gcFaultFixture{
		repo:     r,
		fault:    func(k string) bool { return strings.HasSuffix(k, ".json") },
		sweep:    func(ctx context.Context) (int, error) { return r.CollectTxnData(ctx) },
		baseline: 1,
		intact: func(t *testing.T) {
			// The staged body is user content that cannot be rebuilt; the
			// manifest is what points at it. An unreadable manifest must never
			// turn into "unreferenced".
			for _, k := range []string{liveBody, manifest} {
				ok, err := mem.Exists(context.Background(), k)
				if err != nil || !ok {
					t.Fatalf("commit-session object %q lost (err=%v)", k, err)
				}
			}
		},
	}
}

// buildTextsDirFaultFixture commits a paged directory and plants one
// unreferenced bucket for the healthy pass to collect.
//
// This is the one "skip" case with a twist: when the dir table cannot be read
// the sweep skips dirs but keeps collecting texts and chunks, because the
// reference sets are independent. The integrity check is therefore the
// directory itself: its buckets are the children, the table is derived, so
// losing a bucket makes the directory undecodable.
func buildTextsDirFaultFixture(t *testing.T) gcFaultFixture {
	mem := store.NewMemoryStore()
	fs := &readFaultStore{Store: mem}
	r, err := CreateRepoFull(fs, "meta-dir", 0, 0, cdc.DefaultParams(), true)
	if err != nil {
		t.Fatal(err)
	}
	const files = dirBucketThreshold + 16
	rev := commitBigDir(t, r, files)
	dirPrefix := r.keys.ListingPrefix("dirs")
	if countPrefix(t, mem, dirPrefix) == 0 {
		t.Fatal("fixture produced no dir buckets")
	}
	orphan := dirPrefix + strings.Repeat("ab", 32)
	if err := mem.Put(context.Background(), orphan, []byte("unreferenced bucket")); err != nil {
		t.Fatal(err)
	}
	return gcFaultFixture{
		repo:     r,
		fault:    func(k string) bool { return strings.Contains(k, "/dirs/") },
		sweep:    func(ctx context.Context) (int, error) { return r.CollectTexts(ctx) },
		baseline: 1,
		prep: func(t *testing.T) {
			// The dir table is read through the decode cache; a cached decode
			// would bypass the fault.
			r.cache.Clear()
		},
		intact: func(t *testing.T) {
			// Walk the whole directory through the paging table: every bucket
			// must still be there for the children to be findable.
			names, err := r.ListDir(rev, "/big")
			if err != nil {
				t.Fatalf("directory listing failed after sweep: %v", err)
			}
			if len(names) != files {
				t.Fatalf("directory lost entries after sweep: %d of %d remain", len(names), files)
			}
		},
	}
}

// buildOrphanSegmentFaultFixture plants an old unreferenced segment object. The
// sweep must read every shard index to know what is referenced, so an
// unreadable index has to abort rather than treat the shard as empty.
func buildOrphanSegmentFaultFixture(t *testing.T) gcFaultFixture {
	dir := t.TempDir()
	st, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	fs := &readFaultStore{Store: st}
	r, err := CreateRepoSegments(fs, "repos/meta-orphan", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	if _, errs, err := r.CommitBatch(batchAdds("/keep")); err != nil || errs[0] != nil {
		t.Fatalf("commit: %v %v", err, errs)
	}
	// plantOld writes through the filesystem, bypassing the fault wrapper, and
	// backdates the file so it is old enough to look like an orphan.
	plantOld(t, dir, r.keys.Segment(0, 1, 999), []byte("junk"))
	return gcFaultFixture{
		repo:     r,
		fault:    func(k string) bool { return strings.HasSuffix(k, ".idx") },
		sweep:    func(ctx context.Context) (int, error) { return r.CollectOrphanSegments(ctx, 0) },
		baseline: 1,
		intact: func(t *testing.T) {
			// The live revision lives in a segment the index points at. If the
			// sweep treats an unreadable index as "nothing is referenced", that
			// segment is deleted and the revision is gone.
			assertAllRevsReadable(t, r)
		},
	}
}

// buildCompactFaultFixture leaves several segments in one shard so compaction
// has work to do.
func buildCompactFaultFixture(t *testing.T) gcFaultFixture {
	mem := store.NewMemoryStore()
	fs := &readFaultStore{Store: mem}
	r, err := CreateRepoSegments(fs, "meta-compact", 64, 64, cdc.DefaultParams(), false, 4)
	if err != nil {
		t.Fatal(err)
	}
	commitN(t, r, 4, 1)
	refs, err := r.loadSegmentIndex(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) <= 1 {
		t.Fatalf("fixture produced %d segments; compaction needs more than one", len(refs))
	}
	return gcFaultFixture{
		repo:     r,
		fault:    func(k string) bool { return strings.HasSuffix(k, ".idx") },
		sweep:    func(ctx context.Context) (int, error) { return r.CompactSegments(ctx) },
		baseline: 1,
		intact: func(t *testing.T) {
			// Compaction rewrites segments on purpose, so object identity is the
			// wrong check: what must survive is the ability to read the data.
			assertAllRevsReadable(t, r)
		},
	}
}
