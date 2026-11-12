package fs

import (
	"sync"
	"testing"

	"svngod/internal/cdc"
	"svngod/internal/fs/store"
)

// mirrorFixture builds a repository on a two-member WAL mirror (quorum 2), the
// shape where journal records actually exist and the hash chain matters.
func mirrorFixture(t *testing.T, prefix string) (*Repo, []*store.MemoryStore) {
	t.Helper()
	mems := []*store.MemoryStore{store.NewMemoryStore(), store.NewMemoryStore()}
	ms := store.NewMirror([]store.Store{mems[0], mems[1]},
		store.WithMutableOrder(CompareMutable), store.WithRepoPrefix(prefix))
	// Lazy repair scans the whole prefix on construction; disable it so the
	// fixture is deterministic and fast.
	if _, err := CreateRepo(ms, prefix, 4096, 64); err != nil {
		t.Fatal(err)
	}
	r, err := OpenRepo(ms, prefix)
	if err != nil {
		t.Fatal(err)
	}
	return r, mems
}

// TestBatchWALChainStaysIntactUnderGroupCommit is the regression guard for
// group commit: one CommitBatch publishes N revisions, and every journal record
// must still chain to its predecessor. Records within a batch were written
// concurrently, so the chain depended on each writer seeing the previous
// record's bytes - which a single-slot cache cannot guarantee.
func TestBatchWALChainStaysIntactUnderGroupCommit(t *testing.T) {
	r, _ := mirrorFixture(t, "gc")
	const n = 12
	reqs := make([]BatchCommit, 0, n)
	for i := 0; i < n; i++ {
		reqs = append(reqs, BatchCommit{Changes: []Change{{
			Path: "/b" + itoa(i), Action: ActionAdd, Kind: KindFile, Data: []byte("payload"),
		}}})
	}
	revs, errs, err := r.CommitBatch(reqs)
	if err != nil {
		t.Fatalf("CommitBatch: %v", err)
	}
	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("req %d: %v", i, errs[i])
		}
	}
	if revs[0] != 1 || revs[n-1] != n {
		t.Fatalf("revs %v, want 1..%d", revs, n)
	}
	if err := r.VerifyJournalChain(1, n); err != nil {
		t.Fatalf("WAL chain broken after group commit: %v", err)
	}
	// VerifyJournalChain accepts an empty PrevJournalSHA (legacy records), so it
	// cannot tell "chain threaded" from "chain restarted every batch". Assert the
	// links explicitly: a regression here silently disables tamper detection for
	// every grouped commit.
	for rev := uint64(2); rev <= n; rev++ {
		rec, err := r.JournalRecord(rev)
		if err != nil || rec == nil {
			t.Fatalf("rev %d record: %v", rev, err)
		}
		if rec.PrevJournalSHA == "" {
			t.Fatalf("rev %d lost its chain link under group commit", rev)
		}
	}
}

// TestConcurrentCommitsKeepChainOnMirror covers the batcher path: many
// goroutines submit single-revision commits that get coalesced into batches.
func TestConcurrentCommitsKeepChainOnMirror(t *testing.T) {
	ConfigureCommitBatch(8, 5)
	defer ConfigureCommitBatch(0, 0)
	r, _ := mirrorFixture(t, "gcc")
	const n = 20
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = r.CommitTxn(0, []Change{{
				Path: "/c" + itoa(i), Action: ActionAdd, Kind: KindFile, Data: []byte("p"),
			}}, nil)
		}(i)
	}
	wg.Wait()
	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("commit %d: %v", i, errs[i])
		}
	}
	head, _ := r.HeadFresh()
	if err := r.VerifyJournalChain(1, head); err != nil {
		t.Fatalf("WAL chain broken under concurrent group commit: %v", err)
	}
}

func itoa(i int) string {
	if i < 10 {
		return string(rune('0' + i))
	}
	return string(rune('a' + i - 10))
}

// TestSegmentedBatchChainVerifies covers the version-2 record shape: group
// commit writes ONE record per batch covering [First, First+Count), so the
// revisions after it legitimately have no record of their own. VerifyJournalChain
// used to report the first such revision as "chain gap at rev 2" and declare a
// healthy segmented repository corrupt - every real v2 repository failed verify.
func TestSegmentedBatchChainVerifies(t *testing.T) {
	mems := []*store.MemoryStore{store.NewMemoryStore(), store.NewMemoryStore()}
	ms := store.NewMirror([]store.Store{mems[0], mems[1]},
		store.WithMutableOrder(CompareMutable), store.WithRepoPrefix("segc"))
	r, err := CreateRepoSegments(ms, "segc", 4096, 64, cdc.DefaultParams(), false, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Two batches so the chain has an inter-batch link as well.
	for round := 0; round < 2; round++ {
		reqs := make([]BatchCommit, 0, 10)
		for i := 0; i < 10; i++ {
			reqs = append(reqs, BatchCommit{Changes: []Change{{
				Path: "/s" + itoa(round*10+i), Action: ActionAdd, Kind: KindFile, Data: []byte("x"),
			}}})
		}
		if _, errs, err := r.CommitBatch(reqs); err != nil {
			t.Fatalf("round %d: %v", round, err)
		} else {
			for i := range errs {
				if errs[i] != nil {
					t.Fatalf("round %d req %d: %v", round, i, errs[i])
				}
			}
		}
	}
	head, _ := r.HeadFresh()
	if head != 20 {
		t.Fatalf("head=%d, want 20", head)
	}
	if err := r.VerifyJournalChain(1, head); err != nil {
		t.Fatalf("segmented repository must pass the chain check: %v", err)
	}
	// A batch record must still be tamper-evident: the second batch's record
	// chains to the first batch's record, not to a per-revision record.
	rec, err := r.JournalRecord(11)
	if err != nil || rec == nil {
		t.Fatalf("record for rev 11 (second batch first rev): %v", err)
	}
	if rec.PrevJournalSHA == "" {
		t.Fatal("second batch record lost its chain link to the first batch")
	}
}

// TestBatchResolvesNodesItJustCreated covers the dependency that makes a batch
// of consecutive revisions possible at all: a later request modifies a
// directory created by an earlier one in the same batch. The directory's
// NodeID points at a revision that is not published yet, so resolving it from
// the store fails ("object not found") unless the batch keeps its own in-memory
// nodes. This is the ordinary shape of an incremental dump import, where
// revision 2 modifies what revision 1 added.
func TestBatchResolvesNodesItJustCreated(t *testing.T) {
	r, _ := mirrorFixture(t, "gcnode")
	reqs := []BatchCommit{
		{Changes: []Change{
			{Path: "/trunk", Action: ActionAdd, Kind: KindDir},
			{Path: "/trunk/a.txt", Action: ActionAdd, Kind: KindFile, Data: []byte("v1")},
		}},
		{Changes: []Change{
			{Path: "/trunk", Action: ActionModify, Kind: KindDir, Props: map[string]string{"svn:ignore": "*.o"}},
			{Path: "/trunk/a.txt", Action: ActionModify, Kind: KindFile, Data: []byte("v2 longer")},
		}},
		{Changes: []Change{
			{Path: "/trunk/b.txt", Action: ActionAdd, Kind: KindFile, Data: []byte("b")},
		}},
	}
	revs, errs, err := r.CommitBatch(reqs)
	if err != nil {
		t.Fatalf("CommitBatch: %v", err)
	}
	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("req %d: %v", i, errs[i])
		}
	}
	if revs[0] != 1 || revs[1] != 2 || revs[2] != 3 {
		t.Fatalf("revs=%v, want 1,2,3", revs)
	}
	// Each revision must see the tree as of its own point in the batch.
	nr, err := r.Resolve(2, "/trunk/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.GetText(nr.Node)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "v2 longer" {
		t.Fatalf("rev2 content = %q", got)
	}
	dnr, err := r.Resolve(2, "/trunk")
	if err != nil {
		t.Fatal(err)
	}
	if dnr.Props["svn:ignore"] != "*.o" {
		t.Fatalf("rev2 dir props = %v", dnr.Props)
	}
	if _, err := r.Resolve(3, "/trunk/b.txt"); err != nil {
		t.Fatalf("rev3 must see both the earlier files and its own: %v", err)
	}
}
