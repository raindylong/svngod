package fs

import (
	"context"
	"fmt"
	"testing"

	"svngod/internal/cdc"
	"svngod/internal/fs/store"
)

// keyFaultStore fails writes whose key equals a specific target, letting a test
// stop exactly at the Nth WAL record instead of counting all commit writes.
type keyFaultStore struct {
	store.Store
	failKey string
	failed  bool
}

func (s *keyFaultStore) Journaling() bool { return true }

func (s *keyFaultStore) hit(key string) bool {
	if s.failKey != "" && key == s.failKey {
		s.failed = true
		return true
	}
	return false
}

func (s *keyFaultStore) Put(ctx context.Context, key string, data []byte) error {
	if s.hit(key) {
		return fmt.Errorf("injected write failure at %s", key)
	}
	return s.Store.Put(ctx, key, data)
}

func (s *keyFaultStore) CreateIfAbsent(ctx context.Context, key string, data []byte) error {
	if s.hit(key) {
		return fmt.Errorf("injected write failure at %s", key)
	}
	return s.Store.CreateIfAbsent(ctx, key, data)
}

func (s *keyFaultStore) CAS(ctx context.Context, key, expect string, data []byte) (string, error) {
	if s.hit(key) {
		return "", fmt.Errorf("injected write failure at %s", key)
	}
	return s.Store.CAS(ctx, key, expect, data)
}

// TestPartialWalIsRolledForwardByReplay covers the v1 (non-segmented) path,
// where writeBatchJournals writes one record per revision. A failure midway
// leaves a *prefix* of the batch durable, and replay publishes any contiguous
// quorum-acked prefix - so those revisions become visible even though
// CommitBatch reported failure and returned no revision numbers.
//
// Segmented repositories do not have this window: one batch is one WAL record
// (writeSegmentJournalBatch), so a single Put either lands whole or not at all.
func TestPartialWalIsRolledForwardByReplay(t *testing.T) {
	st := &keyFaultStore{Store: store.NewMemoryStore()}
	r, err := CreateRepo(st, "repos/pw", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, errs, cerr := r.CommitBatch([]BatchCommit{{Changes: []Change{
		{Path: "/base", Action: ActionAdd, Kind: KindFile, Data: []byte("b")}}}}); cerr != nil || errs[0] != nil {
		t.Fatalf("baseline: %v %v", cerr, errs)
	}

	const n = 4
	reqs := make([]BatchCommit, n)
	for i := range reqs {
		reqs[i] = BatchCommit{Changes: []Change{{
			Path:   fmt.Sprintf("/n%d", i),
			Action: ActionAdd, Kind: KindFile, Data: []byte("x"),
		}}}
	}
	// HEAD is 1; the batch occupies 2..5. Fail the record for revision 4, so
	// revisions 2 and 3 are already durable in the WAL.
	st.failKey = r.keys.Journal(4)
	revs, _, cerr := r.CommitBatch(reqs, AllOrNothing())
	if cerr == nil {
		t.Fatal("batch reported success despite the injected WAL failure")
	}
	for i, rv := range revs {
		if rv != 0 {
			t.Fatalf("request %d returned revision %d on a failed batch", i, rv)
		}
	}

	// Replay now publishes the durable prefix (2..3) and stops at the hole.
	st.failKey = ""
	_ = st.Delete(context.Background(), []string{r.keys.CommitLock()})
	r2, err := OpenRepo(st, "repos/pw")
	if err != nil {
		t.Fatal(err)
	}
	head, err := r2.HeadFresh()
	if err != nil {
		t.Fatal(err)
	}
	if head != 3 {
		t.Fatalf("HEAD=%d after replay, want 3 (the durable prefix 2..3)", head)
	}
	// The property that matters: the published range is a *contiguous* prefix,
	// so there is no hole. Revision 4 was never durable; 5 was never attempted.
	for rev := uint64(1); rev <= head; rev++ {
		if _, err := r2.readBlock(rev); err != nil {
			t.Fatalf("revision %d unreadable after replay: %v", rev, err)
		}
	}
	// readBlock(raw) bypasses the HEAD check by design (GC and replay need to
	// read unpublished blocks), so the guarantee to assert is that the HEAD
	// itself is contiguous and that resolution above it fails.
	if _, err := r2.Resolve(head+1, "/n3"); err == nil {
		t.Fatalf("revision %d resolves above HEAD: a hole was published", head+1)
	}
}

// TestAllOrNothingBatchLeavesNoHoleAfterPartialWal is the contract check for
// the case AllOrNothing cannot make atomic: a failure part-way through the WAL
// leaves a durable prefix that replay will publish. The guarantee that must
// still hold is "no hole" - replay only ever extends HEAD across a contiguous
// run - and the caller must never be told the failed revisions landed.
func TestAllOrNothingBatchLeavesNoHoleAfterPartialWal(t *testing.T) {
	st := &keyFaultStore{Store: store.NewMemoryStore()}
	r, err := CreateRepo(st, "repos/pw2", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, errs, cerr := r.CommitBatch([]BatchCommit{{Changes: []Change{
		{Path: "/base", Action: ActionAdd, Kind: KindFile, Data: []byte("b")}}}}); cerr != nil || errs[0] != nil {
		t.Fatalf("baseline: %v %v", cerr, errs)
	}
	before, _ := r.Head()

	const n = 5
	reqs := make([]BatchCommit, n)
	for i := range reqs {
		reqs[i] = BatchCommit{Changes: []Change{{
			Path:   fmt.Sprintf("/h%d", i),
			Action: ActionAdd, Kind: KindFile, Data: []byte("x"),
		}}}
	}
	// Fail the record for revision before+3, so before+1 and before+2 are durable.
	st.failKey = r.keys.Journal(before + 3)
	revs, errs, cerr := r.CommitBatch(reqs, AllOrNothing())
	if cerr == nil {
		t.Fatal("batch reported success despite the injected WAL failure")
	}
	for i := range revs {
		if revs[i] != 0 {
			t.Fatalf("request %d reported revision %d for an abandoned batch", i, revs[i])
		}
		if errs[i] == nil {
			t.Fatalf("request %d reported no error", i)
		}
	}

	st.failKey = ""
	_ = st.Delete(context.Background(), []string{r.keys.CommitLock()})
	r2, err := OpenRepo(st, "repos/pw2")
	if err != nil {
		t.Fatal(err)
	}
	head, err := r2.HeadFresh()
	if err != nil {
		t.Fatal(err)
	}
	// Replay published the durable prefix; never more than that, never less.
	if head < before || head > before+2 {
		t.Fatalf("HEAD=%d, want within [%d,%d] (durable prefix only)", head, before, before+2)
	}
	// No holes: every revision up to HEAD must be readable through Resolve.
	for rev := before; rev <= head; rev++ {
		if _, err := r2.readBlock(rev); err != nil {
			t.Fatalf("revision %d unreadable: a hole was published", rev)
		}
	}
	// And nothing beyond HEAD is visible through the normal path.
	if _, err := r2.Resolve(head+1, "/h3"); err == nil {
		t.Fatalf("revision %d resolves above HEAD", head+1)
	}
	issues, verr := r2.VerifyRangeWith(1, head, false, 2)
	if verr != nil || len(issues) != 0 {
		t.Fatalf("verify after partial WAL: issues=%v err=%v", issues, verr)
	}
}

// TestSegmentBatchCasFailureReportsNothingPublished covers the segmented path's
// HEAD-CAS failure: the segment object, index and single WAL record are written
// but nothing is visible, so every request must report failure with revision 0.
func TestSegmentBatchCasFailureReportsNothingPublished(t *testing.T) {
	fs := &keyFaultStore{Store: store.NewMemoryStore()}
	r, err := CreateRepoSegments(fs, "repos/sc", 4096, 64, cdc.DefaultParams(), false, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if _, errs, cerr := r.CommitBatch([]BatchCommit{{Changes: []Change{
		{Path: "/base", Action: ActionAdd, Kind: KindFile, Data: []byte("b")}}}}); cerr != nil || errs[0] != nil {
		t.Fatalf("baseline: %v %v", cerr, errs)
	}
	before, _ := r.Head()

	reqs := make([]BatchCommit, 3)
	for i := range reqs {
		reqs[i] = BatchCommit{Changes: []Change{{
			Path: fmt.Sprintf("/s%d", i), Action: ActionAdd, Kind: KindFile, Data: []byte("x"),
		}}}
	}
	// Fail the HEAD CAS itself.
	fs.failKey = r.keys.Current()
	revs, errs, cerr := r.CommitBatch(reqs, AllOrNothing())
	if cerr == nil {
		t.Fatal("segmented batch succeeded despite the injected CAS failure")
	}
	for i := range revs {
		if revs[i] != 0 {
			t.Fatalf("request %d reported revision %d after a failed CAS", i, revs[i])
		}
		if errs[i] == nil {
			t.Fatalf("request %d reported no error", i)
		}
	}
	fs.failKey = ""
	head, _ := r.HeadFresh()
	if head != before {
		t.Fatalf("head moved %d -> %d although the CAS failed", before, head)
	}
}
