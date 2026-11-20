package fs

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"svngod/internal/fs/store"
)

// racStore injects the exact race the v1 CommitBatch CAS branch is meant to
// handle: between the batch reading HEAD and its CAS, another writer publishes a
// prefix of the range. The store cannot see the batch's plans, so the test
// signals the "peer published up to N" point explicitly.
//
// This is what WAL replay on a second instance does: replay publishes every
// contiguous quorum-acked record, so a batch of revisions 2..5 can find HEAD
// already at 2 or 3 when it finally CASes.
type racStore struct {
	store.Store
	// publishBeforeCAS, when non-nil, is called on the FIRST CAS to keys.Current:
	// it returns the head a peer has published in the meantime, and the store
	// then fails the CAS with ErrPreconditionFailed, exactly as a real conflict
	// would.
	publishBeforeCAS func() uint64
	fired            bool
}

func (s *racStore) Journaling() bool { return true }

func (s *racStore) CAS(ctx context.Context, key, expect string, data []byte) (string, error) {
	if s.publishBeforeCAS != nil && !s.fired && strings.HasSuffix(key, "current") {
		s.fired = true
		head := s.publishBeforeCAS()
		// Land the peer's publication, then report the conflict: the etag the
		// batch read is now stale.
		cur := &Current{Head: head, Created: 1}
		if _, err := s.Store.CAS(ctx, key, expect, cur.Marshal()); err != nil {
			return "", err
		}
		return "", store.ErrPreconditionFailed
	}
	return s.Store.CAS(ctx, key, expect, data)
}

// TestV1BatchCasRaceReportsOnlyPublishedRevisions is the v1 counterpart of
// TestSegmentBatchCasFailureReportsNothingPublished.
//
// When the HEAD CAS loses to a peer's replay, the batch must report a revision
// ONLY for requests whose durable record the peer actually published. Anything
// above the new HEAD was not published: reporting its number tells an
// AllOrNothing caller (svnadmin load) that the revision landed.
//
// svnadmin load only inspects err and rv == 0 (dump/load.go), so a nonzero
// revision here means the loader advances lastRev past revisions that were
// never written - silent history loss on restore.
func TestV1BatchCasRaceReportsOnlyPublishedRevisions(t *testing.T) {
	rac := &racStore{Store: store.NewMemoryStore()}
	r, err := CreateRepo(rac, "repos/rac", 4096, 64)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// Baseline revision 1 so the batch starts at 2.
	if _, err := r.CommitTxn(0, []Change{
		{Path: "/base.txt", Action: ActionAdd, Kind: KindFile, Data: []byte("b")},
	}, map[string]string{"svn:log": "base"}); err != nil {
		t.Fatalf("baseline: %v", err)
	}

	reqs := make([]BatchCommit, 4) // revisions 2,3,4,5
	for i := range reqs {
		reqs[i] = BatchCommit{Changes: []Change{{
			Path: fmt.Sprintf("/f%d", i), Action: ActionAdd, Kind: KindFile, Data: []byte("x"),
		}}}
	}

	// The peer publishes revision 2 only (a one-revision prefix of 2..5) before
	// our CAS lands.
	rac.publishBeforeCAS = func() uint64 { return 2 }

	revs, errs, cerr := r.CommitBatch(reqs, AllOrNothing())

	head, _ := r.HeadFresh()
	if head != 2 {
		t.Fatalf("head=%d, want 2 (the peer's publication)", head)
	}
	// The invariant: every reported revision must be <= head, i.e. actually visible.
	for i, rv := range revs {
		if rv == 0 {
			continue
		}
		if rv > head {
			t.Fatalf("request %d reported revision %d but head is %d: that revision was never published", i, rv, head)
		}
	}
	if cerr != nil {
		t.Fatalf("unexpected top-level error: %v", cerr)
	}
	// Requests whose revisions were not published must carry an error, so an
	// AllOrNothing caller does not treat them as done.
	for i, rv := range revs {
		if rv == 0 && errs[i] == nil {
			t.Fatalf("request %d reported no revision and no error", i)
		}
	}
}
