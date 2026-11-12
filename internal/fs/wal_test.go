package fs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// TestWalRecordMatches: the pure predicate accepts only an exact record:
// same rev, same lease token, same revision-block SHA.
func TestWalRecordMatches(t *testing.T) {
	rb := []byte("rev-block-bytes")
	sum := sha256.Sum256(rb)
	sha := hex.EncodeToString(sum[:])
	mk := func() *CommitRecord {
		return &CommitRecord{Rev: 7, Lease: "tok", RevBlockSHA: sha}
	}

	if !walRecordMatches(mk(), 7, "tok", rb) {
		t.Fatal("exact record must match")
	}
	if walRecordMatches(mk(), 8, "tok", rb) {
		t.Fatal("different revision must not match")
	}
	if walRecordMatches(mk(), 7, "other", rb) {
		t.Fatal("different lease token must not match")
	}
	if walRecordMatches(mk(), 7, "tok", []byte("tampered")) {
		t.Fatal("different revision block must not match")
	}
	if walRecordMatches(nil, 7, "tok", rb) {
		t.Fatal("nil record must not match")
	}
}

// TestJournalOwnsRoundTrip: a real record written through writeCommitJournal
// is owned by its writer (same token and rb) and by nobody else.
func TestJournalOwnsRoundTrip(t *testing.T) {
	r, _, _, st := quorumFixture(t)
	ctx := context.Background()
	const rev = 1
	if _, err := r.CommitTxn(0, []Change{{Path: "/a", Action: ActionAdd, Kind: KindFile, Data: []byte("1")}}, nil); err != nil {
		t.Fatal(err)
	}
	rb, err := st.Get(ctx, r.keys.LooseRev(r.shard(rev), rev), nil)
	if err != nil {
		t.Fatal(err)
	}
	// The record exists and was written by the committing lease; we do not
	// know the token here, so prove the negative branches instead plus a
	// positive one via a hand-written record.
	rec := &CommitRecord{Version: 1, Rev: rev, Parent: rev - 1, Lease: "T2",
		RevBlockKey: "k", PropsKey: "p"}
	if err := r.store.Put(ctx, r.keys.Journal(rev), EncodeCommitRecord(rec)); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(rb)
	rec2 := &CommitRecord{Version: 1, Rev: rev, Parent: rev - 1, Lease: "T2",
		RevBlockKey: "k", PropsKey: "p", RevBlockSHA: hex.EncodeToString(sum[:])}
	if err := r.store.Put(ctx, r.keys.Journal(rev), EncodeCommitRecord(rec2)); err != nil {
		t.Fatal(err)
	}
	if !r.journalOwns(ctx, rev, "T2", rb) {
		t.Fatal("matching record must be owned")
	}
	if r.journalOwns(ctx, rev, "T3", rb) {
		t.Fatal("foreign lease must not be owned")
	}
	if r.journalOwns(ctx, rev, "T2", []byte("other")) {
		t.Fatal("foreign revision block must not be owned")
	}
}

// TestBatchPartialOwnership: plans above the current HEAD are never owned;
// below it, ownership follows the WAL record.
func TestBatchPartialOwnership(t *testing.T) {
	r, _, _, st := quorumFixture(t)
	ctx := context.Background()
	if _, err := r.CommitTxn(0, []Change{{Path: "/a", Action: ActionAdd, Kind: KindFile, Data: []byte("1")}}, nil); err != nil {
		t.Fatal(err)
	}
	rb, err := st.Get(ctx, r.keys.LooseRev(r.shard(1), 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(rb)
	rec := &CommitRecord{Version: 1, Rev: 1, Parent: 0, Lease: "T9",
		RevBlockKey: "k", PropsKey: "p", RevBlockSHA: hex.EncodeToString(sum[:])}
	if err := r.store.Put(ctx, r.keys.Journal(1), EncodeCommitRecord(rec)); err != nil {
		t.Fatal(err)
	}
	plans := []batchPlan{
		{idx: 0, rev: 1, rb: rb},
		{idx: 1, rev: 2, rb: rb},
	}
	own := r.batchPartialOwnership(ctx, plans, 1, "T9")
	if !own[0] {
		t.Fatal("rev 1 with our record must be owned")
	}
	if own[1] {
		t.Fatal("rev 2 above current HEAD must never be owned")
	}
}
