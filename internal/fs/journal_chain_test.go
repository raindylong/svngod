package fs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"svngod/internal/fs/store"
)

// chainFixture: quorum mirror + 3 committed revisions; each journal record
// after the first must carry the SHA of its predecessor's stored bytes.
func chainFixture(t *testing.T) (*Repo, *store.MirrorStore) {
	t.Helper()
	ms := store.NewMirror([]store.Store{
		store.NewMemoryStore(), store.NewMemoryStore(), store.NewMemoryStore(),
	}, store.WithWriteQuorum(2))
	r, err := CreateRepo(ms, "hc", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := r.CommitTxn(uint64(i), []Change{{
			Path: "/c", Action: ActionAdd, Kind: KindFile, Data: []byte("v"),
		}}, nil); err != nil {
			t.Fatal(err)
		}
	}
	return r, ms
}

// TestJournalChainLinked: every record after the first pins its predecessor.
func TestJournalChainLinked(t *testing.T) {
	r, _ := chainFixture(t)
	for rev := uint64(2); rev <= 3; rev++ {
		prev, err := r.store.Get(context.Background(), r.keys.Journal(rev-1), nil)
		if err != nil {
			t.Fatal(err)
		}
		rec, err := r.JournalRecord(rev)
		if err != nil || rec == nil {
			t.Fatalf("record %d missing: %v", rev, err)
		}
		sum := sha256.Sum256(prev)
		if rec.PrevJournalSHA != hex.EncodeToString(sum[:]) {
			t.Fatalf("rev %d: PrevJournalSHA=%q, want SHA of stored rev %d", rev, rec.PrevJournalSHA, rev-1)
		}
	}
	if err := r.VerifyJournalChain(1, 3); err != nil {
		t.Fatalf("chain verify on intact journal: %v", err)
	}
}

// TestJournalChainDetectsTamper: rewriting a historical record must break
// the chain at the next revision.
func TestJournalChainDetectsTamper(t *testing.T) {
	r, _ := chainFixture(t)
	ctx := context.Background()
	b2, err := r.store.Get(ctx, r.keys.Journal(2), nil)
	if err != nil {
		t.Fatal(err)
	}
	// Rewrite journal/2 with a forged (well-formed) record: same fields, but
	// a different author - the historical rewrite WORM must catch.
	rec, err := DecodeCommitRecord(b2)
	if err != nil {
		t.Fatal(err)
	}
	rec.Author = "attacker"
	if err := r.store.Put(ctx, r.keys.Journal(2), EncodeCommitRecord(rec)); err != nil {
		t.Fatal(err)
	}
	err = r.VerifyJournalChain(1, 3)
	if err == nil {
		t.Fatal("chain verify accepted a rewritten historical record")
	}
}

// TestJournalChainAcceptsLegacyRecords: records without PrevJournalSHA
// (pre-4.2) leave the chain walk running, not failing.
func TestJournalChainAcceptsLegacyRecords(t *testing.T) {
	r, _ := chainFixture(t)
	ctx := context.Background()
	for rev := uint64(1); rev <= 3; rev++ {
		b, err := r.store.Get(ctx, r.keys.Journal(rev), nil)
		if err != nil {
			t.Fatal(err)
		}
		rec, derr := DecodeCommitRecord(b)
		if derr != nil {
			t.Fatal(derr)
		}
		rec.PrevJournalSHA = "" // strip: simulate legacy records
		if err := r.store.Put(ctx, r.keys.Journal(rev), EncodeCommitRecord(rec)); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.VerifyJournalChain(1, 3); err != nil {
		t.Fatalf("legacy records must not fail the chain walk: %v", err)
	}
}

// TestReplayPausedOnBrokenChain: a forged predecessor record must stop WAL
// replay from publishing on top of tampered history.
func TestReplayPausedOnBrokenChain(t *testing.T) {
	r, ms := chainFixture(t)
	ctx := context.Background()
	// Forge journal/1, then rewind HEAD to 0 so replay must walk 1,2,3.
	b1, err := ms.Get(ctx, r.keys.Journal(1), nil)
	if err != nil {
		t.Fatal(err)
	}
	rec, derr := DecodeCommitRecord(b1)
	if derr != nil {
		t.Fatal(derr)
	}
	rec.Log = "rewritten history"
	if err := ms.Put(ctx, r.keys.Journal(1), EncodeCommitRecord(rec)); err != nil {
		t.Fatal(err)
	}
	cur, etag, err := r.readCurrent()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ms.CAS(ctx, r.keys.Current(), etag, (&Current{Head: 0, Created: cur.Created}).Marshal()); err != nil {
		t.Fatal(err)
	}
	r.reconcileJournal(true)
	// rev 1's own record verifies (it has no predecessor to pin), but the
	// forged bytes make rev 2's PrevJournalSHA mismatch: replay must stop
	// there and never publish rev 2/3 on top of tampered history.
	if hc, _, rerr := r.readCurrent(); rerr != nil || hc.Head != 1 {
		t.Fatalf("head=%v (err %v) after broken-chain replay, want 1: replay must stop at the chain break, not roll rev 2/3 over tampered history", hc.Head, rerr)
	}
}
