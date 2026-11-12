package fs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
)

// This file keeps the batch/publication decision logic pure: every ownership
// question about a durable WAL record ("did our commit get published?") is
// answered from record fields alone, so the same predicate guards the single
// commit path, the batch path, and recovery. Store reads stay in thin
// wrappers next to the predicates.

// walRecordMatches reports whether a decoded WAL record describes this exact
// commit: same revision, same lease token, and the same revision-block bytes
// (by SHA-256). rev==0 and parent checks are caller policy via opts below.
func walRecordMatches(rec *CommitRecord, rev uint64, token string, rbBytes []byte) bool {
	if rec == nil || rec.Rev != rev || rec.Lease != token {
		return false
	}
	sum := sha256.Sum256(rbBytes)
	return rec.RevBlockSHA == hex.EncodeToString(sum[:])
}

// journalOwns reports whether the durable WAL record for rev still describes
// this writer's commit (same lease token and revision block). Used after a
// failed HEAD CAS or a failed post-WAL lease check: the record being ours
// means the revision can still be rolled forward by replay.
func (r *Repo) journalOwns(ctx context.Context, rev uint64, token string, rbBytes []byte) bool {
	if !r.journal {
		return false
	}
	b, err := r.store.Get(ctx, r.keys.Journal(rev), nil)
	if err != nil {
		return false
	}
	rec, derr := DecodeCommitRecord(b)
	if derr != nil {
		return false
	}
	return walRecordMatches(rec, rev, token, rbBytes)
}

// publishedByReplay reports whether rev is already the published HEAD and the
// journal record for it describes this commit: same parent, same lease token
// and the same revision block. WAL replay publishes a record once it is
// quorum-acked and complete, so a writer that lost the HEAD CAS to that replay
// can safely treat its commit as successful.
func (r *Repo) publishedByReplay(ctx context.Context, rev uint64, token string, rbBytes []byte, parent uint64) bool {
	if !r.journal {
		return false
	}
	cur, _, err := r.readCurrentStrict()
	if err != nil || cur.Head != rev {
		return false
	}
	b, err := r.store.Get(ctx, r.keys.Journal(rev), nil)
	if err != nil {
		return false
	}
	rec, derr := DecodeCommitRecord(b)
	if derr != nil || rec.Parent != parent {
		return false
	}
	return walRecordMatches(rec, rev, token, rbBytes)
}

// batchPartialOwnership returns, for each plan, whether its revision was
// already published durably by our WAL record after the batch lost the HEAD
// CAS. now is the freshly re-read HEAD; a revision above now can never be
// owned yet. The returned slice mirrors plans order.
func (r *Repo) batchPartialOwnership(ctx context.Context, plans []batchPlan, now uint64, token string) []bool {
	own := make([]bool, len(plans))
	for i, p := range plans {
		if p.rev > now {
			continue
		}
		own[i] = r.journalOwns(ctx, p.rev, token, p.rb)
	}
	return own
}
