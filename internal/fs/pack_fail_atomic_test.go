package fs

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"svngod/internal/fs/store"
)

// A commit that publishes a revision must report success, and a commit that
// reports failure must not have published. The shard auto-pack step runs AFTER
// HEAD has advanced; if it fails, returning (N, err) tells the client the commit
// failed while revision N is durable and visible. The client retries and is then
// rejected as out-of-date - the classic "ack iff committed" violation.
//
// This store fails a chosen number of Puts after the HEAD CAS, which is what a
// transient backend error during pack looks like.
type packFailStore struct {
	store.Store
	// failPackUntilCleared fails EVERY pack-object write while set. A one-shot
	// countdown is not enough: packShardLocked issues several writes, so a
	// single injected failure lands on one of them and a retry that reaches a
	// different key would succeed - the test would then pass without the retry
	// logic existing at all.
	// Both fields are accessed from the server's background pack-retry
	// goroutine as well as from the test goroutine, so they must be atomic.
	// Plain bools here were a real data race - caught by `-race` in CI, not by
	// the assertions, because the assertions only look at the values and the
	// race is between the writes themselves.
	failPackUntilCleared atomic.Bool
	// sawPackWrite records that the injected path was reached.
	sawPackWrite atomic.Bool
}

func (s *packFailStore) Put(ctx context.Context, key string, data []byte) error {
	if s.failPackUntilCleared.Load() && isPackKey(key) {
		s.sawPackWrite.Store(true)
		return errors.New("injected: backend write failed during pack")
	}
	return s.Store.Put(ctx, key, data)
}

// isPackKey matches the objects packShardLocked writes (packed rev / index /
// revprops / done marker) but NOT the loose objects a commit writes beforehand.
func isPackKey(key string) bool {
	// The packed objects end with .pack / .idx / .done (see store.Keys). Loose
	// rev objects use a different suffix, so a commit's own writes are untouched.
	return strings.HasSuffix(key, ".pack") || strings.HasSuffix(key, ".idx") || strings.HasSuffix(key, ".done")
}

// TestCommitReportedFailureMustNotHavePublished is the invariant: if CommitTxn
// returns an error, revision N must not be visible.
func TestCommitReportedFailureMustNotHavePublished(t *testing.T) {
	mem := store.NewMemoryStore()
	// shardSize 1: every commit closes a shard, so pack runs on every commit.
	st := &packFailStore{Store: mem}
	repo, err := CreateRepo(st, "pf", 1, 8)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// Seed revision 1 (pack for shard 0 runs here; let it succeed).
	if _, err := repo.CommitTxn(0, []Change{
		{Path: "/a.txt", Action: ActionAdd, Kind: KindFile, Data: []byte("1")},
	}, map[string]string{"svn:log": "one"}); err != nil {
		t.Fatalf("seed commit: %v", err)
	}

	// Fail the pack writes of the next commit only. Cleared right after so the
	// background retry (if any) can succeed and does not keep the store failing.
	st.failPackUntilCleared.Store(true)

	rev, err := repo.CommitTxn(1, []Change{
		{Path: "/b.txt", Action: ActionAdd, Kind: KindFile, Data: []byte("2")},
	}, map[string]string{"svn:log": "two"})

	st.failPackUntilCleared.Store(false)

	// The injection must actually have fired, or the test proves nothing about
	// pack failures: a fault that never reaches the pack path would make the
	// "reported success" branch trivially true.
	if !st.sawPackWrite.Load() {
		t.Fatal("the injected pack failure never fired; the test did not exercise the pack path")
	}

	head, _ := repo.Head()
	if err != nil {
		// Reported failure. Then nothing may have been published.
		if head >= 2 {
			t.Fatalf("commit reported error %v but published revision %d (head=%d): the client will retry an applied change", err, rev, head)
		}
		return
	}
	// Reported success. Then the revision must be readable.
	if rev != 2 {
		t.Fatalf("success with rev=%d, want 2", rev)
	}
	if _, gerr := repo.GetRevProps(2); gerr != nil {
		t.Fatalf("commit reported success but revision 2 is unreadable: %v", gerr)
	}
}

// TestFailedPackIsRetriedInBackground closes the loop on "report success and
// move on": if the pack never runs again, the shard's loose objects stay behind
// forever, because GC only removes loose copies of shards that are ALREADY
// packed. A transient error would then cost the repository its O(shards) object
// budget - the very property packing exists to provide.
//
// The store fails exactly one post-CAS write, so the first pack fails and the
// retry succeeds.
func TestFailedPackIsRetriedInBackground(t *testing.T) {
	mem := store.NewMemoryStore()
	st := &packFailStore{Store: mem}
	repo, err := CreateRepo(st, "pr", 1, 8) // shardSize 1: pack runs every commit
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := repo.CommitTxn(0, []Change{
		{Path: "/a.txt", Action: ActionAdd, Kind: KindFile, Data: []byte("1")},
	}, map[string]string{"svn:log": "one"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// shardSize 1: committing revision 2 closes shard 1, so pack shard 1 runs
	// and is the one that fails. Asserting on shard 0 would check a shard that
	// was already packed by the seed commit - the test would pass without any
	// retry existing.
	st.failPackUntilCleared.Store(true)
	if _, err := repo.CommitTxn(1, []Change{
		{Path: "/b.txt", Action: ActionAdd, Kind: KindFile, Data: []byte("2")},
	}, map[string]string{"svn:log": "two"}); err != nil {
		// The commit itself must succeed; pack failure is not a commit failure.
		t.Fatalf("commit failed although only the post-CAS pack was injected: %v", err)
	}
	if !st.sawPackWrite.Load() {
		t.Fatal("the injected pack failure never fired")
	}
	// Confirm the shard really is unpacked at this point, so the assertion below
	// is about the retry and not about a pack that never needed to happen.
	if packed, perr := repo.isPacked(1); perr != nil || packed {
		t.Fatalf("shard 1 should be unpacked right after the failed pack (packed=%v err=%v)", packed, perr)
	}

	// Let the retry succeed.
	st.failPackUntilCleared.Store(false)

	deadline := time.Now().Add(15 * time.Second)
	for {
		packed, perr := repo.isPacked(1)
		if perr == nil && packed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("shard 1 was never packed after the retry (packed=%v err=%v)", packed, perr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestAbandonedPackLeavesRepositoryReadable is the safety property behind
// "log it and move on": giving up on a pack must never make data unreadable.
// The shard's loose objects are the record of truth until the .done marker
// exists, so an abandoned pack leaves a fully working - just unconsolidated -
// repository.
func TestAbandonedPackLeavesRepositoryReadable(t *testing.T) {
	mem := store.NewMemoryStore()
	st := &packFailStore{Store: mem}
	repo, err := CreateRepo(st, "pa", 1, 8)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := repo.CommitTxn(0, []Change{
		{Path: "/a.txt", Action: ActionAdd, Kind: KindFile, Data: []byte("alpha")},
	}, map[string]string{"svn:log": "one"}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	st.failPackUntilCleared.Store(true) // every pack attempt fails
	if _, err := repo.CommitTxn(1, []Change{
		{Path: "/b.txt", Action: ActionAdd, Kind: KindFile, Data: []byte("beta")},
	}, map[string]string{"svn:log": "two"}); err != nil {
		t.Fatalf("commit must succeed even though its pack will fail: %v", err)
	}
	st.failPackUntilCleared.Store(false)

	// Both revisions must read back intact from their loose objects.
	for _, tc := range []struct {
		rev  uint64
		path string
		want string
	}{
		{1, "/a.txt", "alpha"},
		{2, "/b.txt", "beta"},
	} {
		nr, rerr := repo.Resolve(tc.rev, tc.path)
		if rerr != nil {
			t.Fatalf("rev %d: resolve %s: %v", tc.rev, tc.path, rerr)
		}
		got, gerr := repo.ReadTextRange(nr, 0, -1)
		if gerr != nil {
			t.Fatalf("rev %d: read %s: %v", tc.rev, tc.path, gerr)
		}
		if string(got) != tc.want {
			t.Fatalf("rev %d: %s = %q, want %q", tc.rev, tc.path, got, tc.want)
		}
	}
}
