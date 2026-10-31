package fs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"svngod/internal/fs/store"
)

// A commit lease serialises the finalize step across server processes that
// share one object store. r.mu already serialises commits inside a process,
// but two replicas can otherwise both write the same revision block before
// the `current` CAS resolves the winner, letting the loser overwrite the
// winner's revision. The lease is a single object under the repository:
// acquired with CreateIfAbsent, released by its owner, and broken after a TTL
// by a single elected breaker so a crashed holder cannot block commits.
var (
	ErrCommitBusy = errors.New("fs: another commit is in progress")

	commitLeaseTTL  = 2 * time.Minute
	commitLeaseWait = 30 * time.Second
	commitLeasePoll = 50 * time.Millisecond
)

func leaseToken() string {
	var b [8]byte
	rand.Read(b[:])
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), hex.EncodeToString(b[:]))
}

func leaseTimestamp(payload string) (time.Time, bool) {
	fields := strings.Fields(payload)
	if len(fields) == 0 {
		return time.Time{}, false
	}
	ns, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.Unix(0, ns), true
}

// acquireCommitLease blocks until it owns the repository commit lease.
func (r *Repo) acquireCommitLease(ctx context.Context) (func(), error) {
	key := r.keys.CommitLock()
	token := leaseToken()
	payload := []byte(fmt.Sprintf("%s %d", token, time.Now().UnixNano()))
	deadline := time.Now().Add(commitLeaseWait)
	for {
		err := r.store.CreateIfAbsent(ctx, key, payload)
		if err == nil {
			return func() { r.releaseCommitLease(key, token) }, nil
		}
		if !errors.Is(err, store.ErrPreconditionFailed) {
			return nil, err
		}
		if cur, gerr := r.store.Get(ctx, key, nil); gerr == nil {
			if ts, ok := leaseTimestamp(string(cur)); !ok || time.Since(ts) > commitLeaseTTL {
				r.breakCommitLease(ctx, key, token)
				continue
			}
		}
		if time.Now().After(deadline) {
			return nil, ErrCommitBusy
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(commitLeasePoll):
		}
	}
}

// breakCommitLease lets exactly one waiter remove a stale lease: the breaker
// marker is created atomically, then the stale lock is deleted.
func (r *Repo) breakCommitLease(ctx context.Context, key, token string) {
	marker := key + ".break"
	payload := []byte(fmt.Sprintf("%s %d", token, time.Now().UnixNano()))
	if err := r.store.CreateIfAbsent(ctx, marker, payload); err != nil {
		return
	}
	_ = r.store.Delete(ctx, []string{key})
	_ = r.store.Delete(ctx, []string{marker})
}

// releaseCommitLease removes the lease only if this caller still owns it.
func (r *Repo) releaseCommitLease(key, token string) {
	ctx := context.Background()
	cur, err := r.store.Get(ctx, key, nil)
	if err != nil {
		return
	}
	if strings.HasPrefix(string(cur), token+" ") {
		_ = r.store.Delete(ctx, []string{key})
	}
}
