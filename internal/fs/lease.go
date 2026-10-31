package fs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
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
	ErrLeaseLost  = errors.New("fs: commit lease lost")

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
	// Payload is "<token> <unix-nanos>"; the token itself may contain dashes
	// and digits, so the timestamp is always the last field.
	ns, err := strconv.ParseInt(fields[len(fields)-1], 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.Unix(0, ns), true
}

// commitLease is an owned repository commit lease. It renews itself while the
// finalize runs, so a slow commit (large mirror upload, 30k files) cannot
// expire mid-flight and let another replica start writing the same revision.
type commitLease struct {
	key   string
	token string
	lost  atomic.Bool
	stop  chan struct{}
}

func (l *commitLease) payload() []byte {
	return []byte(fmt.Sprintf("%s %d", l.token, time.Now().UnixNano()))
}

// Verify fails once the lease was lost (expired, broken, or replaced).
func (l *commitLease) Verify(ctx context.Context, st store.Store) error {
	if l.lost.Load() {
		return ErrLeaseLost
	}
	cur, err := st.Get(ctx, l.key, nil)
	if err != nil {
		return ErrLeaseLost
	}
	if !strings.HasPrefix(string(cur), l.token+" ") {
		return ErrLeaseLost
	}
	return nil
}

func (l *commitLease) Release(st store.Store) {
	close(l.stop)
	releaseCommitLease(st, l.key, l.token)
}

func (l *commitLease) keepAlive(ctx context.Context, st store.Store) {
	interval := commitLeaseTTL / 3
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-ctx.Done():
			return
		case <-t.C:
			cur, err := st.Get(ctx, l.key, nil)
			if err != nil || !strings.HasPrefix(string(cur), l.token+" ") {
				l.lost.Store(true)
				return
			}
			// CAS keeps the same owner while refreshing the timestamp.
			if _, err := st.CAS(ctx, l.key, store.ContentETag(cur), l.payload()); err != nil {
				l.lost.Store(true)
				return
			}
		}
	}
}

// acquireCommitLease blocks until it owns the repository commit lease.
func (r *Repo) acquireCommitLease(ctx context.Context) (*commitLease, error) {
	key := r.keys.CommitLock()
	l := &commitLease{key: key, token: leaseToken(), stop: make(chan struct{})}
	payload := l.payload()
	deadline := time.Now().Add(commitLeaseWait)
	for {
		err := r.store.CreateIfAbsent(ctx, key, payload)
		if err == nil {
			go l.keepAlive(ctx, r.store)
			return l, nil
		}
		if !errors.Is(err, store.ErrPreconditionFailed) {
			return nil, err
		}
		if cur, gerr := r.store.Get(ctx, key, nil); gerr == nil {
			if ts, ok := leaseTimestamp(string(cur)); !ok || time.Since(ts) > commitLeaseTTL {
				r.breakCommitLease(ctx, key, l.token)
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
// marker is created atomically, the lock is re-read to confirm it is still
// stale, and only then conditionally deleted. A live holder that renewed in
// between must not be evicted. A marker left behind by a crashed breaker is
// itself broken after the same TTL.
func (r *Repo) breakCommitLease(ctx context.Context, key, token string) {
	marker := key + ".break"
	payload := []byte(fmt.Sprintf("%s %d", token, time.Now().UnixNano()))
	if err := r.store.CreateIfAbsent(ctx, marker, payload); err != nil {
		if cur, gerr := r.store.Get(ctx, marker, nil); gerr == nil {
			if ts, ok := leaseTimestamp(string(cur)); !ok || time.Since(ts) > commitLeaseTTL {
				_ = r.store.Delete(ctx, []string{marker})
			}
		}
		return
	}
	defer r.store.Delete(ctx, []string{marker})
	cur, gerr := r.store.Get(ctx, key, nil)
	if gerr != nil {
		return
	}
	if ts, ok := leaseTimestamp(string(cur)); ok && time.Since(ts) <= commitLeaseTTL {
		return // renewed by a live holder
	}
	_ = store.DeleteIfMatch(ctx, r.store, key, store.ContentETag(cur))
}

// releaseCommitLease removes the lease only if this caller still owns it.
// Conditional delete closes the window where a stale holder could otherwise
// delete a lock that another replica had just acquired.
func releaseCommitLease(st store.Store, key, token string) {
	ctx := context.Background()
	cur, err := st.Get(ctx, key, nil)
	if err != nil {
		return
	}
	if strings.HasPrefix(string(cur), token+" ") {
		_ = store.DeleteIfMatch(ctx, st, key, store.ContentETag(cur))
	}
}
