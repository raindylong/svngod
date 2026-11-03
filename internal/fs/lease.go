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

	// Nanoseconds, accessed atomically: tests retune the timings while a
	// background heartbeat from another lease may still be reading them.
	commitLeaseTTL  atomic.Int64
	commitLeaseWait atomic.Int64
	commitLeasePoll atomic.Int64
)

func init() {
	commitLeaseTTL.Store(int64(20 * time.Second))
	commitLeaseWait.Store(int64(30 * time.Second))
	commitLeasePoll.Store(int64(50 * time.Millisecond))
}

func leaseTTL() time.Duration  { return time.Duration(commitLeaseTTL.Load()) }
func leaseWait() time.Duration { return time.Duration(commitLeaseWait.Load()) }
func leasePoll() time.Duration { return time.Duration(commitLeasePoll.Load()) }

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
	// qr/quorum are set when the repository uses a quorum-replicated store:
	// the lease must then be held on a majority, not on one elected primary,
	// so two processes with different primary views cannot both commit.
	qr     store.QuorumRegister
	quorum int
	// dynamic means "use the number of currently reachable replicas" instead
	// of a fixed quorum (availability mode). A node outage must not block
	// commits, but while both nodes are up their lease copies must agree.
	dynamic bool
}

func (l *commitLease) useQuorum() bool { return l.qr != nil }

// effectiveQuorum is the number of replicas a lease operation must reach.
func (l *commitLease) effectiveQuorum() int {
	if !l.dynamic {
		return l.quorum
	}
	if lc, ok := l.qr.(interface{ LiveCount() int }); ok {
		if n := lc.LiveCount(); n > 0 {
			return n
		}
	}
	return 1
}

func (l *commitLease) payload() []byte {
	return []byte(fmt.Sprintf("%s %d", l.token, time.Now().UnixNano()))
}

// readOwn returns the current lease payload. Lease state is authoritative on
// the arbiter mirror; reading another replica after a failover could confirm a
// stale lease (two processes once held "the" lease on different mirrors).
func (l *commitLease) readOwn(ctx context.Context, st store.Store) ([]byte, error) {
	if l.useQuorum() {
		if ar, ok := l.qr.(store.ArbiterReader); ok {
			return ar.ArbiterGet(ctx, l.key)
		}
		b, count, err := l.qr.QuorumGet(ctx, l.key)
		if err != nil || count < l.effectiveQuorum() {
			return nil, ErrLeaseLost
		}
		return b, nil
	}
	return st.Get(ctx, l.key, nil)
}

// Verify fails once the lease was lost (expired, broken, or replaced).
func (l *commitLease) Verify(ctx context.Context, st store.Store) error {
	if l.lost.Load() {
		return ErrLeaseLost
	}
	cur, err := l.readOwn(ctx, st)
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
	if l.useQuorum() {
		releaseCommitLeaseQuorum(l.qr, l.key, l.token, l.effectiveQuorum())
		return
	}
	releaseCommitLease(st, l.key, l.token)
}

func (l *commitLease) keepAlive(ctx context.Context, st store.Store) {
	interval := leaseTTL() / 3
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
			cur, err := l.readOwn(ctx, st)
			if err != nil || !strings.HasPrefix(string(cur), l.token+" ") {
				l.lost.Store(true)
				return
			}
			if l.useQuorum() {
				if err := l.qr.CASQuorum(ctx, l.key, store.ContentETag(cur), l.payload(), l.effectiveQuorum()); err != nil {
					l.lost.Store(true)
					return
				}
				continue
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
	if qr, ok := r.store.(store.QuorumRegister); ok {
		l.qr, l.quorum = qr, r.writeQuorum
		l.dynamic = r.writeQuorum <= 1
	}
	deadline := time.Now().Add(leaseWait())
	for {
		err := r.createLease(ctx, key, payload)
		if err == nil {
			go l.keepAlive(ctx, r.store)
			return l, nil
		}
		if errors.Is(err, store.ErrQuorumUnmet) {
			// Fewer than a quorum of replicas are reachable: waiting cannot
			// help, and refusing now keeps the commit from hanging.
			return nil, err
		}
		if !errors.Is(err, store.ErrPreconditionFailed) {
			return nil, err
		}
		if cur, ok := r.readLease(ctx, key); ok {
			if ts, ok := leaseTimestamp(string(cur)); !ok || time.Since(ts) > leaseTTL() {
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
		case <-time.After(leasePoll()):
		}
	}
}

// createLease creates the lease on a quorum when configured, or on the single
// store otherwise.
func (r *Repo) effectiveQuorum() int {
	if r.writeQuorum > 1 {
		return r.writeQuorum
	}
	if lc, ok := r.store.(interface{ LiveCount() int }); ok {
		if n := lc.LiveCount(); n > 0 {
			return n
		}
	}
	return 1
}

func (r *Repo) createLease(ctx context.Context, key string, payload []byte) error {
	if qr, ok := r.store.(store.QuorumRegister); ok {
		return qr.CreateIfAbsentQuorum(ctx, key, payload, r.effectiveQuorum())
	}
	return r.store.CreateIfAbsent(ctx, key, payload)
}

// readLease returns the lease payload from the arbiter (or the single store).
func (r *Repo) readLease(ctx context.Context, key string) ([]byte, bool) {
	if qr, ok := r.store.(store.ArbiterReader); ok {
		b, err := qr.ArbiterGet(ctx, key)
		return b, err == nil
	}
	if qr, ok := r.store.(store.QuorumRegister); ok {
		b, count, err := qr.QuorumGet(ctx, key)
		return b, err == nil && count >= 1
	}
	b, err := r.store.Get(ctx, key, nil)
	return b, err == nil
}

// breakCommitLease lets exactly one waiter remove a stale lease: the breaker
// marker is created atomically, the lock is re-read to confirm it is still
// stale, and only then conditionally deleted. A live holder that renewed in
// between must not be evicted. A marker left behind by a crashed breaker is
// itself broken after the same TTL.
func (r *Repo) breakCommitLease(ctx context.Context, key, token string) {
	marker := key + ".break"
	payload := []byte(fmt.Sprintf("%s %d", token, time.Now().UnixNano()))
	created := false
	if qr, ok := r.store.(store.QuorumRegister); ok {
		q := r.writeQuorum
		if q < 1 {
			q = r.effectiveQuorum()
		}
		created = qr.CreateIfAbsentQuorum(ctx, marker, payload, q) == nil
	} else {
		created = r.store.CreateIfAbsent(ctx, marker, payload) == nil
	}
	if !created {
		if cur, ok := r.readLease(ctx, marker); ok {
			if ts, ok := leaseTimestamp(string(cur)); !ok || time.Since(ts) > leaseTTL() {
				if qr, ok := r.store.(store.QuorumRegister); ok {
					_ = qr.DeleteIfMatchQuorum(ctx, marker, store.ContentETag(cur), r.effectiveQuorum())
				} else {
					_ = r.store.Delete(ctx, []string{marker})
				}
			}
		}
		return
	}
	defer func() {
		if qr, ok := r.store.(store.QuorumRegister); ok {
			if cur, ok := r.readLease(ctx, marker); ok {
				_ = qr.DeleteIfMatchQuorum(ctx, marker, store.ContentETag(cur), r.effectiveQuorum())
			}
		} else {
			_ = r.store.Delete(ctx, []string{marker})
		}
	}()
	cur, ok := r.readLease(ctx, key)
	if !ok {
		return
	}
	if ts, ok := leaseTimestamp(string(cur)); ok && time.Since(ts) <= leaseTTL() {
		return // renewed by a live holder
	}
	if qr, ok := r.store.(store.QuorumRegister); ok {
		_ = qr.DeleteIfMatchQuorum(ctx, key, store.ContentETag(cur), r.effectiveQuorum())
		return
	}
	_ = store.DeleteIfMatch(ctx, r.store, key, store.ContentETag(cur))
}

// releaseCommitLease removes the lease only if this caller still owns it.
// Conditional delete closes the window where a stale holder could otherwise
// delete a lock that another replica had just acquired.
// releaseCommitLeaseQuorum releases a quorum lease only where this caller
// still owns it. A partial release leaves the lease to expire by TTL.
func releaseCommitLeaseQuorum(qr store.QuorumRegister, key, token string, quorum int) {
	ctx := context.Background()
	cur, count, err := qr.QuorumGet(ctx, key)
	if err != nil || count < quorum || !strings.HasPrefix(string(cur), token+" ") {
		return
	}
	_ = qr.DeleteIfMatchQuorum(ctx, key, store.ContentETag(cur), quorum)
}

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
