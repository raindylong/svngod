package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

// WriteQuorum reports the configured number of copies (0 = best effort).
func (m *MirrorStore) WriteQuorum() int { return m.writeQuorum }

// RegisterQuorum returns the number of copies a commit-lease operation must
// reach. A two-node pair uses the arbiter rule (one copy on the configured
// first mirror is safe because a second writer cannot commit without that
// mirror). Three or more nodes require a strict majority, so any two writers'
// quorums intersect and cannot both hold the lease.
func (m *MirrorStore) RegisterQuorum() int {
	m.mu.Lock()
	n := len(m.mirrors)
	m.mu.Unlock()
	if n <= 2 {
		return 1
	}
	return n/2 + 1
}

// ArbiterRequired reports whether register operations must always target the
// configured first mirror. Only the two-node pair needs that rule; with a
// majority quorum a health-view difference cannot produce two disjoint sets.
func (m *MirrorStore) ArbiterRequired() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.mirrors) <= 2
}

func (m *MirrorStore) noteAcked(b []byte) {
	m.mu.Lock()
	// The acknowledged HEAD is a high-water mark: a partial repair view (the
	// newer replica unreachable, an older one still up) must never lower it.
	// Doing so cleared fencing on a lagging node and let it commit at an old
	// revision number, overwriting a different revision's objects (observed
	// at 800k objects: rev728 existed with two contents).
	if m.mutableOrder != nil && m.controlKey != "" && len(m.ackedCurrent) > 0 &&
		m.mutableOrder(m.controlKey, b, m.ackedCurrent) < 0 {
		m.mu.Unlock()
		return
	}
	m.ackedCurrent = append([]byte(nil), b...)
	m.fenced = false
	m.mu.Unlock()
}

func (m *MirrorStore) ackedCopy() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]byte(nil), m.ackedCurrent...)
}

// quorumWriteTimeout bounds one standby copy in quorum mode. It is larger
// than the best-effort budget because the copy is now required for the write
// to be acknowledged (a 100MiB body over a LAN takes a couple of seconds).
const quorumWriteTimeout = 15 * time.Second

// quorumWriteGrace is how long past a single copy's budget the aggregate wait
// allows before giving up on stragglers. It absorbs scheduling jitter without
// extending the commit path's worst case by more than a few seconds.
const quorumWriteGrace = 3 * time.Second

// quorumRegisterTimeout bounds one replica operation on a lease-sized object
// (the commit lock is a few dozen bytes). It is shorter than the data budget
// so a hung-but-still-"healthy" replica costs seconds, not a failed commit.
const quorumRegisterTimeout = 5 * time.Second

// quorumMirror writes fn to the healthy standbys after the primary has the
// object, returning ErrQuorumUnmet unless at least quorum copies exist. When
// track is set, a skipped or failed copy is recorded in the mirror's pending
// delta so recovery copies it even if it raced a repair pass.
func (m *MirrorStore) quorumMirror(ctx context.Context, pi, quorum int, key string, track bool, fn func(context.Context, Store) error) error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok := 0
	acked := map[int]bool{} // per-index, so a straggler can be identified below
	for i, s := range m.mirrors {
		if i == pi {
			continue
		}
		if track {
			if !m.routeStandby(i, key) {
				continue
			}
		} else if !m.isHealthy(i) {
			continue
		}
		wg.Add(1)
		go func(i int, s Store) {
			defer wg.Done()
			wctx, cancel := context.WithTimeout(ctx, quorumWriteTimeout)
			defer cancel()
			if err := fn(wctx, s); err != nil {
				if track {
					m.skipWrite(i, key)
				}
				m.markDown(i)
				m.noteError(i, err)
				return
			}
			m.noteWrite(i)
			mu.Lock()
			ok++
			acked[i] = true
			mu.Unlock()
		}(i, s)
	}
	// wg.Wait() alone is not a bound: the per-copy context only works if fn
	// honours cancellation, and a backend that blocks in a syscall or an SDK
	// call ignoring ctx makes WithTimeout a no-op. Waiting on it then holds the
	// commit lease forever and every later commit queues behind it. Give up on
	// the stragglers and report what we actually have - a copy that has not
	// acked is not an acked copy.
	//
	// The wait is one copy timeout longer than a single copy's budget so a
	// merely slow-but-honest replica is not cut off early.
	waitDone := make(chan struct{})
	go func() { wg.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-time.After(quorumWriteTimeout + quorumWriteGrace):
		// A replica still blocked after its own budget plus grace is treated as
		// hung, the same way a timed-out health probe is: markDownHung also
		// forces the next repair to enumerate objects instead of trusting the
		// pending delta, because a write "into the socket" may never have been
		// processed.
		mu.Lock()
		pending := make([]int, 0, len(m.mirrors))
		for i := range m.mirrors {
			if i != pi && !acked[i] {
				pending = append(pending, i)
			}
		}
		mu.Unlock()
		for _, i := range pending {
			m.markDownHung(i)
			m.noteError(i, fmt.Errorf("store: replica %d did not acknowledge within %v",
				i, quorumWriteTimeout+quorumWriteGrace))
		}
	}
	// Snapshot under the lock: on the timeout path the goroutines are still
	// running and will keep incrementing ok/acked, so reading them bare would be
	// a data race (and could report a count that never held at one instant).
	mu.Lock()
	got := 1 + ok
	mu.Unlock()
	if got < quorum {
		return fmt.Errorf("%w: %d of %d copies (need %d)", ErrQuorumUnmet, got, len(m.mirrors), quorum)
	}
	return nil
}

// PutAll writes to the primary and every healthy standby, requiring all of
// them. Revision-property updates use it so two processes cannot leave the
// replicas with different copies of the same mutable object.
func (m *MirrorStore) PutAll(ctx context.Context, key string, data []byte) error {
	if isRevisionKey(key) && m.ArbiterRequired() {
		return m.putRevisionAll(ctx, key, data)
	}
	n := m.LiveCount()
	if n <= 1 {
		return m.Put(ctx, key, data)
	}
	pi, err := m.writePrimary(func(s Store) error { return s.Put(ctx, key, data) })
	if err != nil {
		return err
	}
	return m.quorumMirror(ctx, pi, n, key, true, func(ctx context.Context, s Store) error {
		return s.Put(ctx, key, data)
	})
}

// putRevisionAll writes a revision object to the arbiter and every live
// standby, requiring each of them. Revision-property updates use it so two
// processes cannot leave the replicas with different bytes for the same
// revision.
func (m *MirrorStore) putRevisionAll(ctx context.Context, key string, data []byte) error {
	p, pi := m.primary()
	if err := m.checkCaughtUp(p, pi); err != nil {
		return err
	}
	wrote := 0
	for i, s := range m.mirrors {
		if i != 0 && !m.isHealthy(i) {
			continue
		}
		c, cancel := context.WithTimeout(ctx, quorumWriteTimeout)
		err := s.Put(c, key, data)
		cancel()
		if err != nil {
			m.markDown(i)
			m.noteError(i, err)
			m.skipWrite(i, key)
			if i == 0 {
				return fmt.Errorf("%w: arbiter unavailable: %v", ErrQuorumUnmet, err)
			}
			return err
		}
		m.noteWrite(i)
		wrote++
	}
	if wrote == 0 {
		return errors.New("mirror: no replica accepted the revision")
	}
	return nil
}

// QuorumGet returns the value held by the largest number of mirrors and that
// count. It is used to replay the commit journal after a crash: a record only
// counts when a quorum holds the same bytes.
func (m *MirrorStore) QuorumGet(ctx context.Context, key string) ([]byte, int, error) {
	type cand struct {
		val []byte
		n   int
	}
	var best *cand
	for i, s := range m.mirrors {
		// An out-of-service replica cannot contribute a copy to the count and
		// must not add its retry budget to every quorum read (WAL replay runs
		// on the read path). A stale healthy flag still costs one timeout.
		if !m.isHealthy(i) {
			continue
		}
		c, cancel := context.WithTimeout(ctx, quorumReadTimeout)
		b, err := s.Get(c, key, nil)
		cancel()
		if err != nil {
			continue
		}
		if best == nil {
			best = &cand{val: b, n: 1}
			continue
		}
		if bytes.Equal(best.val, b) {
			best.n++
		}
	}
	if best == nil {
		return nil, 0, ErrNotFound
	}
	return best.val, best.n, nil
}

// ArbiterGet reads key from the configured first mirror. Lease state lives
// there even when the replica is marked down: another process may still be
// using it as its arbiter, and failing over to another node for register
// operations would let both hold a lease and publish the same revision.
func (m *MirrorStore) ArbiterGet(ctx context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	a := m.mirrors[0]
	m.mu.Unlock()
	return a.Get(ctx, key, nil)
}

// GetRepairing returns the first replica copy of key that passes verify and
// overwrites the corrupt copies it observed. Used when a read fails its
// content checksum: without this a single bit flip on one replica stayed
// forever and a failover turned it into an unreadable revision.
func (m *MirrorStore) GetRepairing(ctx context.Context, key string, verify func([]byte) bool) ([]byte, error) {
	m.mu.Lock()
	mirrors := append([]Store(nil), m.mirrors...)
	healthy := append([]bool(nil), m.healthy...)
	m.mu.Unlock()
	var bad []int
	var lastErr error
	for i, s := range mirrors {
		if !healthy[i] {
			continue
		}
		c, cancel := context.WithTimeout(ctx, standbyReadTimeout)
		b, err := s.Get(c, key, nil)
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		if verify != nil && !verify(b) {
			bad = append(bad, i)
			lastErr = fmt.Errorf("store: replica %d holds an invalid %q", i, key)
			continue
		}
		for _, j := range bad {
			wc, wcancel := context.WithTimeout(context.Background(), standbyWriteTimeout)
			err := mirrors[j].Put(wc, key, b)
			wcancel()
			if err == nil {
				m.noteWrite(j)
			} else {
				m.noteError(j, err)
			}
		}
		if len(bad) > 0 {
			log.Printf("store: healed %q from replica %d (%d corrupt copies)", key, i, len(bad))
		}
		return b, nil
	}
	if lastErr == nil {
		lastErr = ErrNotFound
	}
	return nil, lastErr
}

// quorumReadTimeout bounds one replica read in QuorumGet. It is shorter than
// the write budget because a read has no durable side effect and a slow
// replica can be excluded from the count instead of holding the request.
const quorumReadTimeout = 5 * time.Second

// standbyWriteTimeout bounds how long a commit waits for one standby. It is
// far above a healthy write (tens of ms locally) but short enough that a dead
// node costs a few seconds, not the S3 SDK's full retry budget.
const standbyWriteTimeout = 8 * time.Second

// Read budgets bound a replica that accepts a request but never answers (a
// paused process, a black-holed path). Without them a commit that falls back
// to read from such a replica hung for minutes; the request only completed when
// the peer was resumed.
const (
	primaryReadTimeout = 60 * time.Second
	standbyReadTimeout = 10 * time.Second
	// startupProbeTimeout bounds the reachability check NewMirror runs before
	// its blocking alignment pass. A down backend must delay startup by
	// seconds, not by a full list/repair timeout.
	startupProbeTimeout = 5 * time.Second
)

// CreateIfAbsentQuorum creates the key on a quorum. Replicas are tried in
// configuration order: the first reachable node is the arbiter, so two
// processes cannot each win on a different node and then roll each other back
// (that livelocked concurrent commits). A node that already holds the key
// stops the attempt immediately.
func (m *MirrorStore) CreateIfAbsentQuorum(ctx context.Context, key string, data []byte, quorum int) error {
	etag := ContentETag(data)
	var created []int
	rollback := func() {
		for _, i := range created {
			c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = DeleteIfMatch(c, m.mirrors[i], key, etag)
			cancel()
		}
	}
	arbiter := m.ArbiterRequired()
	for i, s := range m.mirrors {
		// Two-node pair: the first mirror is the arbiter and is always
		// attempted, even when this process marked it down (skipping it once
		// let two processes create leases on different mirrors and publish the
		// same revision). Three or more nodes use a majority quorum instead;
		// any two majorities intersect, so unhealthy nodes may be skipped.
		if i == 0 {
			if !arbiter && !m.isHealthy(0) {
				continue
			}
		} else if !m.isHealthy(i) {
			continue
		}
		c, cancel := context.WithTimeout(ctx, quorumRegisterTimeout)
		err := s.CreateIfAbsent(c, key, data)
		cancel()
		switch {
		case err == nil:
			created = append(created, i)
			m.noteWrite(i)
			if len(created) >= quorum {
				return nil
			}
		case errors.Is(err, ErrPreconditionFailed):
			rollback()
			return ErrPreconditionFailed
		default:
			m.markDown(i)
			m.noteError(i, err)
			if i == 0 && arbiter {
				// Lease state is only authoritative on the arbiter; using
				// another replica would reopen the split-lease window.
				rollback()
				return fmt.Errorf("%w: arbiter unavailable: %v", ErrQuorumUnmet, err)
			}
		}
	}
	if len(created) < quorum {
		rollback()
		return fmt.Errorf("%w: %d of %d created (need %d)", ErrQuorumUnmet, len(created), len(m.mirrors), quorum)
	}
	return nil
}

// CASQuorum applies the CAS in configuration order and requires quorum
// successes. A mismatch is terminal (the register diverged); successes are
// rolled back so the register is never half-updated.
func (m *MirrorStore) CASQuorum(ctx context.Context, key, expect string, data []byte, quorum int) error {
	etag := ContentETag(data)
	var done []int
	rollback := func() {
		for _, i := range done {
			c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = DeleteIfMatch(c, m.mirrors[i], key, etag)
			cancel()
		}
	}
	arbiter := m.ArbiterRequired()
	for i, s := range m.mirrors {
		if i == 0 {
			if !arbiter && !m.isHealthy(0) {
				continue
			}
		} else if !m.isHealthy(i) {
			continue
		}
		c, cancel := context.WithTimeout(ctx, quorumRegisterTimeout)
		_, err := s.CAS(c, key, expect, data)
		cancel()
		switch {
		case err == nil:
			done = append(done, i)
			m.noteWrite(i)
			if len(done) >= quorum {
				return nil
			}
		case errors.Is(err, ErrPreconditionFailed):
			rollback()
			return ErrPreconditionFailed
		default:
			m.markDown(i)
			m.noteError(i, err)
			if i == 0 && arbiter {
				rollback()
				return fmt.Errorf("%w: arbiter unavailable: %v", ErrQuorumUnmet, err)
			}
		}
	}
	if len(done) < quorum {
		rollback()
		return fmt.Errorf("%w: %d of %d cas (need %d)", ErrQuorumUnmet, len(done), len(m.mirrors), quorum)
	}
	return nil
}

// DeleteIfMatchQuorum deletes in configuration order and requires quorum
// successes (an already-gone replica counts as released).
func (m *MirrorStore) DeleteIfMatchQuorum(ctx context.Context, key, etag string, quorum int) error {
	ok := 0
	arbiter := m.ArbiterRequired()
	for i, s := range m.mirrors {
		if i == 0 {
			if !arbiter && !m.isHealthy(0) {
				continue
			}
		} else if !m.isHealthy(i) {
			continue
		}
		c, cancel := context.WithTimeout(ctx, quorumRegisterTimeout)
		err := DeleteIfMatch(c, s, key, etag)
		cancel()
		switch {
		case err == nil, errors.Is(err, ErrPreconditionFailed), errors.Is(err, ErrNotFound):
			ok++
			m.noteWrite(i)
			if ok >= quorum {
				return nil
			}
		default:
			m.markDown(i)
			m.noteError(i, err)
			if i == 0 && arbiter {
				return fmt.Errorf("%w: arbiter unavailable: %v", ErrQuorumUnmet, err)
			}
		}
	}
	if ok < quorum {
		return fmt.Errorf("%w: %d of %d deleted (need %d)", ErrQuorumUnmet, ok, len(m.mirrors), quorum)
	}
	return nil
}
