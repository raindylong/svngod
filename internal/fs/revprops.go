package fs

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"svngod/internal/fs/store"
)

// revpropRetryAttempts and revpropRetryBudget bound the CAS retry loop for
// revision-property updates.
//
// The loop used to retry a fixed 8 times with no wait. Measured through the real
// HTTP path on a 32-core host, 16 concurrent editors of one revision lost 4.4%
// of their edits and 32 lost 20%, each answered with HTTP 500 "revision property
// update failed" - an edit the client has no reason to resend. Losing a race is
// expected with N writers, but giving up after 8 immediate attempts is not a
// property of the data, it is a property of the retry policy.
//
// Two things change the shape of the failure:
//
//   - More attempts alone barely help: with 32 writers all retrying at once, the
//     loser of each round is re-decided at random, so a fixed small window still
//     rejects someone. Raising 8 to 64 only moves the cliff.
//   - A short randomized backoff breaks the lockstep. Once losers spread out,
//     each round has far fewer contenders and the loop converges quickly.
//
// The budget caps how long a caller may wait, so a pathological store cannot
// turn a revprop edit into a hang. It is a wall-clock limit, not a count: the
// measured point where it starts to bite is around 256 simultaneous writers of
// the SAME revision (2.7s, 37/1280 rejected); 128 succeeds in 1.3s and 64 well
// under that. Exhaustion still returns the contention error, which is honest -
// the edit genuinely did not apply.
const (
	revpropRetryAttempts = 64
	revpropRetryBudget   = 2 * time.Second
)

// revpropRetryPause sleeps before retry number attempt (0-based). It grows
// exponentially to a small cap and is jittered so concurrent losers do not
// retry in step. Returns false when the budget is spent.
func revpropRetryPause(start time.Time, attempt int) bool {
	if time.Since(start) > revpropRetryBudget {
		return false
	}
	// 1ms, 2ms, 4ms ... capped at 64ms, plus up to 1ms of jitter.
	d := time.Millisecond << uint(min(attempt, 6))
	if d > 64*time.Millisecond {
		d = 64 * time.Millisecond
	}
	jitter := time.Duration(rand.Int63n(int64(time.Millisecond) + 1))
	time.Sleep(d + jitter)
	return true
}

// UpdateRevProps applies property sets/removals to one revision. Loose
// revprops are updated with a CAS; packed shards rewrite their revprops pack
// with a CAS and invalidate the parsed cache. Concurrent changes to different
// properties retry instead of losing one writer's update.
func (r *Repo) UpdateRevProps(rev uint64, set map[string]string, remove []string) error {
	head, err := r.HeadFresh()
	if err != nil {
		return err
	}
	if rev > head {
		return errors.New("fs: revision does not exist")
	}
	// Segment repositories keep the committed properties inside immutable
	// segments; an edit is stored as an override object instead of rewriting
	// the segment (which many other revisions share).
	if r.segments && rev >= r.segmentFrom {
		return r.updateSegRevProps(rev, set, remove)
	}
	ctx := context.Background()
	sh := r.shard(rev)
	packed, err := r.isPacked(sh)
	if err != nil {
		return err
	}
	apply := func(props map[string]string) map[string]string {
		out := make(map[string]string, len(props)+len(set))
		for k, v := range props {
			out[k] = v
		}
		for k, v := range set {
			out[k] = v
		}
		for _, k := range remove {
			delete(out, k)
		}
		return out
	}
	// Mirror stores with a WAL serialize revprop changes with commits and
	// write the merged value to every reachable replica. Two processes that
	// elected different mirror primaries could otherwise both CAS the same
	// property and lose one update (observed: one property vanished while the
	// other read lagged by a round).
	if r.journal {
		lease, lerr := r.acquireCommitLease(ctx)
		if lerr != nil {
			return lerr
		}
		defer lease.Release(r.store)
	}
	start := time.Now()
	if packed {
		if r.journal {
			key := r.keys.PackedRevProps(sh)
			b, _, gerr := readForUpdate(ctx, r.store, key)
			if gerr != nil {
				return gerr
			}
			if b == nil {
				return errors.New("fs: revision props not found in pack")
			}
			m, perr := ParseRevPropsPack(b)
			if perr != nil {
				return perr
			}
			rp := m[rev]
			if rp == nil {
				return errors.New("fs: revision props not found in pack")
			}
			m[rev] = &RevProps{Props: apply(rp.Props)}
			if err := store.PutAll(ctx, r.store, key, PackRevProps(m)); err != nil {
				return err
			}
			r.invalidateRevPropsPack(sh)
			r.cacheRevProps(rev, m[rev])
			return nil
		}
		key := r.keys.PackedRevProps(sh)
		for i := 0; i < revpropRetryAttempts; i++ {
			if i > 0 && !revpropRetryPause(start, i) {
				break
			}
			b, gerr := r.store.Get(ctx, key, nil)
			if gerr != nil {
				return gerr
			}
			m, perr := ParseRevPropsPack(b)
			if perr != nil {
				return perr
			}
			rp := m[rev]
			if rp == nil {
				return errors.New("fs: revision props not found in pack")
			}
			m[rev] = &RevProps{Props: apply(rp.Props)}
			data := PackRevProps(m)
			if _, cerr := r.store.CAS(ctx, key, store.ContentETag(b), data); cerr == nil {
				r.invalidateRevPropsPack(sh)
				r.cacheRevProps(rev, m[rev])
				return nil
			} else if !errors.Is(cerr, store.ErrPreconditionFailed) {
				return cerr
			}
		}
		return errors.New("fs: revprop update contention")
	}

	if r.journal {
		key := r.keys.LooseRevProps(sh, rev)
		b, exists, gerr := readForUpdate(ctx, r.store, key)
		if gerr != nil {
			return gerr
		}
		cur := NewRevProps()
		if exists {
			cur, gerr = UnmarshalRevProps(b)
			if gerr != nil {
				return gerr
			}
		}
		next := &RevProps{Props: apply(cur.Props)}
		if err := store.PutAll(ctx, r.store, key, next.Marshal()); err != nil {
			return err
		}
		r.cacheRevProps(rev, next)
		return nil
	}

	key := r.keys.LooseRevProps(sh, rev)
	for i := 0; i < revpropRetryAttempts; i++ {
		if i > 0 && !revpropRetryPause(start, i) {
			break
		}
		b, gerr := r.store.Get(ctx, key, nil)
		exists := gerr == nil
		var cur *RevProps
		if exists {
			cur, gerr = UnmarshalRevProps(b)
			if gerr != nil {
				return gerr
			}
		} else if errors.Is(gerr, store.ErrNotFound) {
			cur = NewRevProps()
		} else {
			return gerr
		}
		next := &RevProps{Props: apply(cur.Props)}
		data := next.Marshal()
		if exists {
			_, gerr = r.store.CAS(ctx, key, store.ContentETag(b), data)
		} else {
			gerr = r.store.CreateIfAbsent(ctx, key, data)
		}
		if gerr == nil {
			r.cacheRevProps(rev, next)
			return nil
		}
		if !errors.Is(gerr, store.ErrPreconditionFailed) {
			return gerr
		}
	}
	return errors.New("fs: revprop update contention")
}

// readForUpdate reads the freshest copy of a mutable object: the value held by
// the most replicas when supported, else the elected primary's copy.
func readForUpdate(ctx context.Context, s store.Store, key string) ([]byte, bool, error) {
	if qg, ok := s.(store.QuorumGetter); ok {
		b, n, err := qg.QuorumGet(ctx, key)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return nil, false, nil
			}
			return nil, false, err
		}
		return b, n > 0, nil
	}
	b, err := s.Get(ctx, key, nil)
	if errors.Is(err, store.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

// getFreshObject reads an object from the most replicas when the store
// supports it, so a lagging primary cannot hide a revprop update made through
// another svngod process.
func getFreshObject(ctx context.Context, s store.Store, key string) ([]byte, error) {
	if qg, ok := s.(store.QuorumGetter); ok {
		b, n, err := qg.QuorumGet(ctx, key)
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, store.ErrNotFound
		}
		return b, nil
	}
	return s.Get(ctx, key, nil)
}

func (r *Repo) cacheRevProps(rev uint64, rp *RevProps) {
	r.revPropsMu.Lock()
	defer r.revPropsMu.Unlock()
	if r.revProps == nil {
		r.revProps = make(map[uint64]*RevProps)
		r.revPropsAt = make(map[uint64]time.Time)
	}
	if len(r.revProps) >= revPropsCacheMax {
		r.revProps = make(map[uint64]*RevProps)
		r.revPropsAt = make(map[uint64]time.Time)
	}
	r.revProps[rev] = rp
	r.revPropsAt[rev] = time.Now()
}
