package fs

import (
	"context"
	"errors"
	"time"

	"svngod/internal/fs/store"
)

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
	const attempts = 8
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
		for i := 0; i < attempts; i++ {
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
	for i := 0; i < attempts; i++ {
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
