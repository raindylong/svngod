package fs

import (
	"context"
	"errors"

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
	const attempts = 8
	if packed {
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

func (r *Repo) cacheRevProps(rev uint64, rp *RevProps) {
	r.revPropsMu.Lock()
	defer r.revPropsMu.Unlock()
	if r.revProps == nil {
		r.revProps = make(map[uint64]*RevProps)
	}
	if len(r.revProps) >= revPropsCacheMax {
		r.revProps = make(map[uint64]*RevProps)
	}
	r.revProps[rev] = rp
}