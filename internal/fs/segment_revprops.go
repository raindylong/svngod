package fs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"svngod/internal/fs/store"
)

// Revision properties are mutable while segment objects are immutable. In a
// format-v2 repository the committed properties live inside the segment, so an
// edit cannot rewrite them in place; it is stored as an override instead:
//
//	seg-revprops/<shard>/<rev>   full effective property set for that revision
//	seg-revprops/<shard>.idx     which revisions of the shard have an override
//
// The index keeps reads cheap: a revision without an override needs no extra
// request, and the small list is cached for the same second as the parsed
// revprops themselves. Overrides are written before the index entry, so a crash
// in between leaves an invisible object instead of a listed-but-missing one.

// segRevPropsOverrides returns the overridden revisions of a shard, cached for
// revPropsCacheTTL. fresh bypasses the cache (used by GetRevPropsFresh).
func (r *Repo) segRevPropsOverrides(sh uint64, fresh bool) (map[uint64]bool, error) {
	r.segOverrideMu.Lock()
	if !fresh {
		if m := r.segOverrides[sh]; m != nil && time.Since(r.segOverrideAt[sh]) < revPropsCacheTTL {
			r.segOverrideMu.Unlock()
			return m, nil
		}
	}
	r.segOverrideMu.Unlock()

	b, err := getFreshObject(context.Background(), r.store, r.keys.SegRevPropsIndex(sh))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			m := map[uint64]bool{}
			r.putSegOverrides(sh, m)
			return m, nil
		}
		return nil, err
	}
	var revs []uint64
	if len(b) > 0 {
		if err := json.Unmarshal(b, &revs); err != nil {
			return nil, fmt.Errorf("fs: segment revprop index: %w", err)
		}
	}
	m := make(map[uint64]bool, len(revs))
	for _, rev := range revs {
		m[rev] = true
	}
	r.putSegOverrides(sh, m)
	return m, nil
}

func (r *Repo) putSegOverrides(sh uint64, m map[uint64]bool) {
	r.segOverrideMu.Lock()
	defer r.segOverrideMu.Unlock()
	if r.segOverrides == nil {
		r.segOverrides = map[uint64]map[uint64]bool{}
		r.segOverrideAt = map[uint64]time.Time{}
	}
	r.segOverrides[sh] = m
	r.segOverrideAt[sh] = time.Now()
}

func (r *Repo) invalidateSegOverrides(sh uint64) {
	r.segOverrideMu.Lock()
	defer r.segOverrideMu.Unlock()
	delete(r.segOverrides, sh)
	delete(r.segOverrideAt, sh)
}

// loadSegRevPropsRaw returns the override object's bytes, or exists=false.
func (r *Repo) loadSegRevPropsRaw(ctx context.Context, sh, rev uint64) ([]byte, bool, error) {
	b, err := getFreshObject(ctx, r.store, r.keys.SegRevPropsOverride(sh, rev))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return b, true, nil
}

// segRevProps returns the effective properties of a revision, preferring the
// override. ok=false means no override exists and the caller must read the
// segment.
func (r *Repo) segRevProps(rev uint64, fresh bool) (*RevProps, bool, error) {
	sh := r.shard(rev)
	revs, err := r.segRevPropsOverrides(sh, fresh)
	if err != nil {
		return nil, false, err
	}
	if !revs[rev] && !fresh {
		return nil, false, nil
	}
	// A fresh read must not trust the cached list: another process may have
	// created the override just now, so it always asks for the object.
	b, ok, err := r.loadSegRevPropsRaw(context.Background(), sh, rev)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		if revs[rev] {
			// The list says this revision was edited but the object is gone.
			// Falling back to the committed value would silently revert the
			// edit, so report the loss instead.
			return nil, false, fmt.Errorf("fs: revision %d property override is missing", rev)
		}
		return nil, false, nil
	}
	rp, err := UnmarshalRevProps(b)
	if err != nil {
		return nil, false, err
	}
	return rp, true, nil
}

// updateSegRevProps applies set/remove to a revision stored in segments.
func (r *Repo) updateSegRevProps(rev uint64, set map[string]string, remove []string) error {
	ctx := context.Background()
	sh := r.shard(rev)
	key := r.keys.SegRevPropsOverride(sh, rev)
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
	for i := 0; i < attempts; i++ {
		raw, exists, err := r.loadSegRevPropsRaw(ctx, sh, rev)
		if err != nil {
			return err
		}
		var cur *RevProps
		if exists {
			cur, err = UnmarshalRevProps(raw)
			if err != nil {
				return err
			}
		} else {
			// First override: start from the committed properties.
			_, props, serr := r.segmentEntry(rev, false)
			if serr != nil {
				return serr
			}
			cur, err = UnmarshalRevProps(props)
			if err != nil {
				return err
			}
		}
		next := &RevProps{Props: apply(cur.Props)}
		data := next.Marshal()
		if exists {
			_, err = r.store.CAS(ctx, key, store.ContentETag(raw), data)
		} else {
			err = r.store.CreateIfAbsent(ctx, key, data)
		}
		if err == nil {
			if lerr := r.addSegRevPropsIndex(sh, rev); lerr != nil {
				return lerr
			}
			r.cacheRevProps(rev, next)
			return nil
		}
		if !errors.Is(err, store.ErrPreconditionFailed) {
			return err
		}
	}
	return errors.New("fs: revprop update contention")
}

// addSegRevPropsIndex records rev in the shard's override list with a CAS loop.
func (r *Repo) addSegRevPropsIndex(sh, rev uint64) error {
	ctx := context.Background()
	key := r.keys.SegRevPropsIndex(sh)
	for i := 0; i < 8; i++ {
		b, err := r.store.Get(ctx, key, nil)
		exists := err == nil
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
		var revs []uint64
		if exists && len(b) > 0 {
			if err := json.Unmarshal(b, &revs); err != nil {
				return fmt.Errorf("fs: segment revprop index: %w", err)
			}
		}
		for _, x := range revs {
			if x == rev {
				r.invalidateSegOverrides(sh)
				return nil
			}
		}
		revs = append(revs, rev)
		sort.Slice(revs, func(i, j int) bool { return revs[i] < revs[j] })
		// At most one entry per revision can exist, so this is a sanity guard
		// against a corrupted list rather than a practical limit.
		if uint64(len(revs)) > r.shardSz {
			return fmt.Errorf("fs: shard %d lists %d overridden revisions (shard holds %d)", sh, len(revs), r.shardSz)
		}
		data, merr := json.Marshal(revs)
		if merr != nil {
			return merr
		}
		if exists {
			_, err = r.store.CAS(ctx, key, store.ContentETag(b), data)
		} else {
			err = r.store.CreateIfAbsent(ctx, key, data)
		}
		if err == nil {
			r.invalidateSegOverrides(sh)
			return nil
		}
		if !errors.Is(err, store.ErrPreconditionFailed) {
			return err
		}
	}
	return errors.New("fs: segment revprop index contention")
}
