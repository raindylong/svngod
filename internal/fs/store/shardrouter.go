package store

// ShardRouter: physically splits one repository's revision history across
// several backing stores by revision range (4.3). Revision-keyed families
// (revs/, revs-loose/, revprops/, revprops-loose/, segs/, seg-revprops/,
// journal/) route to the store that owns the revision's shard; control and
// content objects (current, locks, format, texts, chunks, dirs, txn) go to
// the meta store. Point the meta store at a quorum mirror to get the
// "meta-quorum HEAD" of the design: the HEAD pointer and commit lease stay
// single and strongly consistent while the data plane scales horizontally.
//
// v1 scope: routing only. The commit lease stays repository-global on
// purpose - revision-number continuity and cross-shard batch atomicity are
// correctness invariants; splitting the lease requires a two-level
// (number-allocation + per-shard write) protocol that is deliberately out
// of this change.
//
// Shard-to-store mapping: contiguous ranges of shard indices. Ranges must
// cover 0..n and be given in increasing order. A store may own several
// ranges. Objects for shard s go to the owner of the range containing s.

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// ShardRange maps [From, To] shard indices (inclusive) to a backing store.
type ShardRange struct {
	From, To uint64
	Store    Store
}

// ShardRouterConfig wires a router.
type ShardRouterConfig struct {
	// Prefix is the repository key prefix (e.g. "repos/alpha"); keys under
	// it are parsed as family/... after stripping it.
	Prefix string
	// Meta receives control and content objects. Use a quorum mirror here
	// for the meta-quorum HEAD design.
	Meta Store
	// Ranges are the shard ownership table, in increasing From order,
	// covering all shard indices without gaps or overlaps.
	Ranges []ShardRange
	// ShardSize is revisions per shard (must match the repo's format).
	ShardSize uint64
}

// ShardRouter is a Store that dispatches by key.
type ShardRouter struct {
	prefix    string
	meta      Store
	ranges    []ShardRange
	shardSize uint64
}

// NewShardRouter validates the ranges and builds the router.
func NewShardRouter(cfg ShardRouterConfig) (*ShardRouter, error) {
	if cfg.Meta == nil || len(cfg.Ranges) == 0 {
		return nil, fmt.Errorf("store: shard router needs a meta store and ranges")
	}
	if cfg.ShardSize == 0 {
		return nil, fmt.Errorf("store: shard router needs a shard size")
	}
	r := &ShardRouter{prefix: strings.Trim(cfg.Prefix, "/") + "/", meta: cfg.Meta, ranges: cfg.Ranges, shardSize: cfg.ShardSize}
	prev := uint64(0)
	for i, rg := range r.ranges {
		if i == 0 && rg.From != 0 {
			return nil, fmt.Errorf("store: shard ranges must start at 0")
		}
		if i > 0 && rg.From != prev+1 {
			return nil, fmt.Errorf("store: shard ranges must be contiguous (got From=%d after To=%d)", rg.From, prev)
		}
		if rg.To < rg.From || rg.Store == nil {
			return nil, fmt.Errorf("store: bad shard range %d..%d", rg.From, rg.To)
		}
		prev = rg.To
	}
	return r, nil
}

// shardFamilies are the key prefixes routed by revision shard.
var shardFamilies = map[string]bool{
	"revs": true, "revs-loose": true, "revprops": true, "revprops-loose": true,
	"segs": true, "seg-revprops": true, "journal": true,
}

// route picks the backing store for a key.
func (r *ShardRouter) route(key string) Store {
	fam, shard, ok := r.parseShardKey(key)
	if !ok || !shardFamilies[fam] {
		return r.meta
	}
	for _, rg := range r.ranges {
		if shard >= rg.From && shard <= rg.To {
			return rg.Store
		}
	}
	return r.meta // beyond the table: the last range owns it semantically; validated contiguous so unreachable
}

// parseShardKey extracts the family and shard index from a repo key
// (prefix/family/...). journal/<rev> converts rev to shard via shardSize.
func (r *ShardRouter) parseShardKey(key string) (fam string, shard uint64, ok bool) {
	if !strings.HasPrefix(key, r.prefix) {
		return "", 0, false
	}
	rest := key[len(r.prefix):]
	slash := strings.Index(rest, "/")
	if slash < 0 {
		return "", 0, false
	}
	fam = rest[:slash]
	rest = rest[slash+1:]
	switch fam {
	case "journal":
		// journal/<12-digit rev>
		rev, err := strconv.ParseUint(strings.TrimSuffix(rest, "/"), 10, 64)
		if err != nil {
			return fam, 0, false
		}
		return fam, rev / r.shardSize, true
	case "revs", "revs-loose", "revprops", "revprops-loose", "segs", "seg-revprops":
		// <family>/<10-digit shard>... ; segs/<shard>/... also leads with the shard
		part := rest
		if i := strings.Index(part, "/"); i >= 0 {
			part = part[:i]
		}
		part = strings.TrimSuffix(part, ".pack")
		part = strings.TrimSuffix(part, ".idx")
		part = strings.TrimSuffix(part, ".done")
		part = strings.TrimSuffix(part, ".seg")
		n, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return fam, 0, false
		}
		return fam, n, true
	}
	return "", 0, false
}

// ---------------------------------------------------------------- Store

func (r *ShardRouter) Put(ctx context.Context, key string, data []byte) error {
	return r.route(key).Put(ctx, key, data)
}

func (r *ShardRouter) PutFrom(ctx context.Context, key string, rd io.Reader, size int64) error {
	return r.route(key).PutFrom(ctx, key, rd, size)
}

func (r *ShardRouter) Get(ctx context.Context, key string, rng *Range) ([]byte, error) {
	return r.route(key).Get(ctx, key, rng)
}

func (r *ShardRouter) GetTo(ctx context.Context, key string, w io.Writer) (int64, error) {
	return r.route(key).GetTo(ctx, key, w)
}

func (r *ShardRouter) Exists(ctx context.Context, key string) (bool, error) {
	return r.route(key).Exists(ctx, key)
}

func (r *ShardRouter) Head(ctx context.Context, key string) (string, bool, error) {
	return r.route(key).Head(ctx, key)
}

func (r *ShardRouter) CreateIfAbsent(ctx context.Context, key string, data []byte) error {
	return r.route(key).CreateIfAbsent(ctx, key, data)
}

func (r *ShardRouter) CAS(ctx context.Context, key string, expectETag string, data []byte) (string, error) {
	return r.route(key).CAS(ctx, key, expectETag, data)
}

func (r *ShardRouter) Delete(ctx context.Context, keys []string) error {
	// Route per key; each backend gets its own batch.
	byStore := map[Store][]string{}
	var order []Store
	for _, k := range keys {
		s := r.route(k)
		if _, seen := byStore[s]; !seen {
			order = append(order, s)
		}
		byStore[s] = append(byStore[s], k)
	}
	for _, s := range order {
		if err := s.Delete(ctx, byStore[s]); err != nil {
			return err
		}
	}
	return nil
}

// ListPrefix fans out to every backend that could hold the prefix and
// merges results. (Shard-family prefixes like journal/ exist on every shard
// owner plus possibly meta; control prefixes live on meta only. Merging is
// safe: duplicate-free because each key routes to exactly one backend.)
func (r *ShardRouter) ListPrefix(ctx context.Context, prefix, continuation string, max int) ([]ListedObj, string, error) {
	stores := map[Store]bool{r.meta: true}
	for _, rg := range r.ranges {
		stores[rg.Store] = true
	}
	var all []ListedObj
	var next string
	for s := range stores {
		objs, n, err := s.ListPrefix(ctx, prefix, continuation, max)
		if err != nil {
			return nil, "", err
		}
		all = append(all, objs...)
		if n != "" {
			next = n
		}
	}
	sortListed(all)
	if len(all) > max {
		all = all[:max]
	}
	return all, next, nil
}

func sortListed(objs []ListedObj) {
	for i := 1; i < len(objs); i++ {
		for j := i; j > 0 && objs[j-1].Key > objs[j].Key; j-- {
			objs[j-1], objs[j] = objs[j], objs[j-1]
		}
	}
}

var _ Store = (*ShardRouter)(nil)

// Journaling / quorum capability probes forward to the meta store, which is
// where the WAL arbiter and lease quorum live in the meta-quorum design.
func (r *ShardRouter) Journaling() bool {
	if j, ok := r.meta.(interface{ Journaling() bool }); ok {
		return j.Journaling()
	}
	return false
}

func (r *ShardRouter) WriteQuorum() int {
	if q, ok := r.meta.(interface{ WriteQuorum() int }); ok {
		return q.WriteQuorum()
	}
	return 0
}

func (r *ShardRouter) QuorumGet(ctx context.Context, key string) ([]byte, int, error) {
	if qg, ok := r.meta.(interface {
		QuorumGet(ctx context.Context, key string) ([]byte, int, error)
	}); ok {
		return qg.QuorumGet(ctx, key)
	}
	b, err := r.meta.Get(ctx, key, nil)
	if err != nil {
		return nil, 0, err
	}
	return b, 1, nil
}

func (r *ShardRouter) ArbiterGet(ctx context.Context, key string) ([]byte, error) {
	if ag, ok := r.meta.(interface {
		ArbiterGet(ctx context.Context, key string) ([]byte, error)
	}); ok {
		return ag.ArbiterGet(ctx, key)
	}
	return r.meta.Get(ctx, key, nil)
}
