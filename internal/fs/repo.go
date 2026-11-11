package fs

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"svngod/internal/cache"
	"svngod/internal/cdc"
	"svngod/internal/fs/store"
	"svngod/internal/svndiff"
)

// ErrRepoNotFound is returned by OpenRepo when the repo's format object is
// missing (used by the DAV layer to map to 404).
var ErrRepoNotFound = errors.New("fs: repository not found")

// ErrPathNotFound is returned by Resolve/ListDir when a path does not exist
// as of the given revision (mapped to 404 by the DAV layer).
var ErrPathNotFound = errors.New("fs: path not found")
var ErrOutOfDate = errors.New("fs: commit base revision is out of date")

// debugCommit logs per-phase commit timings (SVNGOD_DEBUG_COMMIT=1) so a slow
// backend can be attributed to lease, head read, tree build, object writes or
// the final CAS without a profiler.
var debugCommit = os.Getenv("SVNGOD_DEBUG_COMMIT") == "1"

// OutOfDateError identifies the path whose working-copy revision predates a
// concurrent change. It unwraps to ErrOutOfDate so callers can keep using
// errors.Is.
type OutOfDateError struct{ Path string }

func (e *OutOfDateError) Error() string { return "fs: " + e.Path + " is out of date" }
func (e *OutOfDateError) Unwrap() error { return ErrOutOfDate }

// Repo is a versioned repository backed by an object Store.
type Repo struct {
	store       store.Store
	keys        *store.Keys
	shardSz     uint64
	maxDelta    uint64
	chunkParams cdc.Params
	dirBuckets  bool
	// segments stores revisions >= segmentFrom in batched segment objects
	// (format v2); 0/false keeps the per-revision loose/pack layout.
	segments      bool
	segmentFrom   uint64
	segmentTarget uint64
	// compactQ carries shards to the background compactor.
	// Merging a full shard costs O(shard size); doing it inline made the commit
	// that closed the shard wait for the merge, and a client that timed out
	// cancelled the merge halfway through.
	compactQ    chan compactJob
	compactOnce sync.Once
	// segMetas caches parsed row tables; segDecoded caches whole decoded
	// segments for sequential scans. Named separately from cache (which holds
	// raw revision blocks) so a scan does not evict the block working set.
	segMetas   *segMetaCache
	segDecoded *segDecodeCache
	// fullReads forces whole-object segment reads. Verify sets it so the index
	// SHA-256 is checked; ranged reads only see the per-entry CRC.
	fullReads atomic.Bool
	// Read-path cache counters (exposed via CacheStats / metrics).
	blockCacheHits   atomic.Uint64
	blockCacheMisses atomic.Uint64
	segCacheHits     atomic.Uint64
	segCacheMisses   atomic.Uint64
	// seqMu guards the sequential-read hint: same segment, previous revision.
	seqMu   sync.Mutex
	seqKey  string
	seqNext uint64
	// segRepairAt rate-limits read-triggered index rebuilds per shard.
	segRepairMu sync.Mutex
	segRepairAt map[uint64]time.Time
	// segOverrides caches which revisions have a mutable revprop override
	// (segment repositories). Bounded by shards touched, same TTL as revprops.
	segOverrideMu sync.Mutex
	segOverrides  map[uint64]map[uint64]bool
	segOverrideAt map[uint64]time.Time
	// writeQuorum is the number of replicas a commit record must reach before
	// publication (0/1 = single-node semantics, no WAL).
	writeQuorum int
	// journal enables the commit WAL for mirror stores; writeQuorum > 1 makes
	// the record (and the lease) quorum-replicated, else it is best-effort.
	journal bool
	// batch groups concurrent online commits (nil when disabled).
	batch *commitBatcher
	// journalMu serialises WAL replay and throttles it during reads.
	journalMu sync.Mutex
	journalAt time.Time

	mu    sync.Mutex
	cache *cache.LRU
	// nodeCache caches decoded node-revs by immutable id so repeated Resolve /
	// ListDir on the same revision reuse the node (and its ByName index) instead
	// of re-decoding the block and re-scanning Children each time. cache.LRU is
	// []byte-only, so this is a dedicated pointer map with its own mutex.
	nodeCacheMu sync.Mutex
	nodeCache   map[cache.Key]*NodeRev
	// blockCache keeps decoded rev blocks. A block is immutable and its nodes
	// are decoded on demand, so sharing it is safe; without this every Resolve
	// and ListDir copied the whole block and re-parsed its node table, which
	// dominates walks over large revisions (a 1 MiB inline text makes the
	// block 1 MiB, and a 200-file report did that hundreds of times).
	blockCacheMu sync.Mutex
	blockCache   map[uint64]*RevBlock
	blockBytes   int

	// Parsed revprops packs, keyed by shard. A packed revprops object holds up
	// to shardSz revisions; fetching it per revision made svn log O(pack size)
	// per entry on object storage. Bounded to a few shards.
	revPropsPackMu    sync.Mutex
	revPropsPacks     map[uint64]map[uint64]*RevProps
	revPropsPackAt    map[uint64]time.Time
	revPropsPackOrder []uint64

	// Short-lived HEAD cache: one commit issues several requests (OPTIONS,
	// PROPFIND, REPORT, CHECKOUT, MERGE), each of which would otherwise read
	// the current pointer from the object store. Writes that depend on a fresh
	// answer use HeadFresh/readCurrent; a local commit refreshes the entry.
	// Revision properties are immutable after commit; reports ask for them
	// once per node, which on S3 turned `svn ls -R` into hundreds of GETs.
	revPropsMu sync.Mutex
	revProps   map[uint64]*RevProps
	revPropsAt map[uint64]time.Time

	headMu  sync.Mutex
	headRev uint64
	headAt  time.Time
	uuidMu  sync.Mutex
	uuid    string
}

// headCacheTTL bounds how long a cached HEAD may be served.
const headCacheTTL = 100 * time.Millisecond

// revPropsCacheTTL bounds how long revision properties may be served from
// memory. Revision props are mutable (svn propset --revprop), and another
// replica may rewrite them at any time, so a permanent cache would hide the
// change until that replica restarts.
const revPropsCacheTTL = 1 * time.Second

// blockCacheBudget bounds decoded rev blocks held per repository.
const blockCacheBudget = 64 << 20

// cachedBlock returns a decoded block, if present.
func (r *Repo) cachedBlock(rev uint64) (*RevBlock, bool) {
	r.blockCacheMu.Lock()
	rb, ok := r.blockCache[rev]
	r.blockCacheMu.Unlock()
	if ok {
		r.blockCacheHits.Add(1)
	} else {
		r.blockCacheMisses.Add(1)
	}
	return rb, ok
}

// CacheStats reports read-path cache effectiveness. Counters are cumulative
// since the repository was opened; the metrics endpoint exposes deltas through
// Prometheus' rate().
type CacheStats struct {
	BlockHits, BlockMisses     uint64
	SegmentHits, SegmentMisses uint64
}

// CacheStats returns a snapshot of the read-path cache counters.
func (r *Repo) CacheStats() CacheStats {
	return CacheStats{
		BlockHits: r.blockCacheHits.Load(), BlockMisses: r.blockCacheMisses.Load(),
		SegmentHits: r.segCacheHits.Load(), SegmentMisses: r.segCacheMisses.Load(),
	}
}

// storeBlock inserts a decoded block, dropping the whole set when over budget.
func (r *Repo) storeBlock(rev uint64, rb *RevBlock, size int) {
	r.blockCacheMu.Lock()
	defer r.blockCacheMu.Unlock()
	if r.blockCache == nil {
		r.blockCache = map[uint64]*RevBlock{}
	}
	if r.blockBytes+size > blockCacheBudget {
		r.blockCache = map[uint64]*RevBlock{}
		r.blockBytes = 0
	}
	if _, ok := r.blockCache[rev]; !ok {
		r.blockBytes += size
	}
	r.blockCache[rev] = rb
}

// CreateRepo writes a fresh repository with the default chunk profile.
func CreateRepo(st store.Store, prefix string, shardSize, maxDelta uint64) (*Repo, error) {
	return CreateRepoParams(st, prefix, shardSize, maxDelta, cdc.DefaultParams())
}

// CreateRepoParams writes a fresh repository with an explicit chunk profile.
func CreateRepoParams(st store.Store, prefix string, shardSize, maxDelta uint64, chunks cdc.Params) (*Repo, error) {
	return CreateRepoFull(st, prefix, shardSize, maxDelta, chunks, false)
}

// CreateRepoFull also selects whether large directories are paged.
func CreateRepoFull(st store.Store, prefix string, shardSize, maxDelta uint64, chunks cdc.Params, dirBuckets bool) (*Repo, error) {
	return createRepo(st, prefix, shardSize, maxDelta, chunks, dirBuckets, false, 0)
}

// CreateRepoSegments creates a format-v2 repository whose commits are stored in
// batched segment objects. target bounds the compaction target (revisions per
// segment; 0 = shard size).
func CreateRepoSegments(st store.Store, prefix string, shardSize, maxDelta uint64, chunks cdc.Params, dirBuckets bool, target uint64) (*Repo, error) {
	return createRepo(st, prefix, shardSize, maxDelta, chunks, dirBuckets, true, target)
}

func createRepo(st store.Store, prefix string, shardSize, maxDelta uint64, chunks cdc.Params, dirBuckets, segments bool, target uint64) (*Repo, error) {
	chunks = chunks.Normalize()
	if shardSize == 0 {
		shardSize = 4096
	}
	if maxDelta == 0 {
		maxDelta = 64
	}
	keys := store.NewKeys(prefix)
	ctx := context.Background()
	format := &Format{
		Version: 1, ShardSize: shardSize, MaxDeltaChain: maxDelta,
		ChunkMin: uint64(chunks.Min), ChunkMax: uint64(chunks.Max), ChunkMask: chunks.Mask,
		DirBuckets: dirBuckets,
	}
	if segments {
		if target == 0 || target > shardSize {
			target = shardSize
		}
		format.Segments = true
		format.SegmentFrom = 1 // revision 0 stays loose
		format.SegmentTarget = target
	}
	if err := st.Put(ctx, keys.Format(), format.Marshal()); err != nil {
		return nil, err
	}
	if err := st.Put(ctx, keys.UUID(), []byte(randomUUID())); err != nil {
		return nil, err
	}
	// Revision 0 is a real, empty root so that every revision (including 0)
	// has an object; shard packing then needs no special-casing.
	rb0 := NewRevBlock(0, 0, NodeID{Rev: 0, ID: 1}, []*NodeRev{
		{Kind: KindDir, Node: NodeID{Rev: 0, ID: 1}, Props: map[string]string{}},
	})
	if err := st.Put(ctx, keys.LooseRev(0, 0), rb0.Marshal()); err != nil {
		return nil, err
	}
	now := time.Now().Unix()
	rpr0 := NewRevProps()
	rpr0.Props = map[string]string{"svn:date": time.Now().UTC().Format("2006-01-02T15:04:05.000000Z")}
	if err := st.Put(ctx, keys.LooseRevProps(0, 0), rpr0.Marshal()); err != nil {
		return nil, err
	}
	if err := st.Put(ctx, keys.Current(), (&Current{Head: 0, Created: now}).Marshal()); err != nil {
		return nil, err
	}
	wireMirrorControl(st, keys)
	r := &Repo{
		store: st, keys: keys, shardSz: shardSize, maxDelta: maxDelta,
		chunkParams: chunks, dirBuckets: dirBuckets, writeQuorum: writeQuorumOf(st),
		journal:  journalOf(st),
		segments: format.Segments, segmentFrom: format.SegmentFrom, segmentTarget: format.SegmentTarget,
		cache: cache.New(1 << 16), nodeCache: make(map[cache.Key]*NodeRev),
	}
	r.initCompaction()
	return r, nil
}

// ReplicaStatus reports mirror health when the repository is backed by a
// MirrorStore (nil otherwise).
func (r *Repo) ReplicaStatus() []store.ReplicaStatus {
	if ms, ok := r.store.(*store.MirrorStore); ok {
		return ms.Status()
	}
	return nil
}

// DirBucketsEnabled reports whether large directories are stored as buckets.
func (r *Repo) DirBucketsEnabled() bool { return r.dirBuckets }

// SegmentsEnabled reports whether commits are stored in batched segments.
func (r *Repo) SegmentsEnabled() bool { return r.segments }

// SegmentTarget returns the compaction target (revisions per merged segment).
func (r *Repo) SegmentTarget() uint64 { return r.segmentTarget }

// ChunkParams returns the repository's content-defined chunking profile.
func (r *Repo) ChunkParams() cdc.Params { return r.chunkParams }

// OpenRepo loads an existing repository's format.
func OpenRepo(st store.Store, prefix string) (*Repo, error) {
	keys := store.NewKeys(prefix)
	ctx := context.Background()
	b, err := st.Get(ctx, keys.Format(), nil)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, ErrRepoNotFound
		}
		return nil, err
	}
	f, err := UnmarshalFormat(b)
	if err != nil {
		return nil, err
	}
	cp := cdc.DefaultParams()
	if f.ChunkMin != 0 || f.ChunkMax != 0 || f.ChunkMask != 0 {
		cp = cdc.Params{Min: int(f.ChunkMin), Max: int(f.ChunkMax), Mask: f.ChunkMask}.Normalize()
	}
	r := &Repo{
		store: st, keys: keys, shardSz: f.ShardSize, maxDelta: f.MaxDeltaChain,
		chunkParams: cp, dirBuckets: f.DirBuckets, writeQuorum: writeQuorumOf(st),
		segments: f.Segments, segmentFrom: f.SegmentFrom, segmentTarget: f.SegmentTarget,
		journal: journalOf(st),
		cache:   cache.New(1 << 16), nodeCache: make(map[cache.Key]*NodeRev),
	}
	if r.segmentTarget == 0 {
		r.segmentTarget = r.shardSz
	}
	r.initCompaction()
	if commitBatchEnabled() {
		r.batch = newCommitBatcher(r)
	}
	// Replay the commit WAL before the mirror adopts a HEAD: a quorum-acked
	// record whose CAS never ran is completed here.
	r.reconcileJournal(true)
	wireMirrorControl(st, keys)
	return r, nil
}

// wireMirrorControl tells a mirror store which object is the HEAD pointer, so
// failover can refuse writes on a standby whose pointer is older.
func wireMirrorControl(st store.Store, keys *store.Keys) {
	if ms, ok := st.(*store.MirrorStore); ok {
		ms.SetControlKey(keys.Current())
	}
}

func randomUUID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// Extremely unlikely (no entropy source); fall back to the clock so
		// repositories still get distinct ids.
		now := uint64(time.Now().UnixNano())
		for i := 0; i < 8; i++ {
			b[i] = byte(now >> (8 * i))
		}
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// Keys exposes the object key builder (for admin/GC tooling).
func (r *Repo) Keys() *store.Keys { return r.keys }

// Store exposes the underlying store (for GC / admin).
func (r *Repo) Store() store.Store { return r.store }

// UUID returns the repository's immutable uuid string.
func (r *Repo) UUID() (string, error) {
	// The UUID is immutable for the life of a repository; serve it from memory
	// after the first read (an S3 GET per OPTIONS/REPORT otherwise).
	r.uuidMu.Lock()
	u := r.uuid
	r.uuidMu.Unlock()
	if u != "" {
		return u, nil
	}
	b, err := r.store.Get(context.Background(), r.keys.UUID(), nil)
	if err != nil {
		return "", err
	}
	u = string(b)
	r.uuidMu.Lock()
	r.uuid = u
	r.uuidMu.Unlock()
	return u, nil
}

// SetUUID overwrites the repository UUID. Used by dump/load migration so a
// loaded repository keeps the identity of its source.
func (r *Repo) SetUUID(u string) error {
	if u == "" {
		return errors.New("fs: empty uuid")
	}
	if err := r.store.Put(context.Background(), r.keys.UUID(), []byte(u)); err != nil {
		return err
	}
	r.uuidMu.Lock()
	r.uuid = u
	r.uuidMu.Unlock()
	return nil
}

// ShardSize returns the configured shard size.
func (r *Repo) ShardSize() uint64 { return r.shardSz }

// --- HEAD ---

// Head returns the current HEAD revision.
func (r *Repo) Head() (uint64, error) {
	r.headMu.Lock()
	if !r.headAt.IsZero() && time.Since(r.headAt) < headCacheTTL {
		rev := r.headRev
		r.headMu.Unlock()
		return rev, nil
	}
	r.headMu.Unlock()
	return r.HeadFresh()
}

// HeadFresh reads the current pointer, bypassing the HEAD cache. Use it when a
// stale answer could change a write decision (e.g. the base revision of a new
// transaction).
func (r *Repo) HeadFresh() (uint64, error) {
	// In quorum mode a process may be reading a mirror that missed a commit
	// published through another process. Replaying the WAL tail (throttled)
	// keeps HEAD monotonic without turning every read into a full reconcile.
	r.reconcileJournal(false)
	cur, _, err := r.readCurrent()
	if err != nil {
		return 0, err
	}
	r.setHead(cur.Head)
	return cur.Head, nil
}

func (r *Repo) setHead(rev uint64) {
	r.headMu.Lock()
	r.headRev = rev
	r.headAt = time.Now()
	r.headMu.Unlock()
}

// readCurrentStrict reads the current pointer from the authoritative replica.
// CommitTxn uses it instead of readCurrent: after a transient arbiter read
// failure the lenient path may fall back to a stale standby, and choosing
// head+1 from a stale value would write a revision block over an existing
// revision on the arbiter (the following CAS would then fail, but the block
// would already be damaged).
func (r *Repo) readCurrentStrict() (*Current, string, error) {
	ctx := context.Background()
	key := r.keys.Current()
	if ar, ok := r.store.(store.ArbiterReader); ok && ar.ArbiterRequired() {
		b, err := ar.ArbiterGet(ctx, key)
		if err != nil {
			return nil, "", fmt.Errorf("fs: read authoritative head: %w", err)
		}
		cur, err := UnmarshalCurrent(b)
		return cur, store.ContentETag(b), err
	}
	return r.readCurrent()
}

func (r *Repo) readCurrent() (*Current, string, error) {
	ctx := context.Background()
	key := r.keys.Current()
	var b []byte
	var err error
	// The commit lease and the HEAD pointer are authoritative on the arbiter
	// of a two-node pair. A standby that missed a publication used to make a
	// retry believe HEAD was older and republish the same revision number on
	// the other replica; reading the arbiter first closes that split. When it
	// cannot answer, fall back to the normal read path: commits still fail at
	// the lease (which requires the arbiter), so a stale answer cannot publish
	// anything.
	if ar, ok := r.store.(store.ArbiterReader); ok && ar.ArbiterRequired() {
		b, err = ar.ArbiterGet(ctx, key)
		if err != nil {
			b = nil
		}
	}
	if b == nil {
		b, err = r.store.Get(ctx, key, nil)
	}
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, "", errors.New("fs: repository not initialized (missing current)")
		}
		return nil, "", err
	}
	cur, err := UnmarshalCurrent(b)
	// The ETag is the content hash of the current pointer itself, not the
	// store's HEAD metadata: on object stores (RustFS) HEAD/GET header ETags
	// are unstable, while the GET body is byte-exact. All store CAS
	// implementations compare against this same content-hash ETag.
	return cur, store.ContentETag(b), err
}

func (r *Repo) shard(rev uint64) uint64 { return rev / r.shardSz }

// --- shard state + block reads (with caching) ---

func (r *Repo) isPacked(shard uint64) (bool, error) {
	k := cache.Key(fmt.Sprintf("st/%d", shard))
	if v, ok := r.cache.Get(k); ok {
		return len(v) == 1 && v[0] == 1, nil
	}
	ctx := context.Background()
	_, exists, err := r.store.Head(ctx, r.keys.PackedDone(shard))
	if err != nil {
		return false, err
	}
	// Cache only the positive result: a shard never becomes loose again, but
	// another process can pack a shard at any time. Caching "not packed" left
	// revprop updates writing to the loose copy after a concurrent pack, so
	// the update was silently invisible to readers.
	if exists {
		r.cache.Put(k, []byte{1})
	}
	return exists, nil
}

func (r *Repo) readShardIdx(shard uint64) (*ShardIdx, error) {
	k := cache.Key(fmt.Sprintf("idx/%d", shard))
	if b, ok := r.cache.Get(k); ok {
		return UnmarshalShardIdx(b)
	}
	ctx := context.Background()
	b, err := r.store.Get(ctx, r.keys.PackedIdx(shard), nil)
	if err != nil {
		return nil, err
	}
	idx, err := UnmarshalShardIdx(b)
	if err != nil {
		return nil, err
	}
	r.cache.Put(k, b)
	return idx, nil
}

// readBlock reads the rev block for a revision from loose or packed storage.
func (r *Repo) readBlock(rev uint64) (*RevBlock, error) {
	if rb, ok := r.cachedBlock(rev); ok {
		return rb, nil
	}
	k := cache.Key(fmt.Sprintf("blk/%d", rev))
	if b, ok := r.cache.Get(k); ok {
		rb, err := UnmarshalRevBlock(b)
		if err == nil {
			r.storeBlock(rev, rb, len(b))
		}
		return rb, err
	}
	ctx := context.Background()
	if r.segments && rev >= r.segmentFrom {
		data, _, err := r.segmentEntry(rev, false)
		if err != nil {
			return nil, err
		}
		rb, err := UnmarshalRevBlock(data)
		if err != nil {
			return nil, err
		}
		cachePut(r.cache, k, data)
		r.storeBlock(rev, rb, len(data))
		return rb, nil
	}
	sh := r.shard(rev)
	var data []byte
	var err error
	packed, err := r.isPacked(sh)
	if err != nil {
		return nil, err
	}
	if packed {
		idx, err := r.readShardIdx(sh)
		if err != nil {
			return nil, err
		}
		off, ln, ok := idx.Lookup(rev)
		if !ok {
			return nil, fmt.Errorf("fs: revision %d not in shard %d index", rev, sh)
		}
		data, err = r.store.Get(ctx, r.keys.PackedRev(sh), &store.Range{Offset: int64(off), Length: int64(ln)})
	} else {
		data, err = r.store.Get(ctx, r.keys.LooseRev(sh, rev), nil)
	}
	if err != nil {
		return nil, err
	}
	rb, err := UnmarshalRevBlock(data)
	if err != nil {
		return nil, err
	}
	cachePut(r.cache, k, data)
	r.storeBlock(rev, rb, len(data))
	return rb, nil
}

// GetNodeRev returns a node-rev by its immutable id.
func (r *Repo) GetNodeRev(id NodeID) (*NodeRev, error) {
	k := cache.Key(fmt.Sprintf("nr/%d.%d", id.Rev, id.ID))
	r.nodeCacheMu.Lock()
	if v, ok := r.nodeCache[k]; ok {
		r.nodeCacheMu.Unlock()
		return v, nil
	}
	r.nodeCacheMu.Unlock()
	rb, err := r.readBlock(id.Rev)
	if err != nil {
		return nil, err
	}
	nr, err := rb.GetNode(id.ID)
	if err != nil {
		return nil, err
	}
	if nr.Kind == KindDir && nr.ByName == nil {
		nr.ByName = make(map[string]Child, len(nr.Children))
		for _, c := range nr.Children {
			nr.ByName[c.Name] = c
		}
	}
	r.nodeCacheMu.Lock()
	if len(r.nodeCache) >= 1<<13 {
		r.nodeCache = make(map[cache.Key]*NodeRev)
	}
	r.nodeCache[k] = nr
	r.nodeCacheMu.Unlock()
	return nr, nil
}

// PutTextFile stores a local file as a content-addressed text object and
// returns its reference. Stores that support PutFile move (link) the file
// instead of copying it.
func (r *Repo) PutTextFile(ctx context.Context, path, ref string) error {
	key := r.keys.Text(ref)
	if ok, err := r.store.Exists(ctx, key); err == nil && ok {
		return nil
	}
	if fp, ok := r.store.(store.FilePutter); ok {
		if err := fp.PutFile(ctx, key, path); err == nil {
			return nil
		}
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	return r.store.PutFrom(ctx, key, f, st.Size())
}

// FullTextBase returns the nearest node in nr's delta chain that stores its
// text in full (DeltaDepth 0). A delta is always taken against such a node so
// reading it costs a single apply, no matter how many edits came before.
func (r *Repo) FullTextBase(nr *NodeRev) (*NodeRev, error) {
	for i := 0; i < 256; i++ {
		if nr.DeltaDepth == 0 {
			return nr, nil
		}
		if nr.Base == nil {
			return nil, errors.New("fs: delta node without base")
		}
		next, err := r.GetNodeRev(*nr.Base)
		if err != nil {
			return nil, err
		}
		nr = next
	}
	return nil, errors.New("fs: delta chain too deep")
}

// deltaBaseOf reports the node with the given id if it is target itself or
// reachable through the node's Delta Base links, i.e. the delta applies to it.
func (r *Repo) deltaBaseOf(target *NodeRev, want NodeID) (*NodeRev, error) {
	for i := 0; i < 256; i++ {
		if target.Node == want {
			return target, nil
		}
		if target.DeltaDepth == 0 || target.Base == nil {
			break
		}
		nr, err := r.GetNodeRev(*target.Base)
		if err != nil {
			return nil, err
		}
		target = nr
	}
	return nil, errors.New("fs: delta base is not in the node's history")
}

// MaxDeltaDepth is the longest delta chain the repository stores before it
// writes a full text instead.
func (r *Repo) MaxDeltaDepth() uint64 { return r.maxDelta }

// externalizeFile streams a file body into a content-addressed text object and
// returns its md5, sha1, size and reference. The body is hashed and uploaded
// in two passes, so memory use stays constant for arbitrarily large uploads.
func (r *Repo) externalizeFile(ctx context.Context, path string, known *[16]byte, knownSHA1 *[20]byte) ([16]byte, [20]byte, uint64, string, error) {
	var sum [16]byte
	var sha [20]byte
	var n int64
	if known != nil && knownSHA1 != nil {
		// The upload path already hashed the body while streaming it.
		sum, sha = *known, *knownSHA1
		st, serr := os.Stat(path)
		if serr != nil {
			return sum, sha, 0, "", serr
		}
		n = st.Size()
	} else {
		f, err := os.Open(path)
		if err != nil {
			return sum, sha, 0, "", err
		}
		h := md5.New()
		hs := sha1.New()
		n, err = io.Copy(io.MultiWriter(h, hs), f)
		f.Close()
		if err != nil {
			return sum, sha, 0, "", err
		}
		copy(sum[:], h.Sum(nil))
		if known != nil {
			sum = *known
		}
		copy(sha[:], hs.Sum(nil))
	}
	ref := hex.EncodeToString(sum[:])
	key := r.keys.Text(ref)
	if ok, herr := r.store.Exists(ctx, key); herr == nil && ok {
		return sum, sha, uint64(n), ref, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return sum, sha, 0, "", err
	}
	defer f.Close()
	if err := r.store.PutFrom(ctx, key, f, n); err != nil {
		return sum, sha, 0, "", err
	}
	return sum, sha, uint64(n), ref, nil
}

// StreamText writes a node's full text to w. External texts are streamed from
// their object; inline full/delta texts are materialized first (delta decoding
// needs random access to the base).
func (r *Repo) StreamText(id NodeID, w io.Writer) (uint64, error) {
	nr, err := r.GetNodeRev(id)
	if err != nil {
		return 0, err
	}
	switch nr.TextRep {
	case TextRepExternal:
		return r.streamExternalText(nr, w)
	case TextRepDelta:
		if nr.Base == nil {
			return 0, errors.New("fs: delta node missing base")
		}
		base, berr := r.GetText(*nr.Base)
		if berr != nil {
			return 0, berr
		}
		cw := &countWriter{w: w}
		if aerr := svndiff.ApplyTo(base, bytes.NewReader(nr.Text), cw); aerr != nil {
			return cw.n, aerr
		}
		return cw.n, nil
	case TextRepChunked:
		refs, perr := cdc.Parse(nr.Text)
		if perr != nil {
			return 0, perr
		}
		cw := &countWriter{w: w}
		if _, cerr := io.Copy(cw, r.chunkReader(refs)); cerr != nil {
			return cw.n, cerr
		}
		return cw.n, nil
	default:
		n, werr := w.Write(nr.Text)
		return uint64(n), werr
	}
}

// externalStreamVerifyLimit: up to this size an external text is loaded,
// verified (and repaired) in memory before any byte reaches the client.
// Larger texts are spooled to a temp file first, so a corrupted replica can
// never stream bad bytes to a client.
const externalStreamVerifyLimit = 32 << 20

// streamExternalText sends an external text after verifying its checksum,
// repairing a corrupt replica from a healthy copy when possible.
func (r *Repo) streamExternalText(nr *NodeRev, w io.Writer) (uint64, error) {
	if nr.TextSize <= externalStreamVerifyLimit {
		b, err := r.GetText(nr.Node)
		if err != nil {
			return 0, err
		}
		n, werr := w.Write(b)
		return uint64(n), werr
	}
	f, err := os.CreateTemp("", "svngod-stream-")
	if err != nil {
		return 0, err
	}
	name := f.Name()
	defer os.Remove(name)
	defer f.Close()
	h := md5.New()
	n, gerr := r.store.GetTo(context.Background(), r.keys.Text(nr.TextRef), io.MultiWriter(f, h))
	if gerr == nil && uint64(n) == nr.TextSize && bytes.Equal(h.Sum(nil), nr.TextMD5[:]) {
		if _, serr := f.Seek(0, io.SeekStart); serr != nil {
			return 0, serr
		}
		m, cerr := io.Copy(w, f)
		return uint64(m), cerr
	}
	// The primary copy is corrupt. Fetch a verified copy (this also heals the
	// bad replicas) and send that instead.
	b := r.repairExternalText(nr)
	if b == nil {
		return 0, errors.New("fs: external text checksum mismatch")
	}
	m, werr := w.Write(b)
	return uint64(m), werr
}

// countWriter counts bytes written through it.
type countWriter struct {
	w io.Writer
	n uint64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += uint64(n)
	return n, err
}

// TextSizeOf returns a file node's text length without loading external texts.
func (r *Repo) TextSizeOf(nr *NodeRev) (int64, error) {
	if nr.TotalSize > 0 {
		return int64(nr.TotalSize), nil
	}
	switch nr.TextRep {
	case TextRepExternal:
		return int64(nr.TextSize), nil
	case TextRepFull:
		return int64(len(nr.Text)), nil
	case TextRepDelta:
		// Old node-revs predate TotalSize: decode to count without keeping
		// the target in memory.
		n, err := r.StreamText(nr.Node, io.Discard)
		return int64(n), err
	case TextRepChunked:
		refs, err := cdc.Parse(nr.Text)
		if err != nil {
			return 0, err
		}
		return int64(cdc.Size(refs)), nil
	}
	return 0, errors.New("fs: bad text rep")
}

// ReadTextRange returns up to length bytes at off. External texts are read
// with a ranged object request, so a Range GET does not materialize the file.
func (r *Repo) ReadTextRange(nr *NodeRev, off, length int64) ([]byte, error) {
	if nr.TextRep == TextRepChunked {
		refs, err := cdc.Parse(nr.Text)
		if err != nil {
			return nil, err
		}
		return r.readChunkedRange(refs, off, length)
	}
	if nr.TextRep == TextRepExternal {
		if off < 0 || length <= 0 || uint64(off+length) > nr.TextSize {
			return nil, errors.New("fs: range out of bounds")
		}
		return r.store.Get(context.Background(), r.keys.Text(nr.TextRef), &store.Range{Offset: off, Length: length})
	}
	text, err := r.GetText(nr.Node)
	if err != nil {
		return nil, err
	}
	if off < 0 || off > int64(len(text)) {
		return nil, errors.New("fs: range out of bounds")
	}
	end := off + length
	if length <= 0 || end > int64(len(text)) {
		end = int64(len(text))
	}
	return text[off:end], nil
}

// GetText reconstructs a file node-rev's full text (following delta chains).
func (r *Repo) GetText(id NodeID) ([]byte, error) {
	if id.IsZero() {
		return nil, errors.New("fs: cannot read text of zero node id")
	}
	k := cache.Key(fmt.Sprintf("txt/%d.%d", id.Rev, id.ID))
	if b, ok := r.cache.Get(k); ok {
		return b, nil
	}
	nr, err := r.GetNodeRev(id)
	if err != nil {
		return nil, err
	}
	if nr.Kind != KindFile {
		return nil, errors.New("fs: not a file")
	}
	var text []byte
	switch nr.TextRep {
	case TextRepFull:
		text = nr.Text
	case TextRepExternal:
		b, gerr := r.store.Get(context.Background(), r.keys.Text(nr.TextRef), nil)
		if gerr != nil {
			return nil, gerr
		}
		if uint64(len(b)) != nr.TextSize || md5.Sum(b) != nr.TextMD5 {
			if fixed := r.repairExternalText(nr); fixed != nil {
				b = fixed
			}
		}
		if uint64(len(b)) != nr.TextSize {
			return nil, errors.New("fs: external text size mismatch")
		}
		text = b
	case TextRepDelta:
		if nr.Base == nil {
			return nil, errors.New("fs: delta node-rev missing base")
		}
		base, err := r.GetText(*nr.Base)
		if err != nil {
			return nil, err
		}
		text, err = svndiff.Apply(base, nr.Text)
		if err != nil {
			return nil, err
		}
	case TextRepChunked:
		refs, perr := cdc.Parse(nr.Text)
		if perr != nil {
			return nil, perr
		}
		var buf bytes.Buffer
		if _, cerr := io.Copy(&buf, r.chunkReader(refs)); cerr != nil {
			return nil, cerr
		}
		text = buf.Bytes()
	default:
		return nil, errors.New("fs: bad text rep")
	}
	if sum := md5.Sum(text); !bytes.Equal(sum[:], nr.TextMD5[:]) {
		return nil, errors.New("fs: text checksum mismatch")
	}
	// Caching a multi-gigabyte text would pin that much memory per node; large
	// reads stream instead.
	if len(text) <= 4<<20 {
		cachePut(r.cache, k, text)
	}
	return text, nil
}

// repairExternalText asks a multi-copy store for a valid copy of an external
// text object and lets it overwrite corrupt replicas. Returns nil when the
// store cannot repair (single copy) or no valid copy exists.
func (r *Repo) repairExternalText(nr *NodeRev) []byte {
	rg, ok := r.store.(store.RepairingGetter)
	if !ok {
		return nil
	}
	want := nr.TextMD5
	b, err := rg.GetRepairing(context.Background(), r.keys.Text(nr.TextRef), func(cand []byte) bool {
		return uint64(len(cand)) == nr.TextSize && md5.Sum(cand) == want
	})
	if err != nil {
		return nil
	}
	return b
}

// --- tree resolution ---

// NodeInfo describes a path node as of a revision.
type NodeInfo struct {
	Name  string
	Rev   uint64
	ID    NodeID
	Kind  byte
	Props map[string]string
}

// Resolve returns the node-rev at path as of revision rev.
func (r *Repo) Resolve(rev uint64, path string) (*NodeRev, error) {
	rb, err := r.readBlock(rev)
	if err != nil {
		return nil, err
	}
	cur, err := rb.GetNode(rb.Root.ID)
	if err != nil {
		return nil, err
	}
	for _, seg := range segments(path) {
		if cur.Kind != KindDir {
			return nil, ErrPathNotFound
		}
		var next *NodeID
		if cur.TextRep == TextRepExternal {
			c, ok, derr := r.dirChild(cur, seg)
			if derr != nil {
				return nil, derr
			}
			if ok {
				cc := c.ID
				next = &cc
			}
		} else if cur.ByName != nil {
			if c, ok := cur.ByName[seg]; ok {
				cc := c.ID
				next = &cc
			}
		} else {
			for _, c := range cur.Children {
				if c.Name == seg {
					cc := c.ID
					next = &cc
					break
				}
			}
		}
		if next == nil {
			return nil, ErrPathNotFound
		}
		cur, err = r.GetNodeRev(*next)
		if err != nil {
			return nil, err
		}
	}
	return cur, nil
}

// ListDir lists a directory's entries as of revision rev.
func (r *Repo) ListDir(rev uint64, path string) ([]NodeInfo, error) {
	nr, err := r.Resolve(rev, path)
	if err != nil {
		return nil, err
	}
	if nr.Kind != KindDir {
		return nil, errors.New("fs: not a directory")
	}
	children, err := r.dirChildren(nr)
	if err != nil {
		return nil, err
	}
	out := make([]NodeInfo, 0, len(children))
	for _, c := range children {
		cnr, err := r.GetNodeRev(c.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, NodeInfo{Name: c.Name, Rev: c.ID.Rev, ID: c.ID, Kind: cnr.Kind, Props: copyProps(cnr.Props)})
	}
	return out, nil
}

// --- commit ---

// ensureNoPendingPublication refuses to assign head+1 while a WAL record for
// that revision is visible. Such a record means a commit may have been
// acknowledged but not rolled forward (one of its quorum copies is on an
// unavailable replica); reusing the number would overwrite it.
func (r *Repo) ensureNoPendingPublication(head uint64) error {
	if !r.journal {
		return nil
	}
	_, err := r.store.Get(context.Background(), r.keys.Journal(head+1), nil)
	if err == nil {
		return fmt.Errorf("%w: revision %d is pending in the WAL", store.ErrReplicaLagging, head+1)
	}
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	return err
}

// CommitTxn applies the changes as a single new revision and returns its
// number. With online batching enabled, concurrent callers share one lease
// and one HEAD CAS (see CommitBatch); otherwise the commit runs directly.
func (r *Repo) CommitTxn(baseRev uint64, changes []Change, revProps map[string]string) (uint64, error) {
	if r.batch != nil {
		return r.batch.submit(baseRev, changes, revProps)
	}
	if r.segments {
		revs, errs, err := r.CommitBatch([]BatchCommit{{BaseRev: baseRev, Changes: changes, RevProps: revProps}})
		if err != nil {
			return 0, err
		}
		return revs[0], errs[0]
	}
	return r.commitTxn(baseRev, changes, revProps)
}

// commitTxn is the direct single-revision commit path.
func (r *Repo) commitTxn(baseRev uint64, changes []Change, revProps map[string]string) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if revProps == nil {
		revProps = defaultRevProps()
	}

	// Serialise finalize across processes sharing the store: writing the
	// revision block before the `current` CAS is only safe if no second
	// replica is doing the same for the same revision number.
	ctxLease := context.Background()
	t0 := time.Now()
	lease, lerr := r.acquireCommitLease(ctxLease)
	if lerr != nil {
		return 0, lerr
	}
	defer lease.Release(r.store)
	phase := func(name string) {
		if debugCommit {
			log.Printf("commit phase %-9s %s", name, time.Since(t0))
			t0 = time.Now()
		}
	}
	phase("lease")

	// Another process may have published through a different mirror primary
	// while this one was stale; adopt the quorum-acked WAL before choosing the
	// next revision number.
	r.reconcileJournal(true)
	phase("reconcile")

	if len(changes) == 0 {
		return 0, errors.New("fs: empty commit")
	}
	cur, etag, err := r.readCurrentStrict()
	if err != nil {
		return 0, err
	}
	// Never assign head+1 while a WAL record for that revision is visible: a
	// commit may have been acknowledged but not rolled forward because one of
	// its quorum copies is unavailable. Reusing the number would overwrite a
	// different revision's data (the split-lease class of failure).
	if perr := r.ensureNoPendingPublication(cur.Head); perr != nil {
		return 0, perr
	}
	phase("head")
	// Optimistic concurrency control: the client commits against a base
	// revision. If the repository head has moved past it (another writer won
	// the race), refuse so a stale txn cannot clobber newer data (lost update).
	// baseRev==0 skips the check (internal / bulk-load writers).
	//
	// Unlike a whole-repo base==head gate, we do per-path out-of-date checks:
	// a change is only refused when its own target path was modified in the
	// baseRev..HEAD window by another commit. Concurrent commits that touch
	// disjoint paths merge cleanly; concurrent commits to the same path are
	// rejected (native svn raises E160024 here) instead of silently
	// last-write-wins over each other.
	for _, ch := range changes {
		if ch.Path == "/" {
			continue
		}
		// Prefer the client's own base revision for this path; fall back to
		// the transaction base for writers that do not send one.
		base := ch.BaseRev
		if base == 0 {
			base = baseRev
		}
		// Root / directory property changes: svn clients routinely attach root
		// and directory property changes (e.g. svn:date) to every commit. A
		// directory's node-rev moves whenever any child changes, so a strict
		// check here would falsely reject disjoint-path commits. Children carry
		// their own Change entries and are guarded below.
		//
		// A directory delete or copy target is different: applying it over a
		// directory that gained content since the client's base destroys
		// another writer's commit (observed as a silently lost file).
		if ch.Kind == KindDir && ch.Action == ActionModify {
			continue
		}
		if ch.Kind == KindDir {
			hnr, herr := r.Resolve(cur.Head, ch.Path)
			if herr == nil {
				if base > 0 && hnr.Node.Rev > base {
					log.Printf("fs: commit OOD dir path=%s headRev=%d baseRev=%d -> reject", ch.Path, hnr.Node.Rev, base)
					return 0, &OutOfDateError{Path: ch.Path}
				}
				if base == 0 && ch.Action == ActionAdd {
					return 0, &OutOfDateError{Path: ch.Path}
				}
				continue
			}
			// Absent at HEAD: fall through to the generic deleted-in-window
			// and missing-path handling below.
		}
		if len(ch.Delta) > 0 && base == 0 {
			log.Printf("fs: commit OOD delta without a base path=%s", ch.Path)
			return 0, &OutOfDateError{Path: ch.Path}
		}
		hnr, err := r.Resolve(cur.Head, ch.Path)
		if err == nil {
			if base == 0 {
				if ch.Action == ActionAdd {
					// The client adds a node that another commit created first.
					log.Printf("fs: commit conflict (already exists) path=%s head=%d", ch.Path, cur.Head)
					return 0, &OutOfDateError{Path: ch.Path}
				}
				continue
			}
			if hnr.Node.Rev > base {
				log.Printf("fs: commit OOD path=%s headRev=%d baseRev=%d -> reject", ch.Path, hnr.Node.Rev, base)
				return 0, &OutOfDateError{Path: ch.Path}
			}
			if len(ch.Delta) > 0 {
				// The delta was built against a node in this path's history.
				// HEAD must still be exactly the node the client diffed from
				// (a delete/re-add can leave a different node with an older
				// revision in place), and the recorded base must be reachable
				// from it.
				bnr, berr := r.Resolve(base, ch.Path)
				if berr != nil || hnr.Node != bnr.Node {
					log.Printf("fs: commit OOD delta path=%s base=%d (node changed)", ch.Path, base)
					return 0, &OutOfDateError{Path: ch.Path}
				}
				if _, berr := r.deltaBaseOf(bnr, ch.DeltaBase); berr != nil {
					log.Printf("fs: commit OOD delta base path=%s base=%d: %v", ch.Path, base, berr)
					return 0, &OutOfDateError{Path: ch.Path}
				}
			}
			continue
		}
		// Path absent at HEAD but present at the client's base revision: it was
		// deleted by a concurrent commit, so a change touching it is out of date.
		if base > 0 {
			if _, e := r.Resolve(base, ch.Path); e == nil {
				log.Printf("fs: commit OOD deleted-in-window path=%s head=%d base=%d -> reject", ch.Path, cur.Head, base)
				return 0, &OutOfDateError{Path: ch.Path}
			}
		}
	}
	phase("ood")
	N := cur.Head + 1
	ctx := context.Background()
	hb, err := r.readBlock(cur.Head)
	if err != nil {
		return 0, err
	}
	root, err := r.loadNode(hb.Root)
	if err != nil {
		return 0, err
	}
	rbBytes, rpBytes, err := r.buildRevisionLocked(ctx, root, cur.Head, N, uint64(cur.Created), changes, revProps)
	if err != nil {
		return 0, err
	}

	// The lease may have expired during a long finalize; stop before writing
	// a revision block that another replica could already be publishing.
	if err := lease.Verify(ctx, r.store); err != nil {
		return 0, err
	}
	sh := r.shard(N)
	// The revision block and its properties are independent objects; writing
	// them concurrently saves one ordered write round trip per mirror on the
	// commit path (each Put itself targets the arbiter before the standby).
	var rbErr, rpErr error
	var writeWG sync.WaitGroup
	writeWG.Add(2)
	go func() {
		defer writeWG.Done()
		rbErr = r.store.Put(ctx, r.keys.LooseRev(sh, N), rbBytes)
	}()
	go func() {
		defer writeWG.Done()
		rpErr = r.store.Put(ctx, r.keys.LooseRevProps(sh, N), rpBytes)
	}()
	writeWG.Wait()
	if rbErr != nil {
		return 0, rbErr
	}
	if rpErr != nil {
		return 0, rpErr
	}
	phase("writes")
	nc := &Current{Head: N, Created: cur.Created}
	if nc.Created == 0 {
		nc.Created = time.Now().Unix()
	}
	// WAL: record the publication intent on a quorum before the pointer moves.
	// A crash right after this leaves a record the ledger can roll forward; a
	// crash before it leaves no visible revision at all.
	if err := lease.Verify(ctx, r.store); err != nil {
		return 0, err
	}
	if err := r.writeCommitJournal(ctx, N, cur.Head, lease.token, nc.Created, rbBytes,
		r.keys.LooseRev(sh, N), r.keys.LooseRevProps(sh, N), revProps); err != nil {
		return 0, err
	}
	if err := lease.Verify(ctx, r.store); err != nil {
		// The WAL record is already durable and names this commit. If it is
		// still ours, replay would publish it anyway, so verify the lease can
		// no longer turn an already-reserved revision into a client-visible
		// failure. If another writer replaced the record, stop instead.
		if !r.journalOwns(ctx, N, lease.token, rbBytes) {
			return 0, err
		}
		log.Printf("commit: lease unverified after WAL (rev %d); journal record is ours, publishing", N)
	}
	phase("journal")
	// Chaos hook (test-only): exit after the WAL append but before the HEAD CAS
	// to exercise journal roll-forward on a live store.
	if os.Getenv("SVNGOD_CHAOS_AFTER_WAL") == "1" {
		log.Printf("chaos: exiting after WAL rev=%d before current CAS", N)
		os.Exit(9)
	}
	if _, err := r.store.CAS(ctx, r.keys.Current(), etag, nc.Marshal()); err != nil {
		// Another process's WAL replay can publish the record we just wrote
		// before our own CAS lands (replay does not take the commit lease).
		// The revision then exists exactly as we built it, so report success
		// instead of 409 for a commit that is already durable -- otherwise the
		// client retries an already-applied change and every retry is rejected
		// as out of date.
		if errors.Is(err, store.ErrPreconditionFailed) && r.publishedByReplay(ctx, N, lease.token, rbBytes, cur.Head) {
			r.setHead(N)
			r.cache.Clear()
		} else {
			return 0, err
		}
	} else {
		r.setHead(N)
		// invalidate cached HEAD-dependent state
		r.cache.Clear()
	}
	phase("cas")
	// Auto-pack the just-completed shard: when N is the last revision of a
	// shard, its predecessor shard is complete and gets packed. This keeps the
	// repository at ~O(shards) loose objects instead of O(revisions) — required
	// to scale to millions of revisions.
	if N > 0 && N%r.shardSz == 0 {
		if err := r.packShardLocked(r.shard(N) - 1); err != nil {
			return N, err
		}
	}
	return N, nil
}

// journalOwns reports whether the durable WAL record for rev still describes
// this writer's revision block and carries its lease token.
func (r *Repo) journalOwns(ctx context.Context, rev uint64, token string, rbBytes []byte) bool {
	if !r.journal {
		return false
	}
	b, err := r.store.Get(ctx, r.keys.Journal(rev), nil)
	if err != nil {
		return false
	}
	rec, derr := DecodeCommitRecord(b)
	if derr != nil || rec.Rev != rev || rec.Lease != token {
		return false
	}
	sum := sha256.Sum256(rbBytes)
	return rec.RevBlockSHA == hex.EncodeToString(sum[:])
}

// publishedByReplay reports whether rev is already the published HEAD and the
// journal record for it describes this commit: same parent, same lease token
// and the same revision block. WAL replay publishes a record once it is
// quorum-acked and complete, so a writer that lost the HEAD CAS to that replay
// can safely treat its commit as successful.
func (r *Repo) publishedByReplay(ctx context.Context, rev uint64, token string, rbBytes []byte, parent uint64) bool {
	if !r.journal {
		return false
	}
	cur, _, err := r.readCurrentStrict()
	if err != nil || cur.Head != rev {
		return false
	}
	b, err := r.store.Get(ctx, r.keys.Journal(rev), nil)
	if err != nil {
		return false
	}
	rec, derr := DecodeCommitRecord(b)
	if derr != nil || rec.Rev != rev || rec.Parent != parent || rec.Lease != token {
		return false
	}
	sum := sha256.Sum256(rbBytes)
	return rec.RevBlockSHA == hex.EncodeToString(sum[:])
}

// VerifyIssue describes one integrity failure found by Verify.
type VerifyIssue struct {
	Path        string
	Rev         uint64
	Err         error
	Rebuildable bool // text delta whose base+delta are intact => can be rebuilt
}

// Verify walks the HEAD tree, decoding every node-rev (CRC-checked on read)
// and every file text (delta chain applied), and reports any corruption.
// Rebuildable=true marks a text whose delta base chain is still readable, so a
// repair pass could re-derive the payload; other failures need external
// recovery (mirror copy / backup).
func (r *Repo) Verify(headRev uint64) ([]VerifyIssue, error) {
	var issues []VerifyIssue
	var walk func(path string, rev uint64)
	walk = func(path string, rev uint64) {
		nr, err := r.Resolve(rev, path)
		if err != nil {
			issues = append(issues, VerifyIssue{Path: path, Rev: rev, Err: err})
			return
		}
		if nr.Kind == KindDir {
			children, err := r.ListDir(rev, path)
			if err != nil {
				issues = append(issues, VerifyIssue{Path: path, Rev: rev, Err: err})
				return
			}
			for _, c := range children {
				walk(strings.TrimRight(path, "/")+"/"+c.Name, rev)
			}
			return
		}
		if _, err := r.GetText(nr.Node); err != nil {
			issues = append(issues, VerifyIssue{
				Path:        path,
				Rev:         rev,
				Err:         err,
				Rebuildable: nr.TextRep == TextRepDelta && nr.Base != nil,
			})
		}
	}
	walk("/", headRev)
	return issues, nil
}

// VerifyRange checks every revision block in [from,to], every node-rev it
// contains, and (texts=true) every file text, including historical revisions
// that are no longer reachable from HEAD. Nodes shared by several revisions
// are checked once. With texts=false it still verifies that external text
// objects exist.
func (r *Repo) VerifyRange(from, to uint64, texts bool) ([]VerifyIssue, error) {
	return r.VerifyRangeWith(from, to, texts, 1)
}

// VerifyRangeWith is VerifyRange with a worker count. Workers read disjoint
// revision ranges, so a 10M-revision repository is checked in parallel instead
// of one S3 round trip at a time. Segment indexes are validated first: a
// dropped index entry would make whole revision ranges unreadable while every
// remaining object still verifies.
func (r *Repo) VerifyRangeWith(from, to uint64, texts bool, workers int) ([]VerifyIssue, error) {
	return r.VerifyRangeWithProgress(from, to, texts, workers, nil)
}

// VerifyRangeWithProgress is VerifyRangeWith plus a callback invoked after each
// revision. Long walks (10M revisions) need progress reporting; the callback is
// called from worker goroutines, so it must be cheap and concurrency-safe.
func (r *Repo) VerifyRangeWithProgress(from, to uint64, texts bool, workers int,
	progress func(done uint64)) ([]VerifyIssue, error) {
	if from == 0 {
		from = 1
	}
	head, err := r.HeadFresh()
	if err != nil {
		return nil, err
	}
	if to == 0 || to > head {
		to = head
	}
	if from > to {
		return nil, nil
	}
	// Full segment reads: a ranged read can only check the per-entry CRC, while
	// verify must confirm the index SHA-256 of every segment object.
	r.fullReads.Store(true)
	defer r.fullReads.Store(false)
	var done atomic.Uint64
	tick := func() {}
	if progress != nil {
		tick = func() { progress(done.Add(1)) }
	}

	var issues []VerifyIssue
	if r.segments {
		idxIssues, ierr := r.verifySegmentIndexes(head)
		if ierr != nil {
			return nil, ierr
		}
		issues = append(issues, idxIssues...)
	}
	if workers <= 1 || to-from < 8 {
		more, err := r.verifyRangeSeq(from, to, texts, tick)
		if err != nil {
			return nil, err
		}
		issues = append(issues, more...)
		sort.SliceStable(issues, func(i, j int) bool { return issues[i].Rev < issues[j].Rev })
		return issues, nil
	}
	if workers > 64 {
		workers = 64
	}
	span := (to - from + 1 + uint64(workers) - 1) / uint64(workers)
	results := make([][]VerifyIssue, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		lo := from + uint64(i)*span
		if lo > to {
			break
		}
		hi := lo + span - 1
		if hi > to {
			hi = to
		}
		wg.Add(1)
		go func(i int, lo, hi uint64) {
			defer wg.Done()
			results[i], errs[i] = r.verifyRangeSeq(lo, hi, texts, tick)
		}(i, lo, hi)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	for _, res := range results {
		issues = append(issues, res...)
	}
	sort.SliceStable(issues, func(i, j int) bool { return issues[i].Rev < issues[j].Rev })
	return issues, nil
}

// verifyRangeSeq walks [from,to] single-threaded. Callers own the fullReads
// mode and any index-level checks.
func (r *Repo) verifyRangeSeq(from, to uint64, texts bool, tick func()) ([]VerifyIssue, error) {
	var issues []VerifyIssue
	seen := make(map[NodeID]bool)
	for rev := from; rev <= to; rev++ {
		if tick != nil {
			tick()
		}
		rb, berr := r.readBlock(rev)
		if berr != nil {
			issues = append(issues, VerifyIssue{Rev: rev, Err: berr})
			continue
		}
		issues = append(issues, r.verifyRevisionSemantics(rev, rb)...)
		if texts && len(rb.nodeTbl) >= verifyParallelNodes {
			r.verifyNodesParallel(rev, rb, seen, &issues)
		} else {
			for _, e := range rb.nodeTbl {
				nid := NodeID{Rev: rb.Rev, ID: e.ID}
				if seen[nid] {
					continue
				}
				seen[nid] = true
				issues = append(issues, r.verifyNode(rev, nid, texts)...)
			}
		}
	}
	return issues, nil
}

// verifyNode checks one node-rev: external texts/chunks/dir buckets exist (and,
// with texts, hash correctly) and the file text reconstructs to the recorded
// size and md5.
func (r *Repo) verifyNode(rev uint64, nid NodeID, texts bool) []VerifyIssue {
	ctx := context.Background()
	var out []VerifyIssue
	nr, nerr := r.GetNodeRev(nid)
	if nerr != nil {
		return append(out, VerifyIssue{Rev: rev, Err: nerr})
	}
	if nr.Kind == KindDir {
		if nr.TextRep != TextRepExternal || nr.TextRef == "" {
			return out
		}
		table, terr := r.dirTableOf(nr)
		if terr != nil {
			return append(out, VerifyIssue{Rev: rev, Err: terr})
		}
		refs := append([]string{nr.TextRef}, table.Refs[:]...)
		for _, ref := range refs {
			if ref == "" {
				continue
			}
			key := r.keys.Dir(ref)
			ok, eerr := r.store.Exists(ctx, key)
			if eerr != nil || !ok {
				out = append(out, VerifyIssue{Rev: rev, Err: fmt.Errorf("dir object %s missing", ref)})
				continue
			}
			if texts {
				b, gerr := r.store.Get(ctx, key, nil)
				if gerr != nil {
					out = append(out, VerifyIssue{Rev: rev, Err: gerr})
					continue
				}
				sum := sha256.Sum256(b)
				if hex.EncodeToString(sum[:]) != ref {
					out = append(out, VerifyIssue{Rev: rev, Err: fmt.Errorf("dir object %s checksum mismatch", ref)})
				}
			}
		}
		return out
	}
	if nr.Kind != KindFile {
		return out
	}
	if nr.TextRep == TextRepChunked {
		refs, perr := cdc.Parse(nr.Text)
		if perr != nil {
			return append(out, VerifyIssue{Rev: rev, Err: perr})
		}
		var sum uint64
		bad := false
		for _, ref := range refs {
			sum += uint64(ref.Size)
			key := r.keys.Chunk(cdc.HashHex(ref))
			ok, eerr := r.store.Exists(ctx, key)
			if eerr != nil || !ok {
				out = append(out, VerifyIssue{Rev: rev, Err: fmt.Errorf("chunk %s missing", cdc.HashHex(ref))})
				bad = true
				continue
			}
			if !texts {
				continue
			}
			b, gerr := r.store.Get(ctx, key, nil)
			if gerr != nil {
				out = append(out, VerifyIssue{Rev: rev, Err: gerr})
				bad = true
				continue
			}
			if uint32(len(b)) != ref.Size || sha256.Sum256(b) != ref.Hash {
				out = append(out, VerifyIssue{Rev: rev, Err: fmt.Errorf("chunk %s checksum mismatch", cdc.HashHex(ref))})
				bad = true
			}
		}
		if !bad && nr.TotalSize > 0 && sum != nr.TotalSize {
			out = append(out, VerifyIssue{Rev: rev, Err: fmt.Errorf("chunked text size %d, want %d", sum, nr.TotalSize)})
		}
		return out
	}
	if nr.TextRep == TextRepExternal {
		ok, eerr := r.store.Exists(ctx, r.keys.Text(nr.TextRef))
		if eerr != nil || !ok {
			return append(out, VerifyIssue{Rev: rev, Err: fmt.Errorf("text object %s missing", nr.TextRef)})
		}
	}
	if !texts {
		return out
	}
	data, gerr := r.GetText(nr.Node)
	if gerr != nil {
		return append(out, VerifyIssue{Rev: rev, Err: gerr,
			Rebuildable: nr.TextRep == TextRepDelta && nr.Base != nil})
	}
	if nr.TotalSize > 0 && uint64(len(data)) != nr.TotalSize {
		return append(out, VerifyIssue{Rev: rev, Err: fmt.Errorf("text size %d, want %d", len(data), nr.TotalSize)})
	}
	if nr.TextMD5 != ([16]byte{}) {
		sum := md5.Sum(data)
		if sum != nr.TextMD5 {
			out = append(out, VerifyIssue{Rev: rev, Err: fmt.Errorf("text md5 mismatch")})
		}
	}
	return out
}

// verifyNodesParallel verifies the nodes of one revision with a small worker
// pool. A revision created by a large import carries thousands of files, and
// with texts enabled each is an object read; doing them serially made that
// revision the slowest part of `verify --texts`.
const verifyParallelNodes = 8

// verifyTextWorkers bounds concurrent node checks inside one revision. It is a
// variable so operators can tune it for their backend (more for a fast object
// store, fewer for a weak link or a memory-constrained host).
var verifyTextWorkers atomic.Int64

func init() { verifyTextWorkers.Store(4) }

// verifyReadSlots bounds concurrent text verifications across every worker,
// range and repository in the process. `--workers` times `--text-workers` is
// otherwise unbounded, and a weak or shared object store must not be hit by
// 64 simultaneous GETs because two tuning knobs were raised.
var verifyReadSlots = make(chan struct{}, 32)

// SetVerifyTextWorkers changes the per-revision text-check concurrency.
func SetVerifyTextWorkers(n int) {
	if n < 1 {
		n = 1
	}
	if n > 32 {
		n = 32
	}
	verifyTextWorkers.Store(int64(n))
}

func (r *Repo) verifyNodesParallel(rev uint64, rb *RevBlock, seen map[NodeID]bool, issues *[]VerifyIssue) {
	var batch []NodeID
	for _, e := range rb.nodeTbl {
		nid := NodeID{Rev: rb.Rev, ID: e.ID}
		if seen[nid] {
			continue
		}
		seen[nid] = true
		batch = append(batch, nid)
	}
	sem := make(chan struct{}, int(verifyTextWorkers.Load()))
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, nid := range batch {
		wg.Add(1)
		sem <- struct{}{}
		go func(nid NodeID) {
			defer wg.Done()
			defer func() { <-sem }()
			verifyReadSlots <- struct{}{}
			defer func() { <-verifyReadSlots }()
			res := r.verifyNode(rev, nid, true)
			if len(res) == 0 {
				return
			}
			mu.Lock()
			*issues = append(*issues, res...)
			mu.Unlock()
		}(nid)
	}
	wg.Wait()
}

// verifyRevisionSemantics checks the invariants that survive byte-level
// validation: the block must belong to its revision, follow the previous one,
// name a root node written at this revision, and carry readable properties.
// A segment whose entries were mapped to the wrong revisions still passes the
// CRC and SHA checks, so these are checked separately.
func (r *Repo) verifyRevisionSemantics(rev uint64, rb *RevBlock) []VerifyIssue {
	var out []VerifyIssue
	if rb.Rev != rev {
		out = append(out, VerifyIssue{Rev: rev, Err: fmt.Errorf("block belongs to revision %d", rb.Rev)})
	}
	wantParent := uint64(0)
	if rev > 0 {
		wantParent = rev - 1
	}
	if rb.PrevRev != wantParent {
		out = append(out, VerifyIssue{Rev: rev, Err: fmt.Errorf("parent revision %d, want %d", rb.PrevRev, wantParent)})
	}
	if rb.Root.Rev != rev {
		out = append(out, VerifyIssue{Rev: rev, Err: fmt.Errorf("root node is from revision %d", rb.Root.Rev)})
	}
	if _, err := r.loadRevProps(rev); err != nil {
		out = append(out, VerifyIssue{Rev: rev, Err: fmt.Errorf("revision properties: %w", err)})
	}
	return out
}

// applyChange mutates the working tree for one Change.
func (r *Repo) applyChange(root *tnode, ch *Change) error {
	if ch.Noop {
		return nil
	}
	segs := segments(ch.Path)
	if len(segs) == 0 {
		return errors.New("fs: empty path in change")
	}
	parent := root
	for _, seg := range segs[:len(segs)-1] {
		if err := r.ensureDir(parent); err != nil {
			return err
		}
		child, ok := parent.children[seg]
		if !ok {
			child = &tnode{kind: KindDir, props: map[string]string{}, children: map[string]*tnode{}, dirty: true}
			parent.children[seg] = child
		}
		parent.dirty = true
		parent = child
	}
	name := segs[len(segs)-1]

	switch ch.Action {
	case ActionDelete:
		if err := r.ensureDir(parent); err != nil {
			return err
		}
		if _, ok := parent.children[name]; !ok {
			return errors.New("fs: delete of nonexistent path " + ch.Path)
		}
		delete(parent.children, name)
		parent.dirty = true
	case ActionAdd, ActionModify:
		if err := r.ensureDir(parent); err != nil {
			return err
		}
		if ch.Kind == KindFile {
			t := &tnode{kind: KindFile, props: copyProps(ch.Props), data: append([]byte(nil), ch.Data...),
				dataFile: ch.DataFile, dataMD5: ch.DataMD5, hasMD5: ch.HasMD5,
				dataSHA1: ch.DataSHA1, hasSHA1: ch.HasSHA1,
				textRef: ch.TextRef, textSize: ch.TextSize,
				chunkManifest: append([]byte(nil), ch.ChunkManifest...), chunkTotal: ch.ChunkTotal,
				dirty: true}
			if len(ch.Delta) > 0 {
				t.deltaRep = true
				t.delta = append([]byte(nil), ch.Delta...)
				t.deltaBase = ch.DeltaBase
				t.deltaTotal = ch.TextTotal
			}
			if old, ok := parent.children[name]; ok && old.kind == KindFile {
				// A streamed body becomes an external full text, a delta
				// change keeps only its delta, and a chunked change carries a
				// manifest + pre-stored chunks; none needs the old text.
				if ch.DataFile == "" && len(ch.Delta) == 0 && len(ch.ChunkManifest) == 0 {
					if err := r.ensureData(old); err != nil {
						return err
					}
				}
				t.id = old.id
				t.pred = old.pred
				t.deltaDepth = old.deltaDepth
				t.textRep = old.textRep
				t.base = old.base
				t.baseText = old.data
				t.copyFrom = old.copyFrom
				t.copyFromPath = old.copyFromPath
				// A props-only change (PROPPATCH) carries no new text; keep the
				// base revision's content so the commit does not empty the file.
				// A chunk manifest is new text and must not be replaced, and an
				// explicit text-less PUT (TextSet) means the file is now empty.
				if !ch.TextSet && len(t.data) == 0 && t.dataFile == "" && !t.deltaRep && len(t.chunkManifest) == 0 {
					t.data = append([]byte(nil), old.data...)
				}
				// A content-only change (PUT with no PROPPATCH) carries no
				// props; keep the base revision's props so svn:mime-type etc
				// survive a subsequent content edit.
				if len(ch.Props) == 0 {
					t.props = copyProps(old.props)
				}
			}
			parent.children[name] = t
		} else {
			t := &tnode{kind: KindDir, props: copyProps(ch.Props), children: map[string]*tnode{}, dirty: true}
			if old, ok := parent.children[name]; ok && old.kind == KindDir {
				if err := r.ensureDir(old); err != nil {
					return err
				}
				t.id = old.id
				t.pred = old.pred
				t.deltaDepth = old.deltaDepth
				t.textRep = old.textRep
				t.base = old.base
				t.copyFrom = old.copyFrom
				t.copyFromPath = old.copyFromPath
				t.pagedRef = old.pagedRef
				t.priorTableRef = old.pagedRef
				t.priorChildren = old.children
				for n, c := range old.children {
					t.children[n] = c
				}
				if len(ch.Props) == 0 {
					t.props = copyProps(old.props)
				}
			}
			parent.children[name] = t
		}
		parent.dirty = true
	case ActionCopy:
		src, err := r.resolveNode(root, ch.CopyFromPath)
		if err != nil {
			// Source is not in the working tree; materialize it from history
			// (baseline copy / branch). CopyFromRev identifies the source rev.
			src, err = r.loadHistorySubtree(ch.CopyFromRev, ch.CopyFromPath)
			if err != nil {
				return fmt.Errorf("fs: copy source %q rev %d: %w", ch.CopyFromPath, ch.CopyFromRev, err)
			}
		} else if err := r.ensureFull(src); err != nil {
			return err
		}
		cp := deepCopy(src)
		cp.dirty = true
		if ch.TextSet && cp.kind == KindFile {
			// The client edited the copied file after the COPY (svn mv of a
			// modified file): the PUT text replaces the inherited content. The
			// representations are mutually exclusive, so clear the source's.
			cp.data = append([]byte(nil), ch.Data...)
			cp.dataFile = ch.DataFile
			cp.dataMD5, cp.hasMD5 = ch.DataMD5, ch.HasMD5
			cp.dataSHA1, cp.hasSHA1 = ch.DataSHA1, ch.HasSHA1
			cp.textRef, cp.textSize = ch.TextRef, ch.TextSize
			cp.chunkManifest = append([]byte(nil), ch.ChunkManifest...)
			cp.chunkTotal = ch.ChunkTotal
			cp.deltaRep = len(ch.Delta) > 0
			cp.delta = append([]byte(nil), ch.Delta...)
			cp.deltaBase = ch.DeltaBase
			cp.deltaTotal = ch.TextTotal
			cp.base = nil
			cp.textRep = 0
			cp.deltaDepth = 0
		}
		if len(ch.Props) > 0 {
			// ch.Props is the full map for a property change handled by the
			// server, but a PROPPATCH that arrived before the COPY carries only
			// the patched entries; merge so inherited props are not lost.
			if cp.props == nil {
				cp.props = map[string]string{}
			}
			for k, v := range ch.Props {
				cp.props[k] = v
			}
		}
		// copyfrom is the source as of the revision the client asked for
		// (the COPY URL's peg). Resolving that revision gives the node svn
		// would have recorded; children updated after it then differ from the
		// parent's copy source and show up as replaced, exactly as svn
		// reports a working-copy copy with mixed base revisions.
		if ch.CopyFromRev > 0 {
			if snr, serr := r.Resolve(ch.CopyFromRev, ch.CopyFromPath); serr == nil {
				cp.copyFrom = &snr.Node
			} else {
				cp.copyFrom = &src.id
			}
		} else {
			cp.copyFrom = &src.id
		}
		cp.copyFromPath = ch.CopyFromPath
		cp.copyFromRev = ch.CopyFromRev
		if err := r.ensureDir(parent); err != nil {
			return err
		}
		parent.children[name] = cp
		parent.dirty = true
	}
	return nil
}

// loadHistorySubtree loads a node (and, for a directory, its whole subtree)
// from a historical revision into a tnode so it can be copied by reference.
func (r *Repo) loadHistorySubtree(rev uint64, path string) (*tnode, error) {
	nr, err := r.Resolve(rev, path)
	if err != nil {
		return nil, err
	}
	t := &tnode{
		kind: nr.Kind, props: copyProps(nr.Props), id: nr.Node, pred: nr.Pred,
		deltaDepth: nr.DeltaDepth, textRep: nr.TextRep, base: nr.Base,
		copyFromPath: nr.CopyFromPath,
	}
	if nr.CopyFrom != nil {
		cf := *nr.CopyFrom
		t.copyFrom = &cf
	}
	if nr.Kind == KindFile {
		txt, err := r.GetText(nr.Node)
		if err != nil {
			return nil, err
		}
		t.data = txt
		return t, nil
	}
	t.children = map[string]*tnode{}
	list, err := r.ListDir(rev, path)
	if err != nil {
		return nil, err
	}
	base := strings.TrimSuffix(path, "/")
	for _, ci := range list {
		child, err := r.loadHistorySubtree(rev, base+"/"+ci.Name)
		if err != nil {
			return nil, err
		}
		t.children[ci.Name] = child
	}
	return t, nil
}

// resolveNode finds a tnode in the working tree by path.
func (r *Repo) resolveNode(root *tnode, path string) (*tnode, error) {
	cur := root
	for _, seg := range segments(path) {
		if err := r.ensureDir(cur); err != nil {
			return nil, err
		}
		next, ok := cur.children[seg]
		if !ok {
			return nil, errors.New("no such node")
		}
		cur = next
	}
	return cur, nil
}

func deepCopy(t *tnode) *tnode {
	c := &tnode{
		kind: t.kind, props: copyProps(t.props), id: t.id, pred: t.pred,
		deltaDepth: t.deltaDepth, textRep: t.textRep, base: t.base,
		copyFromPath: t.copyFromPath,
	}
	if t.copyFrom != nil {
		cf := *t.copyFrom
		c.copyFrom = &cf
	}
	if t.kind == KindFile {
		c.data = append([]byte(nil), t.data...)
		c.baseText = append([]byte(nil), t.baseText...)
	} else {
		c.children = make(map[string]*tnode, len(t.children))
		for n, ch := range t.children {
			c.children[n] = deepCopy(ch)
		}
	}
	return c
}

var _ = strings.TrimSpace

// PackShard consolidates a completed shard's loose revision objects into a
// single immutable pack + idx + revprops pack, then switches the shard to
// packed via the .done marker. Reads before .done still use loose objects;
// loose objects become unreachable after the marker and are GC's later.
func (r *Repo) PackShard(shard uint64) error {
	// Packing moves revprops from loose objects into the shard pack. Serialise
	// with commits and revprop updates: a concurrent update that saw "loose"
	// would write the object the pack is about to replace, and its change would
	// be invisible afterwards.
	lease, lerr := r.acquireCommitLease(context.Background())
	if lerr != nil {
		return lerr
	}
	defer lease.Release(r.store)
	return r.packShardLocked(shard)
}

// packShardLocked is PackShard for callers that already hold the commit lease
// (CommitTxn auto-packs a completed shard while still holding it).
func (r *Repo) packShardLocked(shard uint64) error {
	ctx := context.Background()
	// Repacking an already packed shard would rewrite its revprops pack from
	// the loose copies, discarding a revprop change made while packed. Packing
	// is a one-way step.
	if packed, err := r.isPacked(shard); err != nil {
		return err
	} else if packed {
		return nil
	}
	cur, _, err := r.readCurrent()
	if err != nil {
		return err
	}
	firstRev := shard * r.shardSz
	lastRev := firstRev + r.shardSz - 1
	if lastRev > cur.Head {
		lastRev = cur.Head
	}
	if firstRev > lastRev {
		return nil
	}
	var packBuf bytes.Buffer
	var entries []revTableEntry
	revProps := map[uint64]*RevProps{}
	for rev := firstRev; rev <= lastRev; rev++ {
		data, err := r.store.Get(ctx, r.keys.LooseRev(shard, rev), nil)
		if err != nil {
			return err
		}
		entries = append(entries, revTableEntry{BlockOffset: uint64(packBuf.Len()), BlockLen: uint64(len(data))})
		packBuf.Write(data)
		rpb, err := r.store.Get(ctx, r.keys.LooseRevProps(shard, rev), nil)
		if err != nil {
			return err
		}
		rp, err := UnmarshalRevProps(rpb)
		if err != nil {
			return err
		}
		revProps[rev] = rp
	}
	if err := r.store.Put(ctx, r.keys.PackedRev(shard), packBuf.Bytes()); err != nil {
		return err
	}
	if err := r.store.Put(ctx, r.keys.PackedIdx(shard), NewShardIdx(shard, firstRev, entries).Marshal()); err != nil {
		return err
	}
	if err := r.store.Put(ctx, r.keys.PackedRevProps(shard), PackRevProps(revProps)); err != nil {
		return err
	}
	if err := r.store.Put(ctx, r.keys.PackedDone(shard), []byte("1")); err != nil {
		return err
	}
	// Reclaim the packed shard's loose copies in the background. Deletes go to
	// the arbiter only (DeletePrimary): mirroring thousands of keys knocked a
	// standby out of service when a 1000-key DELETE exceeded its budget, and a
	// standby's leftover loose copies are harmless (repair lists only the
	// active shard).
	go func() {
		if _, err := r.collectLooseShard(context.Background(), shard, false); err != nil {
			log.Printf("pack: loose cleanup shard %d: %v", shard, err)
		}
	}()

	r.invalidateRevPropsPack(shard)
	r.cache.Clear()
	return nil
}

// externalTextThreshold: full file texts larger than this are stored as
// separate content-addressed objects (texts/<md5>). Rev blocks then stay small,
// object stores serve the data directly, and GET can stream it. Deltas stay
// inline (they are small after the encoder work).
const externalTextThreshold = 256 << 10

// maxCachedObject bounds what the LRU keeps. Full texts of large files are
// built in memory for delta application anyway, but caching them would keep
// them resident on top of that (and evict small hot blocks); repositories with
// multi-hundred-MB files otherwise grow without bound.
const maxCachedObject = 4 << 20

func cachePut(c *cache.LRU, k cache.Key, b []byte) {
	if len(b) > maxCachedObject {
		return
	}
	c.Put(k, b)
}

// GCProgress reports long-running collection progress. phase names the step
// ("loose", "texts-scan", "texts", "segments", "orphans"); done/total are item
// counts, with total 0 when the size is unknown. Callbacks may run from worker
// goroutines and must be cheap.
type GCProgress func(phase string, done, total int, bytes int64)

// CollectLoose deletes loose revision and revprops objects that belong to
// shards already packed. PackShard leaves them behind (readers switch to the
// pack via the .done marker), so storage and repair listings grew without
// bound on long-running repositories. Shard numbers and revision ranges are
// known from HEAD, so this deletes exact keys instead of listing a prefix that
// may hold tens of thousands of objects.
func (r *Repo) CollectLoose(ctx context.Context) (int, error) {
	return r.CollectLooseProgress(ctx, false, nil)
}

// CollectLooseProgress is CollectLoose with phase progress. dryRun counts what
// would be removed without touching the store.
func (r *Repo) CollectLooseProgress(ctx context.Context, dryRun bool, progress GCProgress) (int, error) {
	if r.segments {
		// Segment repositories have no per-revision loose objects (revision 0
		// stays loose but is tiny); compaction reclaims old segments instead.
		return 0, nil
	}
	head, err := r.HeadFresh()
	if err != nil {
		return 0, err
	}
	deleted := 0
	var bytes int64
	for shard := uint64(0); shard <= r.shard(head); shard++ {
		packed, err := r.isPacked(shard)
		if err != nil || !packed {
			continue
		}
		if dryRun {
			// A dry run must not report the shard's theoretical key count: list
			// the loose prefixes to count (and size) what actually remains.
			n, b, cerr := r.countLooseShard(ctx, shard)
			if cerr != nil {
				return deleted, cerr
			}
			deleted += n
			bytes += b
			if progress != nil {
				progress("loose", deleted, 0, bytes)
			}
			continue
		}
		n, err := r.collectLooseShard(ctx, shard, false)
		deleted += n
		if progress != nil {
			progress("loose", deleted, 0, 0)
		}
		if err != nil {
			return deleted, err
		}
	}
	return deleted, nil
}

// collectLooseShard removes the loose rev/revprops keys of one packed shard.
// Deletes are independent and run concurrently: RustFS needs seconds for a
// 1000-key batch, and a serial pass over 625k revisions would take hours.
// deleteLoose uses the primary-only path when the store is a mirror.
// deletePrimaryOrAll removes keys from the authoritative replica when the store
// has one. Bulk cleanup (superseded segments, packed loose objects) uses it: the
// standby copies are unreferenced and harmless, while mirroring thousands of
// deletes competes with commits and can exceed the standby write budget, taking
// a live node out of service.
func (r *Repo) deletePrimaryOrAll(ctx context.Context, keys []string) error {
	if pd, ok := r.store.(interface {
		DeletePrimary(context.Context, []string) error
	}); ok {
		return pd.DeletePrimary(ctx, keys)
	}
	return r.store.Delete(ctx, keys)
}

func (r *Repo) deleteLoose(ctx context.Context, keys []string) error {
	return r.deletePrimaryOrAll(ctx, keys)
}

func (r *Repo) collectLooseShard(ctx context.Context, shard uint64, dryRun bool) (int, error) {
	first := shard * r.shardSz
	last := first + r.shardSz - 1
	var batches [][]string
	var batch []string
	for rev := first; rev <= last; rev++ {
		batch = append(batch, r.keys.LooseRev(shard, rev), r.keys.LooseRevProps(shard, rev))
		if len(batch) >= 1000 {
			batches = append(batches, batch)
			batch = nil
		}
	}
	if len(batch) > 0 {
		batches = append(batches, batch)
	}
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	var mu sync.Mutex
	deleted := 0
	var firstErr error
	for _, b := range batches {
		wg.Add(1)
		go func(b []string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if dryRun {
				mu.Lock()
				deleted += len(b)
				mu.Unlock()
				return
			}
			if err := r.deleteLoose(ctx, b); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			mu.Lock()
			deleted += len(b)
			mu.Unlock()
		}(b)
	}
	wg.Wait()
	return deleted, firstErr
}

// CollectTxnData deletes content-addressed commit-session bodies that no
// activity manifest references any more, with no grace period.
func (r *Repo) CollectTxnData(ctx context.Context) (int, error) {
	return r.CollectTxnDataGrace(ctx, 0)
}

// CollectTxnDataGrace is CollectTxnData with a minimum object age. A fragment
// body is written before the manifest that references it is updated, so a GC
// pass racing a native commit could otherwise delete a live body.
func (r *Repo) CollectTxnDataGrace(ctx context.Context, grace time.Duration) (int, error) {
	return r.collectTxnDataGrace(ctx, grace, false)
}

// CollectTxnDataDry reports how many objects the transaction sweep would remove
// without deleting them.
func (r *Repo) CollectTxnDataDry(ctx context.Context, grace time.Duration) (int, error) {
	return r.collectTxnDataGrace(ctx, grace, true)
}

func (r *Repo) collectTxnDataGrace(ctx context.Context, grace time.Duration, dryRun bool) (int, error) {
	prefix := r.keys.ListingPrefix("dav-txns")
	refRe := regexp.MustCompile(`"dataRef":"([0-9a-f]+)"`)
	referenced := map[string]bool{}
	activityIDs := map[string]bool{}
	var dataKeys []store.ListedObj
	type changeFragment struct {
		act string
		obj store.ListedObj
	}
	var changeFrags []changeFragment
	next := ""
	for {
		objs, n, err := r.store.ListPrefix(ctx, prefix, next, 500)
		if err != nil {
			return 0, err
		}
		for _, o := range objs {
			rel := strings.TrimPrefix(o.Key, prefix)
			if strings.HasPrefix(rel, "data/") {
				dataKeys = append(dataKeys, o)
				continue
			}
			if id, ok := changeFragmentID(rel); ok {
				changeFrags = append(changeFrags, changeFragment{act: id, obj: o})
				continue
			}
			if !strings.HasSuffix(rel, ".json") {
				continue
			}
			activityIDs[strings.TrimSuffix(strings.TrimPrefix(rel, "activity-"), ".json")] = true
			b, err := r.store.Get(ctx, o.Key, nil)
			if err != nil {
				continue
			}
			for _, m := range refRe.FindAllSubmatch(b, -1) {
				referenced[string(m[1])] = true
			}
		}
		if n == "" {
			break
		}
		next = n
	}
	var victims []string
	var victimBytes int64
	for _, k := range dataKeys {
		ref := strings.TrimPrefix(k.Key, prefix+"data/")
		if !referenced[ref] {
			if tooYoung(k, grace) {
				continue
			}
			victims = append(victims, k.Key)
		}
	}
	// Staged change fragments whose activity manifest is gone belong to a
	// commit that finished (or died) and whose background cleanup did not run
	// to completion (process exit). They are unreachable: a restore needs the
	// manifest.
	for _, cf := range changeFrags {
		if activityIDs[cf.act] || tooYoung(cf.obj, grace) {
			continue
		}
		victims = append(victims, cf.obj.Key)
		victimBytes += cf.obj.Size
	}
	if len(victims) == 0 {
		return 0, nil
	}
	if !dryRun {
		if err := r.store.Delete(ctx, victims); err != nil {
			return 0, err
		}
	}
	return len(victims), nil
}

// tooYoung reports whether an un-referenced object is too new to collect. A
// zero ModTime means the backend cannot report age; those objects are treated
// as old so tests and stores without metadata keep deterministic behaviour.
func tooYoung(o store.ListedObj, grace time.Duration) bool {
	return grace > 0 && !o.ModTime.IsZero() && time.Since(o.ModTime) < grace
}

// textRefs is one worker's slice of the reference scan.
type textRefs struct {
	texts, chunks, dirs map[string]bool
	dirScanOK           bool
}

// collectTextRefs builds the reference sets for revisions [from,to].
func (r *Repo) collectTextRefs(from, to uint64) (textRefs, error) {
	out := textRefs{
		texts: map[string]bool{}, chunks: map[string]bool{}, dirs: map[string]bool{},
		dirScanOK: true,
	}
	for rev := from; rev <= to; rev++ {
		rb, err := r.readBlock(rev)
		if err != nil {
			return out, err
		}
		for _, id := range rb.NodeIDs() {
			nr, err := rb.GetNode(id)
			if err != nil {
				return out, err
			}
			if nr.Kind == KindFile && nr.TextRep == TextRepExternal && nr.TextRef != "" {
				out.texts[nr.TextRef] = true
			}
			if nr.Kind == KindFile && nr.TextRep == TextRepChunked {
				if crefs, err := cdc.Parse(nr.Text); err == nil {
					for _, ref := range crefs {
						out.chunks[cdc.HashHex(ref)] = true
					}
				}
			}
			if nr.Kind == KindDir && nr.TextRep == TextRepExternal && nr.TextRef != "" {
				out.dirs[nr.TextRef] = true
				table, terr := r.dirTableOf(nr)
				if terr != nil || table == nil {
					// Fail closed: without the table we cannot know which
					// buckets are referenced, so do not collect dir objects.
					log.Printf("gc: cannot read dir table %s at rev %d: %v", nr.TextRef, rev, terr)
					out.dirScanOK = false
					continue
				}
				for _, ref := range table.Refs {
					if ref != "" {
						out.dirs[ref] = true
					}
				}
			}
		}
	}
	return out, nil
}

// CollectTexts deletes external text objects that no node-rev references,
// with no grace period (tests and an explicit offline pass).
func (r *Repo) CollectTexts(ctx context.Context) (int, error) {
	return r.CollectTextsGrace(ctx, 0)
}

// CollectTextsGrace is CollectTexts with a minimum object age. An upload
// writes its texts and chunks before the commit that references them, so a
// GC pass racing an upload would otherwise delete live data. Objects younger
// than grace are left for the next pass.
func (r *Repo) CollectTextsGrace(ctx context.Context, grace time.Duration) (int, error) {
	return r.CollectTextsGraceProgress(ctx, grace, false, nil)
}

// CollectTextsGraceProgress is CollectTextsGrace with phase progress. dryRun
// reports what would be removed without deleting.
func (r *Repo) CollectTextsGraceProgress(ctx context.Context, grace time.Duration, dryRun bool, progress GCProgress) (int, error) {
	head, err := r.Head()
	if err != nil {
		return 0, err
	}
	// Walk the history in parallel: the scan only reads immutable blocks and
	// tables, and on a 200k-revision repository it dominates `gc -texts`.
	const scanWorkers = 8
	total := head + 1
	workers := scanWorkers
	if uint64(workers) > total {
		workers = int(total)
	}
	parts := make([]textRefs, workers)
	partErrs := make([]error, workers)
	span := (total + uint64(workers) - 1) / uint64(workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		lo := uint64(i) * span
		hi := lo + span - 1
		if hi > head {
			hi = head
		}
		if lo > head {
			break
		}
		wg.Add(1)
		go func(i int, lo, hi uint64) {
			defer wg.Done()
			parts[i], partErrs[i] = r.collectTextRefs(lo, hi)
		}(i, lo, hi)
	}
	wg.Wait()
	refs := map[string]bool{}
	chunkRefs := map[string]bool{}
	dirRefs := map[string]bool{}
	dirScanOK := true
	for i := range parts {
		if partErrs[i] != nil {
			return 0, partErrs[i]
		}
		for k := range parts[i].texts {
			refs[k] = true
		}
		for k := range parts[i].chunks {
			chunkRefs[k] = true
		}
		for k := range parts[i].dirs {
			dirRefs[k] = true
		}
		if !parts[i].dirScanOK {
			dirScanOK = false
		}
	}
	if progress != nil {
		progress("texts-scan", int(head+1), int(head+1), 0)
	}
	prefix := r.keys.ListingPrefix("texts")
	next := ""
	var victims []string
	var victimBytes int64
	for {
		objs, n, err := r.store.ListPrefix(ctx, prefix, next, 500)
		if err != nil {
			return 0, err
		}
		for _, o := range objs {
			ref := strings.TrimPrefix(o.Key, prefix)
			if !refs[ref] {
				if tooYoung(o, grace) {
					continue
				}
				victims = append(victims, o.Key)
				victimBytes += o.Size
			}
		}
		if n == "" {
			break
		}
		next = n
	}
	chunkPrefix := r.keys.ListingPrefix("chunks")
	next = ""
	for {
		objs, n, err := r.store.ListPrefix(ctx, chunkPrefix, next, 500)
		if err != nil {
			return 0, err
		}
		for _, o := range objs {
			ref := strings.TrimPrefix(o.Key, chunkPrefix)
			if !chunkRefs[ref] {
				if tooYoung(o, grace) {
					continue
				}
				victims = append(victims, o.Key)
				victimBytes += o.Size
			}
		}
		if n == "" {
			break
		}
		next = n
	}
	if dirScanOK {
		dirPrefix := r.keys.ListingPrefix("dirs")
		next = ""
		for {
			objs, n, err := r.store.ListPrefix(ctx, dirPrefix, next, 500)
			if err != nil {
				return 0, err
			}
			for _, o := range objs {
				ref := strings.TrimPrefix(o.Key, dirPrefix)
				if !dirRefs[ref] {
					if tooYoung(o, grace) {
						continue
					}
					victims = append(victims, o.Key)
					victimBytes += o.Size
				}
			}
			if n == "" {
				break
			}
			next = n
		}
	}
	if len(victims) == 0 {
		return 0, nil
	}
	if !dryRun {
		if err := r.store.Delete(ctx, victims); err != nil {
			return 0, err
		}
	}
	if progress != nil {
		progress("texts", len(victims), len(victims), victimBytes)
	}
	return len(victims), nil
}

// revPropsCacheMax bounds the per-repository revision-property cache.
const revPropsCacheMax = 4096

// GetRevProps returns a revision's properties (loose or packed). The returned
// value is shared and must not be mutated.
func (r *Repo) GetRevProps(rev uint64) (*RevProps, error) {
	r.revPropsMu.Lock()
	if rp, ok := r.revProps[rev]; ok && time.Since(r.revPropsAt[rev]) < revPropsCacheTTL {
		r.revPropsMu.Unlock()
		return rp, nil
	}
	r.revPropsMu.Unlock()
	rp, err := r.loadRevProps(rev)
	if err != nil {
		return nil, err
	}
	r.cacheRevProps(rev, rp)
	return rp, nil
}

// GetRevPropsFresh bypasses both revprop caches. Use it where a stale answer
// would be user-visible, e.g. reading revision properties back after a
// propset that may have been served by another replica.
func (r *Repo) GetRevPropsFresh(rev uint64) (*RevProps, error) {
	rp, err := r.loadRevPropsFresh(rev)
	if err != nil {
		return nil, err
	}
	r.cacheRevProps(rev, rp)
	return rp, nil
}

const revPropsPackCacheMax = 4

func (r *Repo) cachedRevPropsPack(sh uint64) map[uint64]*RevProps {
	r.revPropsPackMu.Lock()
	defer r.revPropsPackMu.Unlock()
	if time.Since(r.revPropsPackAt[sh]) >= revPropsCacheTTL {
		return nil
	}
	return r.revPropsPacks[sh]
}

func (r *Repo) putRevPropsPack(sh uint64, m map[uint64]*RevProps) {
	r.revPropsPackMu.Lock()
	defer r.revPropsPackMu.Unlock()
	if r.revPropsPacks == nil {
		r.revPropsPacks = make(map[uint64]map[uint64]*RevProps)
		r.revPropsPackAt = make(map[uint64]time.Time)
	}
	if _, ok := r.revPropsPacks[sh]; !ok {
		r.revPropsPackOrder = append(r.revPropsPackOrder, sh)
		for len(r.revPropsPackOrder) > revPropsPackCacheMax {
			evict := r.revPropsPackOrder[0]
			r.revPropsPackOrder = r.revPropsPackOrder[1:]
			delete(r.revPropsPacks, evict)
		}
	}
	r.revPropsPacks[sh] = m
	r.revPropsPackAt[sh] = time.Now()
}

func (r *Repo) invalidateRevPropsPack(sh uint64) {
	r.revPropsPackMu.Lock()
	defer r.revPropsPackMu.Unlock()
	delete(r.revPropsPacks, sh)
	delete(r.revPropsPackAt, sh)
	for i, v := range r.revPropsPackOrder {
		if v == sh {
			r.revPropsPackOrder = append(r.revPropsPackOrder[:i], r.revPropsPackOrder[i+1:]...)
			break
		}
	}
}

func (r *Repo) loadRevProps(rev uint64) (*RevProps, error) {
	ctx := context.Background()
	if r.segments && rev >= r.segmentFrom {
		if rp, ok, oerr := r.segRevProps(rev, false); oerr != nil {
			return nil, oerr
		} else if ok {
			r.cacheRevProps(rev, rp)
			return rp, nil
		}
		_, props, err := r.segmentEntry(rev, false)
		if err != nil {
			return nil, err
		}
		rp, err := UnmarshalRevProps(props)
		if err == nil {
			r.cacheRevProps(rev, rp)
		}
		return rp, err
	}
	sh := r.shard(rev)
	packed, err := r.isPacked(sh)
	if err != nil {
		return nil, err
	}
	if !packed {
		b, err := r.store.Get(ctx, r.keys.LooseRevProps(sh, rev), nil)
		if err != nil {
			return nil, err
		}
		return UnmarshalRevProps(b)
	}
	if m := r.cachedRevPropsPack(sh); m != nil {
		if rp, ok := m[rev]; ok {
			return rp, nil
		}
	}
	b, err := r.store.Get(ctx, r.keys.PackedRevProps(sh), nil)
	if err != nil {
		return nil, err
	}
	m, err := ParseRevPropsPack(b)
	if err != nil {
		return nil, err
	}
	r.putRevPropsPack(sh, m)
	if rp, ok := m[rev]; ok {
		return rp, nil
	}
	return nil, errors.New("fs: revision props not found in pack")
}

// loadRevPropsFresh reads revision properties from the store, ignoring cached
// loose entries and cached packs (another replica may have rewritten either).
func (r *Repo) loadRevPropsFresh(rev uint64) (*RevProps, error) {
	ctx := context.Background()
	if r.segments && rev >= r.segmentFrom {
		if rp, ok, oerr := r.segRevProps(rev, true); oerr != nil {
			return nil, oerr
		} else if ok {
			return rp, nil
		}
		_, props, err := r.segmentEntry(rev, true)
		if err != nil {
			return nil, err
		}
		return UnmarshalRevProps(props)
	}
	sh := r.shard(rev)
	packed, err := r.isPacked(sh)
	if err != nil {
		return nil, err
	}
	if !packed {
		b, err := getFreshObject(ctx, r.store, r.keys.LooseRevProps(sh, rev))
		if err != nil {
			return nil, err
		}
		return UnmarshalRevProps(b)
	}
	b, err := getFreshObject(ctx, r.store, r.keys.PackedRevProps(sh))
	if err != nil {
		return nil, err
	}
	m, err := ParseRevPropsPack(b)
	if err != nil {
		return nil, err
	}
	r.putRevPropsPack(sh, m)
	if rp, ok := m[rev]; ok {
		return rp, nil
	}
	return nil, errors.New("fs: revision props not found in pack")
}

// changeFragmentID extracts the activity id from a dav-txns/changes/<id>/<sha>
// listing key.
func changeFragmentID(rel string) (string, bool) {
	rest, ok := strings.CutPrefix(rel, "changes/")
	if !ok {
		return "", false
	}
	i := strings.IndexByte(rest, '/')
	if i <= 0 || i == len(rest)-1 {
		return "", false
	}
	return rest[:i], true
}

// countLooseShard counts (and sizes) one packed shard's loose objects.
func (r *Repo) countLooseShard(ctx context.Context, shard uint64) (int, int64, error) {
	n := 0
	var bytes int64
	for _, prefix := range []string{r.keys.LooseRevDir(shard), r.keys.LooseRevPropsDir(shard)} {
		next := ""
		for {
			objs, cont, err := r.store.ListPrefix(ctx, prefix, next, 1000)
			if err != nil {
				return n, bytes, err
			}
			for _, o := range objs {
				n++
				bytes += o.Size
			}
			if cont == "" || cont == next {
				break
			}
			next = cont
		}
	}
	return n, bytes, nil
}
