package fs

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"svngod/internal/cache"
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

// OutOfDateError identifies the path whose working-copy revision predates a
// concurrent change. It unwraps to ErrOutOfDate so callers can keep using
// errors.Is.
type OutOfDateError struct{ Path string }

func (e *OutOfDateError) Error() string { return "fs: " + e.Path + " is out of date" }
func (e *OutOfDateError) Unwrap() error { return ErrOutOfDate }

// Repo is a versioned repository backed by an object Store.
type Repo struct {
	store    store.Store
	keys     *store.Keys
	shardSz  uint64
	maxDelta uint64

	mu    sync.Mutex
	cache *cache.LRU
	// nodeCache caches decoded node-revs by immutable id so repeated Resolve /
	// ListDir on the same revision reuse the node (and its ByName index) instead
	// of re-decoding the block and re-scanning Children each time. cache.LRU is
	// []byte-only, so this is a dedicated pointer map with its own mutex.
	nodeCacheMu sync.Mutex
	nodeCache   map[cache.Key]*NodeRev
}

// CreateRepo writes a fresh repository (format/uuid/current) and returns it.
func CreateRepo(st store.Store, prefix string, shardSize, maxDelta uint64) (*Repo, error) {
	if shardSize == 0 {
		shardSize = 4096
	}
	if maxDelta == 0 {
		maxDelta = 64
	}
	keys := store.NewKeys(prefix)
	ctx := context.Background()
	if err := st.Put(ctx, keys.Format(), (&Format{Version: 1, ShardSize: shardSize, MaxDeltaChain: maxDelta}).Marshal()); err != nil {
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
	return &Repo{
		store: st, keys: keys, shardSz: shardSize, maxDelta: maxDelta,
		cache: cache.New(1 << 16), nodeCache: make(map[cache.Key]*NodeRev),
	}, nil
}

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
	return &Repo{
		store: st, keys: keys, shardSz: f.ShardSize, maxDelta: f.MaxDeltaChain,
		cache: cache.New(1 << 16), nodeCache: make(map[cache.Key]*NodeRev),
	}, nil
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
	ctx := context.Background()
	b, err := r.store.Get(ctx, r.keys.UUID(), nil)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// SetUUID overwrites the repository UUID. Used by dump/load migration so a
// loaded repository keeps the identity of its source.
func (r *Repo) SetUUID(u string) error {
	if u == "" {
		return errors.New("fs: empty uuid")
	}
	return r.store.Put(context.Background(), r.keys.UUID(), []byte(u))
}

// ShardSize returns the configured shard size.
func (r *Repo) ShardSize() uint64 { return r.shardSz }

// --- HEAD ---

// Head returns the current HEAD revision.
func (r *Repo) Head() (uint64, error) {
	cur, _, err := r.readCurrent()
	return cur.Head, err
}

func (r *Repo) readCurrent() (*Current, string, error) {
	ctx := context.Background()
	_, ok, err := r.store.Head(ctx, r.keys.Current())
	if err != nil {
		return nil, "", err
	}
	if !ok {
		return nil, "", errors.New("fs: repository not initialized (missing current)")
	}
	b, err := r.store.Get(ctx, r.keys.Current(), nil)
	if err != nil {
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
	var b byte
	if exists {
		b = 1
	}
	r.cache.Put(k, []byte{b})
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
	k := cache.Key(fmt.Sprintf("blk/%d", rev))
	if b, ok := r.cache.Get(k); ok {
		return UnmarshalRevBlock(b)
	}
	ctx := context.Background()
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
	default:
		return nil, errors.New("fs: bad text rep")
	}
	if sum := md5.Sum(text); !bytes.Equal(sum[:], nr.TextMD5[:]) {
		return nil, errors.New("fs: text checksum mismatch")
	}
	cachePut(r.cache, k, text)
	return text, nil
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
		if cur.ByName != nil {
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
	out := make([]NodeInfo, 0, len(nr.Children))
	for _, c := range nr.Children {
		cnr, err := r.GetNodeRev(c.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, NodeInfo{Name: c.Name, Rev: c.ID.Rev, ID: c.ID, Kind: cnr.Kind, Props: copyProps(cnr.Props)})
	}
	return out, nil
}

// --- commit ---

// CommitTxn applies the changes as a single new revision and returns its number.
func (r *Repo) CommitTxn(baseRev uint64, changes []Change, revProps map[string]string) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(changes) == 0 {
		return 0, errors.New("fs: empty commit")
	}
	cur, etag, err := r.readCurrent()
	if err != nil {
		return 0, err
	}
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
		// Root / directory changes: svn clients routinely attach root and
		// directory property changes (e.g. svn:date) to every commit.
		// A directory's node-rev moves whenever any child changes, so a
		// strict check here would falsely reject disjoint-path commits
		// ("/svn/demo conflicts"). Children's add/modify/delete carry their
		// own Change entries and are still guarded below, so skipping
		// directories loses no lost-update protection.
		if ch.Path == "/" || ch.Kind == KindDir {
			continue
		}
		// Prefer the client's own base revision for this path; fall back to
		// the transaction base for writers that do not send one.
		base := ch.BaseRev
		if base == 0 {
			base = baseRev
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
	N := cur.Head + 1
	ctx := context.Background()

	var root *tnode
	hb, err := r.readBlock(cur.Head)
	if err != nil {
		return 0, err
	}
	root, err = r.loadNode(hb.Root)
	if err != nil {
		return 0, err
	}
	for i := range changes {
		if err := r.applyChange(root, &changes[i]); err != nil {
			return 0, err
		}
	}
	root.dirty = true

	// collect dirty nodes top-down
	var dirty []*tnode
	var walk func(t *tnode)
	walk = func(t *tnode) {
		if !t.dirty {
			return
		}
		dirty = append(dirty, t)
		for _, c := range t.children {
			walk(c)
		}
	}
	walk(root)
	for i, t := range dirty {
		t.newID = uint64(i + 1)
	}

	nodes := make([]*NodeRev, 0, len(dirty))
	for _, t := range dirty {
		nr := &NodeRev{Kind: t.kind, Node: NodeID{Rev: N, ID: t.newID}, Props: copyProps(t.props), Pred: t.id}
		if t.copyFrom != nil {
			cf := *t.copyFrom
			nr.CopyFrom = &cf
			nr.CopyFromPath = t.copyFromPath
		}
		if t.kind == KindFile {
			if err := r.ensureData(t); err != nil {
				return 0, err
			}
			sum := md5.Sum(t.data)
			copy(nr.TextMD5[:], sum[:])
			if t.id.IsZero() || t.deltaDepth+1 >= r.maxDelta {
				nr.TextRep = TextRepFull
				nr.Text = append([]byte(nil), t.data...)
				nr.DeltaDepth = 0
			} else {
				delta, derr := svndiff.Encode(t.baseText, t.data)
				if derr != nil {
					return 0, derr
				}
				nr.TextRep = TextRepDelta
				nr.Text = delta
				base := t.id
				nr.Base = &base
				nr.DeltaDepth = t.deltaDepth + 1
			}
		} else {
			if err := r.ensureDir(t); err != nil {
				return 0, err
			}
			for name, c := range t.children {
				cid := c.id
				if c.dirty {
					cid = NodeID{Rev: N, ID: c.newID}
				}
				nr.Children = append(nr.Children, Child{Name: name, ID: cid})
			}
		}
		nodes = append(nodes, nr)
	}

	rootID := NodeID{Rev: N, ID: root.newID}
	rb := NewRevBlock(N, cur.Head, rootID, nodes)

	sh := r.shard(N)
	if err := r.store.Put(ctx, r.keys.LooseRev(sh, N), rb.Marshal()); err != nil {
		return 0, err
	}
	rp := NewRevProps()
	rp.Props = revProps
	if rp.Props == nil {
		rp.Props = map[string]string{"svn:author": "svngod", "svn:date": time.Now().UTC().Format("2006-01-02T15:04:05.000000Z")}
	}
	if err := r.store.Put(ctx, r.keys.LooseRevProps(sh, N), rp.Marshal()); err != nil {
		return 0, err
	}
	nc := &Current{Head: N, Created: cur.Created}
	if nc.Created == 0 {
		nc.Created = time.Now().Unix()
	}
	if _, err := r.store.CAS(ctx, r.keys.Current(), etag, nc.Marshal()); err != nil {
		return 0, err
	}
	// invalidate cached HEAD-dependent state
	r.cache.Clear()
	// Auto-pack the just-completed shard: when N is the last revision of a
	// shard, its predecessor shard is complete and gets packed. This keeps the
	// repository at ~O(shards) loose objects instead of O(revisions) — required
	// to scale to millions of revisions.
	if N > 0 && N%r.shardSz == 0 {
		if err := r.PackShard(r.shard(N) - 1); err != nil {
			return N, err
		}
	}
	return N, nil
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

// applyChange mutates the working tree for one Change.
func (r *Repo) applyChange(root *tnode, ch *Change) error {
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
			t := &tnode{kind: KindFile, props: copyProps(ch.Props), data: append([]byte(nil), ch.Data...), dirty: true}
			if old, ok := parent.children[name]; ok && old.kind == KindFile {
				if err := r.ensureData(old); err != nil {
					return err
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
				if len(t.data) == 0 {
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
	ctx := context.Background()
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
	r.cache.Clear()
	return nil
}

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

// CollectLoose deletes loose revision and revprops objects that belong to
// shards already packed. PackShard leaves them behind (readers switched to the
// pack via the .done marker), so storage grew without bound on long-running
// repositories. Returns the number of objects removed.
func (r *Repo) CollectLoose(ctx context.Context) (int, error) {
	deleted := 0
	for _, sub := range []string{"revs-loose/", "revprops-loose/"} {
		prefix := r.keys.Prefix + "/" + sub
		next := ""
		for {
			objs, n, err := r.store.ListPrefix(ctx, prefix, next, 1000)
			if err != nil {
				return deleted, err
			}
			var victims []string
			for _, o := range objs {
				rel := strings.TrimPrefix(o.Key, prefix)
				shardStr := rel
				if i := strings.IndexByte(rel, '/'); i >= 0 {
					shardStr = rel[:i]
				}
				shard, err := strconv.ParseUint(shardStr, 10, 64)
				if err != nil {
					continue
				}
				packed, err := r.isPacked(shard)
				if err != nil || !packed {
					continue
				}
				victims = append(victims, o.Key)
			}
			if len(victims) > 0 {
				if err := r.store.Delete(ctx, victims); err != nil {
					return deleted, err
				}
				deleted += len(victims)
			}
			if n == "" {
				break
			}
			next = n
		}
	}
	return deleted, nil
}

// CollectTxnData deletes content-addressed commit-session bodies that no
// activity manifest references any more (aborted or finished commits leave
// them behind because bodies are shared by hash). Manifests are scanned with
// a small regexp so this layer does not depend on the server's JSON schema.
func (r *Repo) CollectTxnData(ctx context.Context) (int, error) {
	prefix := r.keys.Prefix + "/dav-txns/"
	refRe := regexp.MustCompile(`"dataRef":"([0-9a-f]+)"`)
	referenced := map[string]bool{}
	var dataKeys []string
	next := ""
	for {
		objs, n, err := r.store.ListPrefix(ctx, prefix, next, 500)
		if err != nil {
			return 0, err
		}
		for _, o := range objs {
			rel := strings.TrimPrefix(o.Key, prefix)
			if strings.HasPrefix(rel, "data/") {
				dataKeys = append(dataKeys, o.Key)
				continue
			}
			if !strings.HasSuffix(rel, ".json") {
				continue
			}
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
	for _, k := range dataKeys {
		ref := strings.TrimPrefix(k, prefix+"data/")
		if !referenced[ref] {
			victims = append(victims, k)
		}
	}
	if len(victims) == 0 {
		return 0, nil
	}
	if err := r.store.Delete(ctx, victims); err != nil {
		return 0, err
	}
	return len(victims), nil
}

// GetRevProps returns a revision's properties (loose or packed).
func (r *Repo) GetRevProps(rev uint64) (*RevProps, error) {
	ctx := context.Background()
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
	b, err := r.store.Get(ctx, r.keys.PackedRevProps(sh), nil)
	if err != nil {
		return nil, err
	}
	return RevPropsFromPack(b, rev)
}
