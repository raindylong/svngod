package fs

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"svngod/internal/cdc"
	"svngod/internal/fs/store"
	"svngod/internal/svndiff"
)

// buildRevisionLocked applies changes to root (mutating it), assigns node ids
// for revision N and returns the marshalled revision block and properties.
// The caller owns the commit lease and publishes the bytes; a failed call may
// leave the in-memory tree partially modified, so the batch aborts on error.
func (r *Repo) buildRevisionLocked(ctx context.Context, root *tnode, parent, N, created uint64,
	changes []Change, revProps map[string]string) (rbBytes, rpBytes []byte, err error) {
	for i := range changes {
		if err := r.applyChange(root, &changes[i]); err != nil {
			return nil, nil, err
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
	if len(dirty) == 0 {
		return nil, nil, errors.New("fs: empty commit")
	}
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
			nr.CopyFromRev = t.copyFromRev
		}
		if t.kind == KindFile {
			if t.deltaRep {
				if t.deltaBase.IsZero() {
					return nil, nil, errors.New("fs: delta change without a base")
				}
				bn, berr := r.GetNodeRev(t.deltaBase)
				if berr != nil {
					return nil, nil, berr
				}
				nr.TextRep = TextRepDelta
				nr.Text = append([]byte(nil), t.delta...)
				base := t.deltaBase
				nr.Base = &base
				nr.DeltaDepth = bn.DeltaDepth + 1
				nr.TotalSize = t.deltaTotal
				if t.hasMD5 {
					copy(nr.TextMD5[:], t.dataMD5[:])
				}
				if t.hasSHA1 {
					nr.TextSHA1 = t.dataSHA1
				}
				nodes = append(nodes, nr)
				continue
			}
			if len(t.chunkManifest) > 0 {
				nr.TextRep = TextRepChunked
				nr.Text = append([]byte(nil), t.chunkManifest...)
				nr.TotalSize = t.chunkTotal
				nr.TextSize = t.chunkTotal
				if t.hasMD5 {
					nr.TextMD5 = t.dataMD5
				}
				if t.hasSHA1 {
					nr.TextSHA1 = t.dataSHA1
				}
				nodes = append(nodes, nr)
				continue
			}
			if len(t.data) > cdc.Threshold {
				manifest, total, cerr := r.storeChunkedReader(ctx, bytes.NewReader(t.data))
				if cerr != nil {
					return nil, nil, cerr
				}
				nr.TextRep = TextRepChunked
				nr.Text = manifest
				nr.TotalSize = uint64(total)
				nr.TextSize = uint64(total)
				nr.TextMD5 = md5.Sum(t.data)
				nr.TextSHA1 = sha1.Sum(t.data)
				nodes = append(nodes, nr)
				continue
			}
			if t.textRef != "" {
				sum, herr := hex.DecodeString(t.textRef)
				if herr != nil || len(sum) != 16 {
					return nil, nil, errors.New("fs: bad text ref")
				}
				if ok, eerr := r.store.Exists(ctx, r.keys.Text(t.textRef)); eerr != nil || !ok {
					return nil, nil, errors.New("fs: uploaded text object missing")
				}
				copy(nr.TextMD5[:], sum)
				if t.hasSHA1 {
					nr.TextSHA1 = t.dataSHA1
				}
				nr.TextRep = TextRepExternal
				nr.TextRef = t.textRef
				nr.TextSize = t.textSize
				nr.TotalSize = t.textSize
				nr.DeltaDepth = 0
				nodes = append(nodes, nr)
				continue
			}
			if t.dataFile != "" {
				if st, serr := os.Stat(t.dataFile); serr == nil && st.Size() > cdc.Threshold {
					f, oerr := os.Open(t.dataFile)
					if oerr != nil {
						return nil, nil, oerr
					}
					var manifest []byte
					var total int64
					var cerr error
					if t.hasMD5 && t.hasSHA1 {
						// The upload already hashed the body while spooling.
						manifest, total, cerr = r.storeChunkedReader(ctx, f)
					} else {
						h := md5.New()
						hs := sha1.New()
						manifest, total, cerr = r.storeChunkedReader(ctx, io.TeeReader(f, io.MultiWriter(h, hs)))
						if cerr == nil {
							copy(nr.TextMD5[:], h.Sum(nil))
							copy(nr.TextSHA1[:], hs.Sum(nil))
						}
					}
					f.Close()
					if cerr != nil {
						return nil, nil, cerr
					}
					nr.TextRep = TextRepChunked
					nr.Text = manifest
					nr.TotalSize = uint64(total)
					nr.TextSize = uint64(total)
					if t.hasMD5 {
						nr.TextMD5 = t.dataMD5
					}
					if t.hasSHA1 {
						nr.TextSHA1 = t.dataSHA1
					}
					nodes = append(nodes, nr)
					continue
				}
				var known *[16]byte
				if t.hasMD5 {
					known = &t.dataMD5
				}
				var knownSHA1 *[20]byte
				if t.hasSHA1 {
					knownSHA1 = &t.dataSHA1
				}
				sum, sha, size, ref, xerr := r.externalizeFile(ctx, t.dataFile, known, knownSHA1)
				if xerr != nil {
					return nil, nil, xerr
				}
				copy(nr.TextMD5[:], sum[:])
				nr.TextSHA1 = sha
				nr.TextRep = TextRepExternal
				nr.TextRef = ref
				nr.TextSize = size
				nr.TotalSize = size
				nr.DeltaDepth = 0
				nodes = append(nodes, nr)
				continue
			}
			if err := r.ensureData(t); err != nil {
				return nil, nil, err
			}
			sum := md5.Sum(t.data)
			copy(nr.TextMD5[:], sum[:])
			if t.hasSHA1 {
				nr.TextSHA1 = t.dataSHA1
			} else if len(t.data) > 0 {
				nr.TextSHA1 = sha1.Sum(t.data)
			}
			if t.id.IsZero() || t.deltaDepth+1 >= r.maxDelta {
				nr.DeltaDepth = 0
				if len(t.data) > externalTextThreshold {
					ref := hex.EncodeToString(nr.TextMD5[:])
					key := r.keys.Text(ref)
					if ok, herr := r.store.Exists(ctx, key); herr != nil || !ok {
						if perr := r.store.Put(ctx, key, t.data); perr != nil {
							return nil, nil, perr
						}
					}
					nr.TextRep = TextRepExternal
					nr.TextRef = ref
					nr.TextSize = uint64(len(t.data))
				} else {
					nr.TextRep = TextRepFull
					nr.Text = append([]byte(nil), t.data...)
				}
			} else {
				delta, derr := svndiff.Encode(t.baseText, t.data)
				if derr != nil {
					return nil, nil, derr
				}
				nr.TextRep = TextRepDelta
				nr.Text = delta
				base := t.id
				nr.Base = &base
				nr.DeltaDepth = t.deltaDepth + 1
			}
			nr.TotalSize = uint64(len(t.data))
		} else {
			if err := r.ensureDir(t); err != nil {
				return nil, nil, err
			}
			if r.dirBuckets && len(t.children) > dirBucketThreshold {
				ref, size, werr := r.writeDirBuckets(ctx, t, N)
				if werr != nil {
					return nil, nil, werr
				}
				nr.TextRep = TextRepExternal
				nr.TextRef = ref
				nr.TextSize = uint64(size)
				nr.Children = nil
			} else {
				for name, c := range t.children {
					cid := c.id
					if c.dirty {
						cid = NodeID{Rev: N, ID: c.newID}
					}
					nr.Children = append(nr.Children, Child{Name: name, ID: cid})
				}
			}
		}
		nodes = append(nodes, nr)
	}

	rootID := NodeID{Rev: N, ID: root.newID}
	rb := NewRevBlock(N, parent, rootID, nodes)
	rp := NewRevProps()
	rp.Props = revProps
	return rb.Marshal(), rp.Marshal(), nil
}

// batchPlan is one successfully built revision waiting for publication.
type batchPlan struct {
	idx   int
	rev   uint64
	rb    []byte
	rp    []byte
	props map[string]string
}

// BatchCommit is one request inside CommitBatch.
type BatchCommit struct {
	// BaseRev is the transaction base revision for per-path out-of-date
	// checks; 0 selects bulk semantics (add-existing / delete-missing only).
	BaseRev  uint64
	Changes  []Change
	RevProps map[string]string
}

// defaultRevProps fills the properties svngod records when a writer sends none.
func defaultRevProps() map[string]string {
	return map[string]string{
		"svn:author": "svngod",
		"svn:date":   time.Now().UTC().Format("2006-01-02T15:04:05.000000Z"),
	}
}

// finalizeTree gives every node created by the just-built revision its final
// identity so the next commit in the batch can modify it without trying to
// read an unpublished node-rev from the store. Children/texts are already in
// memory, so the lazy-load flags are set as if they had been fetched.
func finalizeTree(root *tnode, rev uint64) {
	var walk func(t *tnode)
	walk = func(t *tnode) {
		if !t.dirty || t.newID == 0 {
			return
		}
		t.id = NodeID{Rev: rev, ID: t.newID}
		t.dirty = false
		t.newID = 0
		if t.kind == KindDir {
			t.dirLoaded = true
		} else {
			t.dataLoaded = true
		}
		for _, c := range t.children {
			walk(c)
		}
	}
	walk(root)
}

// lookupTree resolves a path inside the in-memory commit tree.
func (r *Repo) lookupTree(root *tnode, path string) (*tnode, bool) {
	segs := segments(path)
	if len(segs) == 0 {
		return root, true
	}
	cur := root
	for _, seg := range segs {
		if err := r.ensureDir(cur); err != nil {
			return nil, false
		}
		next, ok := cur.children[seg]
		if !ok {
			return nil, false
		}
		cur = next
	}
	return cur, true
}

// checkChangeInTree is the batch equivalent of CommitTxn's per-path
// out-of-date loop: the node is looked up in the evolving tree (which already
// contains the batch's earlier revisions) instead of the published HEAD.
func (r *Repo) checkChangeInTree(root *tnode, ch *Change, baseRev uint64) error {
	if ch.Noop || ch.Path == "/" {
		return nil
	}
	base := ch.BaseRev
	if base == 0 {
		base = baseRev
	}
	// Directory property changes are routine and carry no subtree semantics; a
	// directory's node-rev moves whenever any child changes, so checking those
	// would reject unrelated concurrent commits. A delete or copy *target* is
	// different: applying it over a directory that gained content since the
	// client's base silently destroys another writer's commit, which is exactly
	// what the per-path out-of-date check exists to prevent.
	if ch.Kind == KindDir {
		if ch.Action == ActionModify {
			return nil
		}
		if t, ok := r.lookupTree(root, ch.Path); ok {
			if base > 0 && t.id.Rev > base {
				return &OutOfDateError{Path: ch.Path}
			}
			if base == 0 && ch.Action == ActionAdd {
				return &OutOfDateError{Path: ch.Path}
			}
			return nil
		}
	}
	t, ok := r.lookupTree(root, ch.Path)
	if ok {
		if base == 0 {
			if ch.Action == ActionAdd {
				return &OutOfDateError{Path: ch.Path}
			}
			return nil
		}
		if t.id.Rev > base {
			return &OutOfDateError{Path: ch.Path}
		}
		if len(ch.Delta) > 0 {
			bnr, err := r.Resolve(base, ch.Path)
			if err != nil || t.id != bnr.Node {
				return &OutOfDateError{Path: ch.Path}
			}
			if _, err := r.deltaBaseOf(bnr, ch.DeltaBase); err != nil {
				return &OutOfDateError{Path: ch.Path}
			}
		}
		return nil
	}
	if base > 0 {
		if _, err := r.Resolve(base, ch.Path); err == nil {
			return &OutOfDateError{Path: ch.Path}
		}
	}
	if ch.Action == ActionDelete {
		return fmt.Errorf("fs: delete of nonexistent path %s", ch.Path)
	}
	return nil
}

// CommitBatch applies several independent transactions as consecutive
// revisions with one commit lease and one HEAD CAS. Every revision block,
// property set and WAL record is written before the single CAS, so the batch
// is published atomically: a crash or CAS failure leaves no client with a
// success for an unpublished revision. Requests are bulk writers (baseRev 0);
// per-path base revisions are not available, so conflicts are checked against
// the evolving tree (add-existing / delete-missing).
func (r *Repo) CommitBatch(reqs []BatchCommit) ([]uint64, []error, error) {
	revs := make([]uint64, len(reqs))
	errs := make([]error, len(reqs))
	if len(reqs) == 0 {
		return revs, errs, nil
	}
	if len(reqs) > journalReplayWindow {
		return revs, errs, fmt.Errorf("fs: batch of %d exceeds the %d-revision replay window", len(reqs), journalReplayWindow)
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	ctxLease := context.Background()
	t0 := time.Now()
	lease, lerr := r.acquireCommitLease(ctxLease)
	if lerr != nil {
		for i := range errs {
			errs[i] = lerr
		}
		return revs, errs, lerr
	}
	defer lease.Release(r.store)
	phase := func(name string) {
		if debugCommit {
			log.Printf("batch[%d] phase %-9s %s", len(reqs), name, time.Since(t0))
			t0 = time.Now()
		}
	}
	phase("lease")

	r.reconcileJournal(true)
	phase("reconcile")

	cur, etag, err := r.readCurrentStrict()
	if err != nil {
		return revs, errs, err
	}
	if perr := r.ensureNoPendingPublication(cur.Head); perr != nil {
		return revs, errs, perr
	}
	phase("head")

	ctx := context.Background()
	root, err := r.readRootLocked(cur.Head)
	if err != nil {
		return revs, errs, err
	}
	created := cur.Created
	if created == 0 {
		created = time.Now().Unix()
	}

	var plans []batchPlan
	N := cur.Head
	for i := range reqs {
		req := &reqs[i]
		if req.RevProps == nil {
			req.RevProps = defaultRevProps()
		}
		bad := false
		for _, ch := range req.Changes {
			if cerr := r.checkChangeInTree(root, &ch, req.BaseRev); cerr != nil {
				errs[i] = cerr
				bad = true
				break
			}
		}
		if bad {
			continue
		}
		n := N + 1
		rb, rp, berr := r.buildRevisionLocked(ctx, root, N, n, uint64(created), req.Changes, req.RevProps)
		if berr != nil {
			// The tree may be half-applied; stop rather than publish a
			// revision that does not match the request set.
			errs[i] = berr
			for j := i + 1; j < len(errs); j++ {
				if errs[j] == nil {
					errs[j] = berr
				}
			}
			break
		}
		finalizeTree(root, n)
		N = n
		revs[i] = n
		plans = append(plans, batchPlan{idx: i, rev: n, rb: rb, rp: rp, props: req.RevProps})
	}
	phase("build")
	if len(plans) == 0 {
		return revs, errs, nil
	}
	if r.segments {
		return r.publishSegmentPlans(ctx, lease, cur, etag, plans, created, revs, errs)
	}

	// Data objects first: both mirrors receive every revision block and
	// property set concurrently. The batch is not visible until the CAS below.
	if err := r.putBatchObjects(ctx, plans); err != nil {
		for _, p := range plans {
			errs[p.idx] = err
		}
		return revs, errs, err
	}
	phase("writes")

	if verr := lease.Verify(ctx, r.store); verr != nil {
		// Before the WAL, a lost lease means another writer may be publishing
		// this range; abort instead of gambling on the CAS.
		for _, p := range plans {
			errs[p.idx] = verr
		}
		return revs, errs, verr
	}
	// WAL: one record per revision, all sharing this batch's lease token.
	// They are independent objects; writing them concurrently keeps the batch
	// at a few backend round trips instead of one per revision.
	if err := r.writeBatchJournals(ctx, plans, lease.token, created); err != nil {
		for _, x := range plans {
			errs[x.idx] = err
		}
		return revs, errs, err
	}
	phase("journal")

	nc := &Current{Head: N, Created: created}
	if _, cerr := r.store.CAS(ctx, r.keys.Current(), etag, nc.Marshal()); cerr != nil {
		if !errors.Is(cerr, store.ErrPreconditionFailed) {
			return revs, errs, cerr
		}
		// A peer's WAL replay may have published part or all of the batch.
		// Accept exactly the revisions whose durable record is ours.
		now := cur.Head
		if c, _, rerr := r.readCurrentStrict(); rerr == nil {
			now = c.Head
		}
		for _, p := range plans {
			if p.rev <= now && r.journalOwns(ctx, p.rev, lease.token, p.rb) {
				continue
			}
			errs[p.idx] = store.ErrQuorumUnmet
		}
		if now < N {
			// Continue from wherever the replay stopped: the unpublished tail
			// is not visible and must be retried by its writers.
			N = now
		}
	}
	r.setHead(N)
	r.cache.Clear()
	phase("cas")

	// Pack every shard this batch completed.
	for s := r.shard(cur.Head); s <= r.shard(N); s++ {
		if uint64(s+1)*r.shardSz <= N {
			if perr := r.packShardLocked(s); perr != nil {
				log.Printf("batch: pack shard %d: %v", s, perr)
			}
		}
	}
	return revs, errs, nil
}

// writeBatchJournals appends the WAL record for every plan concurrently.
func (r *Repo) writeBatchJournals(ctx context.Context, plans []batchPlan, token string, created int64) error {
	sem := make(chan struct{}, 16)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for _, p := range plans {
		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			sh := r.shard(p.rev)
			if err := r.writeCommitJournal(ctx, p.rev, p.rev-1, token, created, p.rb,
				r.keys.LooseRev(sh, p.rev), r.keys.LooseRevProps(sh, p.rev), p.props); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return firstErr
}

// putBatchObjects writes every plan's revision block and properties with
// bounded concurrency. Nothing is visible until the caller's HEAD CAS.
func (r *Repo) putBatchObjects(ctx context.Context, plans []batchPlan) error {
	sem := make(chan struct{}, 16)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	add := func(key string, data []byte) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := r.store.Put(ctx, key, data); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}()
	}
	for _, p := range plans {
		sh := r.shard(p.rev)
		add(r.keys.LooseRev(sh, p.rev), p.rb)
		add(r.keys.LooseRevProps(sh, p.rev), p.rp)
	}
	wg.Wait()
	return firstErr
}

// readRootLocked loads the revision's root node for an in-memory commit tree.
func (r *Repo) readRootLocked(rev uint64) (*tnode, error) {
	hb, err := r.readBlock(rev)
	if err != nil {
		return nil, err
	}
	return r.loadNode(hb.Root)
}
