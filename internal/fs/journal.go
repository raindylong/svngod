package fs

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"sort"
	"sync"
	"time"

	"svngod/internal/fs/store"
)

// The commit journal is a write-ahead log in the etcd sense. Before the HEAD
// pointer is advanced, the commit is described by a self-validating record and
// written to a quorum of replicas. A crash can then be classified precisely:
//
//   - record present on a quorum + HEAD advanced: committed (roll forward is
//     only needed when the process died between the record and the CAS);
//   - record present but no quorum: an aborted intent, its objects are orphans
//     for GC and no revision is visible;
//   - record absent: the commit never reached the durable stage.
//
// The record is framed like an etcd WAL entry: magic, length, CRC32C, JSON.
// A torn or corrupted record is rejected instead of being replayed.

const (
	journalMagic   = "SVNGWAL1"
	journalVersion = 1
)

var journalCRC = crc32.MakeTable(crc32.Castagnoli)

// CommitRecord describes one revision publication.
type CommitRecord struct {
	Version int    `json:"v"`
	Rev     uint64 `json:"rev"`
	// Version 2 (segment format) describes a whole batch. Groups lists every
	// shard segment the batch wrote; replay publishes all of them or none, so a
	// batch that crosses a shard boundary cannot be replayed halfway.
	First       uint64        `json:"first,omitempty"`
	Count       uint64        `json:"count,omitempty"`
	SegmentKey  string        `json:"segment_key,omitempty"`
	SegmentSHA  string        `json:"segment_sha256,omitempty"`
	Groups      []CommitGroup `json:"groups,omitempty"`
	Parent      uint64        `json:"parent"`
	Lease       string        `json:"lease,omitempty"`
	Created     int64         `json:"created"`
	RevBlockKey string        `json:"revblock"`
	RevBlockSHA string        `json:"revblock_sha256"`
	PropsKey    string        `json:"props"`
	// PropsSHA pins the initial revprops object of the revision (v1 records).
	// Empty means a pre-2.6' record: presence at a quorum is checked only.
	// Segment (v2) records do not set it - their props live inside the
	// immutable segment covered by SegmentSHA.
	PropsSHA string `json:"props_sha256,omitempty"`
	// PrevJournalSHA chains this record to journal/<rev-1> as stored: the
	// SHA-256 of the previous record's encoded bytes. Any silent rewrite of a
	// historical record breaks the chain of every later record, so a verifier
	// can prove the revision log was not tampered with (WORM audit chain).
	// Empty on the first record (or a pre-4.2 record) starts a fresh chain.
	PrevJournalSHA string `json:"prev_journal_sha256,omitempty"`
	Author         string `json:"author,omitempty"`
	Log            string `json:"log,omitempty"`
	Quorum         int    `json:"quorum"`
}

// CommitGroup is one shard's slice of a segmented commit batch.
type CommitGroup struct {
	First      uint64 `json:"first"`
	Count      uint64 `json:"count"`
	SegmentKey string `json:"segment_key,omitempty"`
	SegmentSHA string `json:"segment_sha256"`
}

// EncodeCommitRecord frames a record: 8-byte magic, big-endian length, CRC32C
// of the JSON body, then the body.
func EncodeCommitRecord(rec *CommitRecord) []byte {
	body, err := json.Marshal(rec)
	if err != nil {
		panic("fs: encode commit record: " + err.Error())
	}
	out := make([]byte, 16, 16+len(body))
	copy(out, journalMagic)
	binary.BigEndian.PutUint32(out[8:12], uint32(len(body)))
	binary.BigEndian.PutUint32(out[12:16], crc32.Checksum(body, journalCRC))
	return append(out, body...)
}

// DecodeCommitRecord validates framing, CRC and version.
func DecodeCommitRecord(b []byte) (*CommitRecord, error) {
	if len(b) < 16 || string(b[:8]) != journalMagic {
		return nil, errors.New("journal: bad magic")
	}
	n := binary.BigEndian.Uint32(b[8:12])
	crc := binary.BigEndian.Uint32(b[12:16])
	if uint64(len(b)) != 16+uint64(n) {
		return nil, fmt.Errorf("journal: length %d != %d", len(b), 16+n)
	}
	body := b[16:]
	if crc32.Checksum(body, journalCRC) != crc {
		return nil, errors.New("journal: checksum mismatch")
	}
	var rec CommitRecord
	if err := json.Unmarshal(body, &rec); err != nil {
		return nil, fmt.Errorf("journal: decode: %w", err)
	}
	if rec.Version != 1 && rec.Version != 2 {
		return nil, fmt.Errorf("journal: unsupported version %d", rec.Version)
	}
	return &rec, nil
}

func journalOf(s store.Store) bool {
	if j, ok := s.(interface{ Journaling() bool }); ok {
		return j.Journaling()
	}
	return false
}

func writeQuorumOf(s store.Store) int {
	if q, ok := s.(interface{ WriteQuorum() int }); ok {
		return q.WriteQuorum()
	}
	return 0
}

// writeCommitJournal appends the commit record. In quorum mode the Put itself
// fails unless the record reached the configured number of replicas, so a
// commit is never acknowledged on a single copy. propsBytes is the initial
// revprops object the commit wrote; its SHA is pinned in the record so replay
// can reject a torn or foreign props write before publishing HEAD (empty for
// segment commits, whose props are inside the segment).
func (r *Repo) writeCommitJournal(ctx context.Context, rev, parent uint64, lease string, created int64,
	revBlock, propsBytes []byte, revBlockKey, propsKey string, revProps map[string]string) error {
	if !r.journal {
		return nil
	}
	sum := sha256.Sum256(revBlock)
	rec := r.newCommitRecord(rev, parent, lease, created, revBlockKey,
		hex.EncodeToString(sum[:]), propsKey, propsBytes, revProps)
	if prev := r.prevJournalSHA(ctx, rev); prev != "" {
		rec.PrevJournalSHA = prev
	}
	enc := EncodeCommitRecord(rec)
	if err := r.store.Put(ctx, r.keys.Journal(rev), enc); err != nil {
		return fmt.Errorf("journal: %w", err)
	}
	r.rememberJournal(rev, enc)
	return nil
}

// newCommitRecord fills the WAL record for one revision. Shared by the single
// commit path and the batch path so the two cannot drift; the caller adds the
// chain link, which differs (store lookup vs in-memory predecessor).
func (r *Repo) newCommitRecord(rev, parent uint64, lease string, created int64,
	revBlockKey, revBlockSHA, propsKey string, propsBytes []byte, revProps map[string]string) *CommitRecord {
	rec := &CommitRecord{
		Version:     journalVersion,
		Rev:         rev,
		Parent:      parent,
		Lease:       lease,
		Created:     created,
		RevBlockKey: revBlockKey,
		RevBlockSHA: revBlockSHA,
		PropsKey:    propsKey,
		Author:      revProps["svn:author"],
		Log:         revProps["svn:log"],
		Quorum:      r.writeQuorum,
	}
	if propsBytes != nil {
		psum := sha256.Sum256(propsBytes)
		rec.PropsSHA = hex.EncodeToString(psum[:])
	}
	return rec
}

// encodeCommitRecord builds the WAL record for one plan of a batch and chains
// it to prevBytes (the encoded predecessor, held in memory while the batch is
// written). Keeping this next to writeCommitJournal means both paths derive the
// record from the same constructor.
func (r *Repo) encodeCommitRecord(p batchPlan, lease string, created int64, prevBytes []byte) ([]byte, error) {
	sum := sha256.Sum256(p.rb)
	rec := r.newCommitRecord(p.rev, p.rev-1, lease, created,
		r.keys.LooseRev(r.shard(p.rev), p.rev), hex.EncodeToString(sum[:]),
		r.keys.LooseRevProps(r.shard(p.rev), p.rev), p.rp, p.props)
	if len(prevBytes) > 0 {
		psum := sha256.Sum256(prevBytes)
		rec.PrevJournalSHA = hex.EncodeToString(psum[:])
	}
	return EncodeCommitRecord(rec), nil
}

// rememberJournal caches the just-written record so the next commit chains
// from memory instead of re-reading journal/<rev-1> from the store (one S3
// round trip per commit on the mirror path). Writes are serialised by the
// commit lease; the cached bytes are exactly what was stored.
func (r *Repo) rememberJournal(rev uint64, enc []byte) {
	r.lastJournalMu.Lock()
	r.lastJournalRev = rev
	r.lastJournalCached = true
	r.lastJournalBytes = enc
	r.lastJournalMu.Unlock()
}

// cachedJournal returns the cached encoded bytes for journal/<rev>, or nil.
func (r *Repo) cachedJournal(rev uint64) []byte {
	r.lastJournalMu.Lock()
	defer r.lastJournalMu.Unlock()
	if r.lastJournalCached && r.lastJournalRev == rev {
		return r.lastJournalBytes
	}
	return nil
}

// prevJournalSHA returns the SHA-256 of journal/<rev-1> as stored, or "" when
// rev is the first record (or the predecessor is absent/unreadable, which
// starts a fresh chain rather than failing the commit - the chain is verified
// as a whole by VerifyJournalChain, not per write). The most recent write is
// usually still cached: commits are consecutive revisions under the lease.

func (r *Repo) prevJournalSHA(ctx context.Context, rev uint64) string {
	if rev == 0 {
		return ""
	}
	r.lastJournalMu.Lock()
	cached, crev, cbytes := r.lastJournalCached, r.lastJournalRev, r.lastJournalBytes
	r.lastJournalMu.Unlock()
	var b []byte
	if cached && crev == rev-1 {
		b = cbytes
	} else {
		var err error
		b, err = r.store.Get(ctx, r.keys.Journal(rev-1), nil)
		if err != nil || len(b) == 0 {
			return ""
		}
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// prevBatchJournalSHA returns the chain hash for a version-2 record that
// starts at rev and spans count revisions.
//
// A v2 record covers [rev, rev+count), so its predecessor is NOT journal/rev-1
// (that revision has no record of its own) but whichever record covers rev-1 -
// typically the previous batch, which starts earlier and spans further. The
// in-memory cache normally answers this: the previous batch was written by this
// process moments ago under the same lease. Otherwise look back over the
// maximum batch span and take the newest record that covers rev-1.
func (r *Repo) prevBatchJournalSHA(ctx context.Context, rev uint64) string {
	if rev <= 1 {
		return ""
	}
	if b := r.cachedJournal(rev - 1); b != nil {
		sum := sha256.Sum256(b)
		return hex.EncodeToString(sum[:])
	}
	for back := uint64(1); back <= maxCommitBatch && back < rev; back++ {
		cand := rev - back
		b, err := r.store.Get(ctx, r.keys.Journal(cand), nil)
		if err != nil || len(b) == 0 {
			continue
		}
		rec, derr := DecodeCommitRecord(b)
		if derr != nil {
			return ""
		}
		last := rec.First + maxU64(rec.Count, 1) - 1
		if last >= rev-1 && rec.First <= rev-1 {
			sum := sha256.Sum256(b)
			return hex.EncodeToString(sum[:])
		}
	}
	return ""
}

func maxU64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// VerifyJournalChain walks journal records from..to and checks the hash
// chain: each record's PrevJournalSHA (when set) must equal the SHA-256 of
// the previous record's stored bytes. Returns the first break as an error
// naming the revision. Records predating the chain feature have an empty
// PrevJournalSHA and simply continue the walk (a fresh chain starts at the
// next record that has one).
func (r *Repo) VerifyJournalChain(from, to uint64) error {
	if !r.journal {
		return nil
	}
	ctx := context.Background()
	var prev []byte
	for rev := from; rev <= to; rev++ {
		b, err := r.store.Get(ctx, r.keys.Journal(rev), nil)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				if prev == nil {
					continue // gap before the first record we saw
				}
				return fmt.Errorf("journal: chain gap at rev %d (predecessor exists, record missing)", rev)
			}
			return err
		}
		rec, derr := DecodeCommitRecord(b)
		if derr != nil {
			return fmt.Errorf("journal: rev %d: %w", rev, derr)
		}
		if rec.Rev != rev {
			return fmt.Errorf("journal: rev %d: record claims rev %d", rev, rec.Rev)
		}
		if prev != nil && rec.PrevJournalSHA != "" {
			sum := sha256.Sum256(prev)
			if hex.EncodeToString(sum[:]) != rec.PrevJournalSHA {
				return fmt.Errorf("journal: hash chain broken at rev %d: predecessor was rewritten or forged", rev)
			}
		}
		prev = b
		// A version-2 record covers [First, First+Count): group commit writes one
		// record per batch, so the revisions after it have no record of their own
		// and must not be reported as a gap. Skipping the covered range is also
		// what keeps the chain check meaningful for segmented repositories, where
		// every batch is a single record.
		if rec.Count > 1 {
			end := rec.First + rec.Count - 1
			if end > to {
				end = to
			}
			if end > rev {
				rev = end
			}
		}
	}
	return nil
}

// reconcileJournal runs the WAL replay, throttled unless force is set.
func (r *Repo) reconcileJournal(force bool) {
	if !r.journal {
		return
	}
	r.journalMu.Lock()
	defer r.journalMu.Unlock()
	if !force && time.Since(r.journalAt) < 500*time.Millisecond {
		return
	}
	r.journalAt = time.Now()
	r.replayJournal()
}

// replayJournal rolls forward a commit that reached a quorum and whose data is
// complete, but whose HEAD CAS never happened (crash between WAL append and
// publication). Entries that did not reach a quorum are ignored: they are the
// remains of an aborted attempt.
// journalReplayWindow bounds how many contiguous revision records one replay
// pass may publish. It matches the largest commit batch.
const journalReplayWindow = 256

func (r *Repo) replayJournal() {
	if !r.journal {
		return
	}
	// Strict mode requires the record on a quorum; availability mode accepts
	// the best copy, because a single-copy ack is the configured trade-off.
	quorum := r.writeQuorum
	if quorum < 1 {
		quorum = 1
	}
	ctx := context.Background()
	read := func(key string) ([]byte, int, error) {
		// Read every replica, not just the elected primary: a process whose
		// primary is stale must still see a newer record published through the
		// other node.
		if qg, ok := r.store.(interface {
			QuorumGet(ctx context.Context, key string) ([]byte, int, error)
		}); ok {
			return qg.QuorumGet(ctx, key)
		}
		b, err := r.store.Get(context.Background(), key, nil)
		if err != nil {
			return nil, 0, err
		}
		return b, 1, nil
	}
	cur0, _, err := r.readCurrent()
	if err != nil {
		return
	}
	head := cur0.Head
	// Only a contiguous run starting at head+1 can be published. Anything
	// further ahead is a different writer's intent and must not be reordered.
	// A missing or invalid record ends the search: probing the next window of
	// revision numbers on every HeadFresh cost one backend retry each while a
	// replica was down (measured as ~10s per upload in availability mode).
	// The window covers one maximum-size commit batch so a crash after the
	// batch's WAL append still rolls the whole batch forward.
	var prevBytes []byte // journal/<head> bytes for the chain check
	if head > 0 {
		if pb := r.cachedJournal(head); pb != nil {
			prevBytes = pb
		} else if pb, perr := r.store.Get(ctx, r.keys.Journal(head), nil); perr == nil {
			prevBytes = pb
		}
	}
	for rev := head + 1; rev <= head+journalReplayWindow; {
		key := r.keys.Journal(rev)
		// Cheap presence probe first. A plain Get reads the primary and does
		// not touch an out-of-service replica, so the common case (no pending
		// record) costs one fast request instead of a quorum scan.
		if _, err := r.store.Get(ctx, key, nil); err != nil {
			break
		}
		b, count, err := read(key)
		if err != nil || count < quorum {
			break
		}
		rec, derr := DecodeCommitRecord(b)
		if derr != nil || rec.Parent != head {
			break
		}
		// WORM hash chain: this record (and its predecessor bytes we just
		// verified in the previous iteration) must be the bytes that were
		// committed. A broken chain means a historical record was rewritten;
		// publishing on top of a forged log would certify tampered history.
		if prevBytes != nil && rec.PrevJournalSHA != "" {
			sum := sha256.Sum256(prevBytes)
			if hex.EncodeToString(sum[:]) != rec.PrevJournalSHA {
				log.Printf("journal: replay paused at revision %d: hash chain broken (predecessor rewritten?)", rev)
				break
			}
		}
		prevBytes = b
		if rec.Version == 2 {
			if rec.First != rev || rec.Count == 0 {
				break
			}
			groups := rec.Groups
			if len(groups) == 0 {
				// Records written before groups were introduced describe exactly
				// one shard segment.
				groups = []CommitGroup{{First: rec.First, Count: rec.Count, SegmentSHA: rec.SegmentSHA}}
			}
			// The whole batch must be contiguous and complete before anything is
			// published: replaying one shard of a two-shard batch would expose a
			// partially applied commit.
			want := rec.First
			for _, g := range groups {
				if g.Count == 0 || g.First != want {
					return
				}
				want += g.Count
			}
			if want != rec.First+rec.Count {
				return
			}
			type pendingGroup struct {
				shard uint64
				ref   segmentRef
			}
			pend := make([]pendingGroup, 0, len(groups))
			for _, g := range groups {
				segKey := r.keys.Segment(r.shard(g.First), g.First, g.Count)
				seg, _, serr := read(segKey)
				if serr != nil {
					return
				}
				sum := sha256.Sum256(seg)
				if hex.EncodeToString(sum[:]) != g.SegmentSHA {
					return
				}
				pend = append(pend, pendingGroup{shard: r.shard(g.First),
					ref: segmentRef{First: g.First, Count: g.Count, SHA256: g.SegmentSHA}})
			}
			// All segments exist: make every shard index name its segment, then
			// publish the batch with one CAS. The index update is a compare-and-
			// swap retry (not a load+Put): a concurrent live commit may append
			// to the same shard's index while this process replays, and a plain
			// Put would silently drop one of the two refs.
			for _, pg := range pend {
				if _, aerr := r.appendSegmentIndexCAS(ctx, pg.ref); aerr != nil {
					log.Printf("journal: replay paused at revision %d: shard %d index CAS: %v", rev, pg.shard, aerr)
					return
				}
			}
			cur, etag, cerr := r.readCurrent()
			if cerr != nil || cur.Head != head {
				return
			}
			last := rec.First + rec.Count - 1
			nc := &Current{Head: last, Created: rec.Created}
			if _, cerr := r.store.CAS(ctx, r.keys.Current(), etag, nc.Marshal()); cerr != nil {
				return
			}
			r.setHead(last)
			r.cache.Clear()
			r.journalReplayed.Add(rec.Count)
			log.Printf("journal: rolled forward revisions %d..%d (segment, %d group(s), head was %d)",
				rec.First, last, len(groups), head)
			head = last
			rev = head + 1
			continue
		}
		if rec.Version != 1 || rec.Rev != rev {
			break
		}
		// A rev-block that is missing below quorum, unreadable, or corrupt
		// ends the run: the loop has no post statement (batch publishes
		// advance rev manually), so `continue` would re-examine the same
		// revision forever while holding journalMu. Reconciliation retries
		// once repair restores a verifying copy.
		rb, rbCount, rerr := read(rec.RevBlockKey)
		if rerr != nil || rbCount < quorum {
			log.Printf("journal: replay paused at revision %d: rev-block copies %d < quorum %d (err %v)", rev, rbCount, quorum, rerr)
			break
		}
		sum := sha256.Sum256(rb)
		if hex.EncodeToString(sum[:]) != rec.RevBlockSHA {
			log.Printf("journal: replay paused at revision %d: rev-block SHA mismatch (possible corruption)", rev)
			break
		}
		// The revision block and its property set were written together under
		// one quorum; publishing without a quorum copy of the props would
		// roll forward a revision whose log/author data is still missing on
		// a majority (the read path would fail it after HEAD moved). Newer
		// records pin the initial props SHA as well: replay must refuse a
		// props object that is not the committed one (a torn write would
		// otherwise publish a revision whose properties are corrupt). The
		// revision is not yet published, so user revprop edits - which
		// require rev <= head - cannot have legitimately replaced it.
		if _, pCount, perr := read(rec.PropsKey); perr != nil || pCount < quorum {
			log.Printf("journal: replay paused at revision %d: rev-props copies %d < quorum %d (err %v)", rev, pCount, quorum, perr)
			break
		}
		if rec.PropsSHA != "" {
			pb, _, pberr := read(rec.PropsKey)
			if pberr != nil {
				log.Printf("journal: replay paused at revision %d: rev-props unreadable (%v)", rev, pberr)
				break
			}
			psum := sha256.Sum256(pb)
			if hex.EncodeToString(psum[:]) != rec.PropsSHA {
				log.Printf("journal: replay paused at revision %d: rev-props SHA mismatch (possible corruption)", rev)
				break
			}
		}
		cur, etag, cerr := r.readCurrent()
		if cerr != nil || cur.Head != head {
			return
		}
		nc := &Current{Head: rev, Created: rec.Created}
		if _, cerr := r.store.CAS(ctx, r.keys.Current(), etag, nc.Marshal()); cerr != nil {
			return
		}
		r.setHead(rev)
		r.cache.Clear()
		r.journalReplayed.Add(1)
		log.Printf("journal: rolled forward revision %d (quorum-acked, head was %d)", rev, head)
		head = rev
		rev = head + 1
	}
}

// segmentIndexHas reports whether the index already contains this exact batch.
func segmentIndexHas(refs []segmentRef, rec *CommitRecord) bool {
	for _, ref := range refs {
		if ref.First == rec.First && ref.Count == rec.Count && ref.SHA256 == rec.SegmentSHA {
			return true
		}
	}
	return false
}

// segmentIndexHasRef reports whether the index contains this exact segment.
func segmentIndexHasRef(refs []segmentRef, want segmentRef) bool {
	for _, ref := range refs {
		if ref == want {
			return true
		}
	}
	return false
}

// JournalTail returns the last n journal records that can be decoded, newest
// first. It lists the journal prefix instead of walking down from HEAD so that
// an in-flight record (rev > HEAD, the crash window) is visible too.
func (r *Repo) JournalTail(n int) ([]*CommitRecord, error) {
	ctx := context.Background()
	prefix := r.keys.JournalPrefix()
	var keys []string
	next := ""
	for {
		objs, nx, err := r.store.ListPrefix(ctx, prefix, next, 1000)
		if err != nil {
			return nil, err
		}
		for _, o := range objs {
			keys = append(keys, o.Key)
		}
		if nx == "" || nx == next {
			break
		}
		next = nx
	}
	sort.Strings(keys)
	var out []*CommitRecord
	for i := len(keys) - 1; i >= 0 && len(out) < n; i-- {
		b, err := r.store.Get(ctx, keys[i], nil)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				continue
			}
			return nil, err
		}
		rec, derr := DecodeCommitRecord(b)
		if derr != nil {
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}

// TrimJournal deletes journal records older than the last keep revisions.
// Records are small, but an unbounded log is still undesirable; GC calls this
// after reclaiming loose objects.
func (r *Repo) TrimJournal(keep uint64) (int, error) {
	return r.trimJournal(keep, false)
}

// TrimJournalDry reports how many WAL records trim would drop without deleting.
func (r *Repo) TrimJournalDry(keep uint64) (int, error) {
	return r.trimJournal(keep, true)
}

func (r *Repo) trimJournal(keep uint64, dryRun bool) (int, error) {
	head, err := r.HeadFresh()
	if err != nil {
		return 0, err
	}
	if head <= keep {
		return 0, nil
	}
	cutoff := head - keep
	if r.segments {
		// One WAL record per batch: delete the records of segments that ended
		// at or below the cutoff, by exact key (no prefix listing).
		deleted := 0
		for sh := uint64(0); sh <= r.shard(head); sh++ {
			refs, ierr := r.loadSegmentIndex(sh)
			if ierr != nil {
				return deleted, ierr
			}
			var keys []string
			for _, ref := range refs {
				if ref.First+ref.Count-1 <= cutoff {
					keys = append(keys, r.keys.Journal(ref.First))
				}
			}
			if dryRun {
				// Only count records that still exist: trimming is idempotent,
				// but a dry run must not report records an earlier pass already
				// removed as reclaimable.
				for _, k := range keys {
					if ok, _ := r.store.Exists(context.Background(), k); ok {
						deleted++
					}
				}
				continue
			}
			for start := 0; start < len(keys); start += 1000 {
				end := start + 1000
				if end > len(keys) {
					end = len(keys)
				}
				if derr := r.deleteLoose(context.Background(), keys[start:end]); derr != nil {
					return deleted, derr
				}
				deleted += end - start
			}
		}
		return deleted, nil
	}
	ctx := context.Background()
	// Delete by exact key range instead of listing the journal prefix: one
	// record per revision means a 625k-revision repository needs 625 list
	// pages, and RustFS needs seconds per page (repair/GC stalled for hours
	// on it). Deletes run concurrently for the same reason.
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	var mu sync.Mutex
	deleted := 0
	var firstErr error
	var batch []string
	flush := func(b []string) {
		if len(b) == 0 {
			return
		}
		if dryRun {
			deleted += len(b)
			return
		}
		wg.Add(1)
		go func(b []string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := r.store.Delete(ctx, b); err != nil {
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
	for rev := uint64(1); rev <= cutoff; rev++ {
		batch = append(batch, r.keys.Journal(rev))
		if len(batch) >= 1000 {
			flush(batch)
			batch = nil
		}
	}
	flush(batch)
	wg.Wait()
	return deleted, firstErr
}

// JournalRecord returns one decoded record, or nil when absent.
func (r *Repo) JournalRecord(rev uint64) (*CommitRecord, error) {
	b, err := r.store.Get(context.Background(), r.keys.Journal(rev), nil)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	rec, derr := DecodeCommitRecord(b)
	if derr != nil {
		return nil, derr
	}
	return rec, nil
}
