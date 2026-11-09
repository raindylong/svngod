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
	// Version 2 (segment format) describes a whole batch:
	First      uint64 `json:"first,omitempty"`
	Count      uint64 `json:"count,omitempty"`
	SegmentKey string `json:"segment_key,omitempty"`
	SegmentSHA string `json:"segment_sha256,omitempty"`
	Parent      uint64 `json:"parent"`
	Lease       string `json:"lease,omitempty"`
	Created     int64  `json:"created"`
	RevBlockKey string `json:"revblock"`
	RevBlockSHA string `json:"revblock_sha256"`
	PropsKey    string `json:"props"`
	Author      string `json:"author,omitempty"`
	Log         string `json:"log,omitempty"`
	Quorum      int    `json:"quorum"`
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
// commit is never acknowledged on a single copy.
func (r *Repo) writeCommitJournal(ctx context.Context, rev, parent uint64, lease string, created int64,
	revBlock []byte, revBlockKey, propsKey string, revProps map[string]string) error {
	if !r.journal {
		return nil
	}
	sum := sha256.Sum256(revBlock)
	rec := &CommitRecord{
		Version:     journalVersion,
		Rev:         rev,
		Parent:      parent,
		Lease:       lease,
		Created:     created,
		RevBlockKey: revBlockKey,
		RevBlockSHA: hex.EncodeToString(sum[:]),
		PropsKey:    propsKey,
		Author:      revProps["svn:author"],
		Log:         revProps["svn:log"],
		Quorum:      r.writeQuorum,
	}
	if err := r.store.Put(ctx, r.keys.Journal(rev), EncodeCommitRecord(rec)); err != nil {
		return fmt.Errorf("journal: %w", err)
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
		if rec.Version == 2 {
			if rec.First != rev || rec.Count == 0 {
				break
			}
			seg, _, serr := read(rec.SegmentKey)
			if serr != nil {
				break
			}
			sum := sha256.Sum256(seg)
			if hex.EncodeToString(sum[:]) != rec.SegmentSHA {
				break
			}
			refs, ierr := r.loadSegmentIndex(r.shard(rec.First))
			if ierr != nil {
				break
			}
			if !segmentIndexHas(refs, rec) {
				refs = appendSegmentRef(refs, segmentRef{First: rec.First, Count: rec.Count,
					Key: rec.SegmentKey, SHA256: rec.SegmentSHA})
				if perr := r.store.Put(ctx, r.keys.SegmentIndex(r.shard(rec.First)), encodeSegmentIndex(refs)); perr != nil {
					break
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
			log.Printf("journal: rolled forward revisions %d..%d (segment, head was %d)", rec.First, last, head)
			head = last
			rev = head + 1
			continue
		}
		if rec.Version != 1 || rec.Rev != rev {
			break
		}
		rb, rbCount, rerr := read(rec.RevBlockKey)
		if rerr != nil || rbCount < quorum {
			continue
		}
		sum := sha256.Sum256(rb)
		if hex.EncodeToString(sum[:]) != rec.RevBlockSHA {
			continue
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
		log.Printf("journal: rolled forward revision %d (quorum-acked, head was %d)", rev, head)
		head = rev
		rev = head + 1
	}
}

// segmentIndexHas reports whether the index already contains this exact batch.
func segmentIndexHas(refs []segmentRef, rec *CommitRecord) bool {
	for _, ref := range refs {
		if ref.First == rec.First && ref.Count == rec.Count && ref.Key == rec.SegmentKey && ref.SHA256 == rec.SegmentSHA {
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
