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
	Version     int    `json:"v"`
	Rev         uint64 `json:"rev"`
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
	if rec.Version != journalVersion {
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
	for rev := head + 1; rev <= head+8; rev++ {
		b, count, err := read(r.keys.Journal(rev))
		if err != nil || count < quorum {
			continue
		}
		rec, derr := DecodeCommitRecord(b)
		if derr != nil || rec.Rev != rev || rec.Parent != head {
			continue
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
	}
}

// JournalTail returns the last n journal records that can be decoded, newest
// first. It is used by svnadmin for operator inspection.
func (r *Repo) JournalTail(n int) ([]*CommitRecord, error) {
	head, err := r.HeadFresh()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	var out []*CommitRecord
	for rev := head; rev > 0 && len(out) < n; rev-- {
		b, err := r.store.Get(ctx, r.keys.Journal(rev), nil)
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
