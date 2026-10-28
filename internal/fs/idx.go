package fs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
)

const shardIdxMagic = "SVNGIDX1"

// ShardIdx maps revision -> rev-block location inside revs/<shard>.pack.
type ShardIdx struct {
	Shard    uint64
	FirstRev uint64
	revTable []revTableEntry // entry i corresponds to rev = FirstRev + i
}

type revTableEntry struct {
	BlockOffset uint64
	BlockLen    uint64
}

// NewShardIdx builds an index from a packed shard's block list.
func NewShardIdx(shard, firstRev uint64, blocks []revTableEntry) *ShardIdx {
	return &ShardIdx{Shard: shard, FirstRev: firstRev, revTable: blocks}
}

// Marshal serializes the index with a trailing CRC32.
func (s *ShardIdx) Marshal() []byte {
	var b bytes.Buffer
	b.WriteString(shardIdxMagic)
	var hdr [16]byte
	binary.BigEndian.PutUint64(hdr[0:], s.Shard)
	binary.BigEndian.PutUint64(hdr[8:], s.FirstRev)
	b.Write(hdr[:])
	body := b.Bytes()
	for _, e := range s.revTable {
		var row [16]byte
		binary.BigEndian.PutUint64(row[0:], e.BlockOffset)
		binary.BigEndian.PutUint64(row[8:], e.BlockLen)
		body = append(body, row[:]...)
	}
	var crc [4]byte
	binary.BigEndian.PutUint32(crc[:], crc32.ChecksumIEEE(body))
	return append(body, crc[:]...)
}

// UnmarshalShardIdx decodes an index.
func UnmarshalShardIdx(b []byte) (*ShardIdx, error) {
	if len(b) < len(shardIdxMagic)+16+4 {
		return nil, errors.New("fs: shard idx too short")
	}
	if string(b[:len(shardIdxMagic)]) != shardIdxMagic {
		return nil, errors.New("fs: bad shard-idx magic")
	}
	body := b[:len(b)-4]
	if want := binary.BigEndian.Uint32(b[len(b)-4:]); crc32.ChecksumIEEE(body) != want {
		return nil, errors.New("fs: shard idx CRC mismatch")
	}
	off := len(shardIdxMagic)
	s := &ShardIdx{
		Shard:    binary.BigEndian.Uint64(body[off:]),
		FirstRev: binary.BigEndian.Uint64(body[off+8:]),
	}
	off += 16
	n := (len(body) - off) / 16
	s.revTable = make([]revTableEntry, n)
	for i := 0; i < n; i++ {
		row := body[off+i*16 : off+(i+1)*16]
		s.revTable[i] = revTableEntry{
			BlockOffset: binary.BigEndian.Uint64(row[0:]),
			BlockLen:    binary.BigEndian.Uint64(row[8:]),
		}
	}
	return s, nil
}

// Lookup returns the block location for a revision.
func (s *ShardIdx) Lookup(rev uint64) (off, ln uint64, ok bool) {
	if rev < s.FirstRev {
		return 0, 0, false
	}
	i := int(rev - s.FirstRev)
	if i >= len(s.revTable) {
		return 0, 0, false
	}
	e := s.revTable[i]
	return e.BlockOffset, e.BlockLen, true
}

// Count returns the number of revisions indexed.
func (s *ShardIdx) Count() int { return len(s.revTable) }
