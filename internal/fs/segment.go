package fs

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"

	"svngod/internal/fs/store"
)

// Segment storage (format v2) bundles several consecutive revisions into one
// immutable object: their revision blocks and revision properties, with a
// checksum. A small per-shard index maps revision ranges to segment objects,
// so a commit batch writes one segment instead of two objects per revision
// (plus a journal record per revision).
//
//	segs/<shard>/<first>-<count>.seg   segment (this file)
//	segs/<shard>.idx                    ordered list of segments
const segmentMagic = "SVNGSEG1"

// segmentData is the decoded content of one segment.
type segmentData struct {
	First  uint64
	Count  uint64
	Parent uint64
	Blocks [][]byte
	Props  [][]byte
}

// segmentRef is one entry of the per-shard index.
type segmentRef struct {
	First  uint64 `json:"first"`
	Count  uint64 `json:"count"`
	SHA256 string `json:"sha256"`
}

var segmentCRC = crc32.MakeTable(crc32.Castagnoli)

// encodeSegment serializes a segment in version 2 (with the front row table).
// Layout is documented in segment_read.go.
func encodeSegment(s *segmentData) []byte {
	if uint64(len(s.Blocks)) != s.Count || uint64(len(s.Props)) != s.Count {
		panic("fs: segment block/props count mismatch")
	}
	tableLen := int(s.Count) * segRowLen
	size := segHeaderLen + tableLen + 4
	for i := range s.Blocks {
		size += 8 + len(s.Blocks[i]) + len(s.Props[i])
	}
	out := make([]byte, 0, size)
	out = append(out, segmentMagic...)
	out = binary.BigEndian.AppendUint32(out, 2)
	out = binary.BigEndian.AppendUint64(out, s.First)
	out = binary.BigEndian.AppendUint64(out, s.Count)
	out = binary.BigEndian.AppendUint64(out, s.Parent)
	tableAt := len(out)
	out = append(out, make([]byte, tableLen)...)
	for i := range s.Blocks {
		off := uint64(len(out))
		out = binary.BigEndian.AppendUint32(out, uint32(len(s.Blocks[i])))
		out = binary.BigEndian.AppendUint32(out, uint32(len(s.Props[i])))
		bodyAt := len(out)
		out = append(out, s.Blocks[i]...)
		out = append(out, s.Props[i]...)
		crc := crc32.Checksum(out[bodyAt:], segmentCRC)
		row := out[tableAt+i*segRowLen:]
		binary.BigEndian.PutUint64(row[0:8], off)
		binary.BigEndian.PutUint32(row[8:12], uint32(len(s.Blocks[i])))
		binary.BigEndian.PutUint32(row[12:16], uint32(len(s.Props[i])))
		binary.BigEndian.PutUint32(row[16:20], crc)
	}
	crc := crc32.Checksum(out, segmentCRC)
	return binary.BigEndian.AppendUint32(out, crc)
}

// decodeSegment validates and parses a whole segment (version 1 or 2).
func decodeSegment(b []byte) (*segmentData, error) {
	if len(b) < 40 || string(b[:8]) != segmentMagic {
		return nil, errors.New("fs: bad segment magic")
	}
	want := binary.BigEndian.Uint32(b[len(b)-4:])
	if crc32.Checksum(b[:len(b)-4], segmentCRC) != want {
		return nil, errors.New("fs: segment checksum mismatch")
	}
	version := binary.BigEndian.Uint32(b[8:12])
	if version != 1 && version != 2 {
		return nil, fmt.Errorf("fs: unsupported segment version %d", version)
	}
	s := &segmentData{
		First:  binary.BigEndian.Uint64(b[12:20]),
		Count:  binary.BigEndian.Uint64(b[20:28]),
		Parent: binary.BigEndian.Uint64(b[28:36]),
	}
	if s.Count == 0 || s.Count > 1<<20 {
		return nil, fmt.Errorf("fs: invalid segment count %d", s.Count)
	}
	off := segHeaderLen
	end := len(b) - 4
	if version == 2 {
		if end < segHeaderLen+int(s.Count)*segRowLen {
			return nil, errors.New("fs: truncated segment table")
		}
		if _, err := decodeSegmentMeta(b[:segHeaderLen+int(s.Count)*segRowLen], s.First, s.Count); err != nil {
			return nil, err
		}
		off += int(s.Count) * segRowLen
	}
	s.Blocks = make([][]byte, 0, s.Count)
	s.Props = make([][]byte, 0, s.Count)
	for i := uint64(0); i < s.Count; i++ {
		if off+8 > end {
			return nil, errors.New("fs: truncated segment entry")
		}
		bl := int(binary.BigEndian.Uint32(b[off : off+4]))
		pl := int(binary.BigEndian.Uint32(b[off+4 : off+8]))
		off += 8
		if bl < 0 || pl < 0 || off+bl+pl > end {
			return nil, errors.New("fs: truncated segment body")
		}
		s.Blocks = append(s.Blocks, b[off:off+bl])
		off += bl
		s.Props = append(s.Props, b[off:off+pl])
		off += pl
	}
	if off != end {
		return nil, errors.New("fs: trailing bytes in segment")
	}
	return s, nil
}

// encodeSegmentIndex serializes the per-shard index.
func encodeSegmentIndex(refs []segmentRef) []byte {
	b, _ := json.Marshal(refs)
	return b
}

// decodeSegmentIndex parses an index; an absent index is an empty one.
func decodeSegmentIndex(b []byte) ([]segmentRef, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var refs []segmentRef
	if err := json.Unmarshal(b, &refs); err != nil {
		return nil, fmt.Errorf("fs: segment index: %w", err)
	}
	for i := 1; i < len(refs); i++ {
		if refs[i].First <= refs[i-1].First {
			return nil, errors.New("fs: segment index is not ordered")
		}
	}
	return refs, nil
}

// findSegment returns the segment covering rev.
func findSegment(refs []segmentRef, rev uint64) (*segmentRef, bool) {
	lo, hi := 0, len(refs)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		r := &refs[mid]
		switch {
		case rev < r.First:
			hi = mid - 1
		case rev >= r.First+r.Count:
			lo = mid + 1
		default:
			return r, true
		}
	}
	return nil, false
}

// loadSegmentIndex reads the index for one shard (nil when absent).
func (r *Repo) loadSegmentIndex(shard uint64) ([]segmentRef, error) {
	b, err := r.store.Get(context.Background(), r.keys.SegmentIndex(shard), nil)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return decodeSegmentIndex(b)
}
