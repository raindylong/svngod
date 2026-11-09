package fs

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"

	"svngod/internal/fs/store"
)

// Ranged reads
//
// A merged segment can be several megabytes, and checkout/log resolve scattered
// revision ids, so reading a whole segment per revision made a single-entry
// read cost megabytes of transfer. Segment objects are therefore written in
// version 2, which puts a fixed-width row table right after the header:
//
//	magic(8) version(4)=2 first(8) count(8) parent(8)
//	rows: count × [blockOff(8) blockLen(4) propsLen(4) crc32c(4)]
//	entries: [blockLen(4) propsLen(4) block props] × count
//	crc32c over everything above(4)
//
// The table sits at the front, so a reader that knows count (from the shard
// index) can fetch the header and table with one small ranged GET and then
// fetch only the entry it needs. Versions 1 (no table) remain readable and
// always take the whole-object path.
const (
	segHeaderLen = 36
	segRowLen    = 20
	segMaxEntry  = 512 << 20
)

// segRow locates one entry inside a segment.
type segRow struct {
	off      uint64
	blkLen   uint32
	propsLen uint32
	crc      uint32
}

func (r segRow) size() uint64 { return uint64(r.blkLen) + uint64(r.propsLen) }

// segMeta is the parsed front matter of a version-2 segment.
type segMeta struct {
	first, count, parent uint64
	rows                 []segRow
}

var errSegmentV1 = errors.New("fs: segment is version 1 (no entry table)")

// decodeSegmentMeta parses the header and row table of a version-2 segment.
// b must be exactly the first segHeaderLen+segRowLen*count bytes.
func decodeSegmentMeta(b []byte, first, count uint64) (*segMeta, error) {
	if len(b) < segHeaderLen || string(b[:8]) != segmentMagic {
		return nil, errors.New("fs: bad segment magic")
	}
	if v := binary.BigEndian.Uint32(b[8:12]); v != 2 {
		return nil, errSegmentV1
	}
	m := &segMeta{
		first:  binary.BigEndian.Uint64(b[12:20]),
		count:  binary.BigEndian.Uint64(b[20:28]),
		parent: binary.BigEndian.Uint64(b[28:36]),
	}
	if m.first != first || m.count != count {
		return nil, fmt.Errorf("fs: segment header %d+%d does not match index %d+%d", m.first, m.count, first, count)
	}
	want := segHeaderLen + int(count)*segRowLen
	if len(b) != want {
		return nil, fmt.Errorf("fs: segment table length %d, want %d", len(b), want)
	}
	m.rows = make([]segRow, count)
	var prev uint64
	for i := uint64(0); i < count; i++ {
		p := b[segHeaderLen+int(i)*segRowLen:]
		row := segRow{
			off:      binary.BigEndian.Uint64(p[0:8]),
			blkLen:   binary.BigEndian.Uint32(p[8:12]),
			propsLen: binary.BigEndian.Uint32(p[12:16]),
			crc:      binary.BigEndian.Uint32(p[16:20]),
		}
		if row.off < uint64(want) || row.off < prev {
			return nil, fmt.Errorf("fs: segment entry %d has bad offset %d", i, row.off)
		}
		if row.size() > segMaxEntry {
			return nil, fmt.Errorf("fs: segment entry %d too large (%d bytes)", i, row.size())
		}
		prev = row.off
		m.rows[i] = row
	}
	return m, nil
}

// decodeSegmentEntry parses one entry body and checks its checksum. It returns
// subslices of b.
func decodeSegmentEntry(b []byte, row segRow) ([]byte, []byte, error) {
	if uint64(len(b)) != row.size() {
		return nil, nil, fmt.Errorf("fs: segment entry is %d bytes, want %d", len(b), row.size())
	}
	if got := crc32.Checksum(b, segmentCRC); got != row.crc {
		return nil, nil, errors.New("fs: segment entry checksum mismatch")
	}
	return b[:row.blkLen], b[row.blkLen:], nil
}

// segMetaCache holds parsed tables. A table is ~20 bytes per revision, so a
// few hundred entries cost a couple of megabytes and remove one round trip per
// read.
type segMetaCache struct {
	mu    sync.Mutex
	items map[string]*segMeta
	order []string
	limit int
}

// segDecodeCache holds fully decoded segments (last few touched). Sequential
// scans (log, blame, dump, verify) read every revision of a segment; without
// this each revision re-decoded the whole object.
type segDecodeCache struct {
	mu    sync.Mutex
	items map[string]*segmentData
	order []string
	limit int
}

func newSegMetaCache(limit int) *segMetaCache {
	return &segMetaCache{items: map[string]*segMeta{}, limit: limit}
}

func newSegDecodeCache(limit int) *segDecodeCache {
	return &segDecodeCache{items: map[string]*segmentData{}, limit: limit}
}

func (c *segMetaCache) get(key string) *segMeta {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.items[key]
	if m != nil {
		for i, k := range c.order {
			if k == key {
				c.order = append(append(c.order[:i], c.order[i+1:]...), key)
				break
			}
		}
	}
	return m
}

func (c *segMetaCache) put(key string, m *segMeta) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.items[key]; !ok {
		c.order = append(c.order, key)
	}
	c.items[key] = m
	for len(c.order) > c.limit {
		old := c.order[0]
		c.order = c.order[1:]
		delete(c.items, old)
	}
}

func (c *segDecodeCache) get(key string) *segmentData {
	c.mu.Lock()
	defer c.mu.Unlock()
	d := c.items[key]
	if d != nil {
		for i, k := range c.order {
			if k == key {
				c.order = append(append(c.order[:i], c.order[i+1:]...), key)
				break
			}
		}
	}
	return d
}

func (c *segDecodeCache) put(key string, s *segmentData) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.items[key]; !ok {
		c.order = append(c.order, key)
	}
	c.items[key] = s
	for len(c.order) > c.limit {
		old := c.order[0]
		c.order = c.order[1:]
		delete(c.items, old)
	}
}

// loadSegmentMeta reads and caches a segment's v2 table. errSegmentV1 means the
// caller must read the whole object instead.
func (r *Repo) loadSegmentMeta(ctx context.Context, ref segmentRef) (*segMeta, error) {
	objKey := r.segmentKeyFor(ref.First, ref.Count)
	if m := r.segMetas.get(objKey); m != nil {
		return m, nil
	}
	b, err := r.store.Get(ctx, objKey, &store.Range{
		Offset: 0,
		Length: int64(segHeaderLen + int(ref.Count)*segRowLen),
	})
	if err != nil {
		return nil, err
	}
	m, err := decodeSegmentMeta(b, ref.First, ref.Count)
	if err != nil {
		return nil, err
	}
	r.segMetas.put(objKey, m)
	return m, nil
}

// readSegmentEntry fetches one revision's block and props with two small ranged
// GETs (table, then entry) instead of the whole segment.
func (r *Repo) readSegmentEntry(ctx context.Context, ref segmentRef, idx uint64) ([]byte, []byte, error) {
	m, err := r.loadSegmentMeta(ctx, ref)
	if err != nil {
		return nil, nil, err
	}
	if idx >= m.count {
		return nil, nil, fmt.Errorf("fs: revision index %d outside segment %d+%d", idx, ref.First, ref.Count)
	}
	row := m.rows[idx]
	objKey := r.segmentKeyFor(ref.First, ref.Count)
	// row.off points at the entry's 8-byte length header; the body follows it.
	b, err := r.store.Get(ctx, objKey, &store.Range{Offset: int64(row.off) + 8, Length: int64(row.size())})
	if err != nil {
		return nil, nil, err
	}
	blk, props, err := decodeSegmentEntry(b, row)
	if err != nil {
		return nil, nil, err
	}
	return blk, props, nil
}

// verifySegmentFull reads the whole segment, checks the index SHA-256 and
// returns it decoded. Verify must take this path: ranged reads can only check
// the per-entry CRC, not the object digest.
func (r *Repo) verifySegmentFull(ref segmentRef) (*segmentData, error) {
	objKey := r.segmentKeyFor(ref.First, ref.Count)
	b, err := r.store.Get(context.Background(), objKey, nil)
	if err != nil {
		return nil, err
	}
	if ref.SHA256 != "" {
		sum := sha256.Sum256(b)
		if hex.EncodeToString(sum[:]) != ref.SHA256 {
			return nil, fmt.Errorf("fs: segment %s checksum mismatch", objKey)
		}
	}
	return decodeSegment(b)
}

