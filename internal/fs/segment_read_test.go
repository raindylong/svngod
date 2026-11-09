package fs

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"strings"
	"sync"
	"testing"

	"svngod/internal/cdc"
	"svngod/internal/fs/store"
)

// readSizer records how many bytes each Get transferred, keyed by object key.
type readSizer struct {
	store.Store
	mu    sync.Mutex
	bytes map[string]int64
	calls map[string]int
}

func (s *readSizer) Get(ctx context.Context, key string, rng *store.Range) ([]byte, error) {
	b, err := s.Store.Get(ctx, key, rng)
	s.mu.Lock()
	if s.bytes == nil {
		s.bytes = map[string]int64{}
		s.calls = map[string]int{}
	}
	s.bytes[key] += int64(len(b))
	s.calls[key]++
	s.mu.Unlock()
	return b, err
}

func (s *readSizer) transferred(key string) (int64, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bytes[key], s.calls[key]
}

func (s *readSizer) reset() {
	s.mu.Lock()
	s.bytes = map[string]int64{}
	s.calls = map[string]int{}
	s.mu.Unlock()
}

func commitN(t *testing.T, r *Repo, n, per int) {
	t.Helper()
	for b := 0; b < n; b++ {
		reqs := make([]BatchCommit, per)
		for i := range reqs {
			reqs[i] = BatchCommit{Changes: []Change{{
				Path:   fmt.Sprintf("/r%d-%d", b, i),
				Action: ActionAdd,
				Kind:   KindFile,
				// Compressible but non-trivial payload keeps segments realistic.
				Data: []byte(strings.Repeat("abcd", 256)),
			}}}
		}
		if _, errs, err := r.CommitBatch(reqs); err != nil {
			t.Fatal(err)
		} else {
			for _, e := range errs {
				if e != nil {
					t.Fatal(e)
				}
			}
		}
	}
}

// TestSegmentRangedReadTransfer checks that reading one revision of a large
// merged segment transfers the table plus one entry, not the whole object.
func TestSegmentRangedReadTransfer(t *testing.T) {
	base := store.NewMemoryStore()
	rs := &readSizer{Store: base}
	r, err := CreateRepoSegments(rs, "repos/rr", 64, 64, cdc.DefaultParams(), false, 64)
	if err != nil {
		t.Fatal(err)
	}
	commitN(t, r, 1, 32) // one batch of 32 revisions = one ~32 KiB segment
	// Reopen so no decoded/block cache hides the network cost.
	r2, err := OpenRepo(rs, "repos/rr")
	if err != nil {
		t.Fatal(err)
	}
	refs, err := r2.loadSegmentIndex(0)
	if err != nil || len(refs) != 1 {
		t.Fatalf("index: %v %v", refs, err)
	}
	segKey := r2.segmentKeyFor(refs[0].First, refs[0].Count)
	size := int64(len(mustSegment(t, base, segKey)))
	rs.reset()
	if _, err := r2.readBlock(refs[0].First + 20); err != nil {
		t.Fatal(err)
	}
	got, calls := rs.transferred(segKey)
	if got >= size/4 {
		t.Fatalf("ranged read transferred %d of %d bytes (%d calls)", got, size, calls)
	}
	if calls < 2 {
		t.Fatalf("expected table + entry GETs, got %d", calls)
	}
}

func mustSegment(t *testing.T, st store.Store, key string) []byte {
	t.Helper()
	b, err := st.Get(context.Background(), key, nil)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestSegmentSequentialReadLoadsWhole checks that a scan (rev, rev+1, ...)
// switches to one whole-segment fetch instead of one GET per revision.
func TestSegmentSequentialReadLoadsWhole(t *testing.T) {
	base := store.NewMemoryStore()
	rs := &readSizer{Store: base}
	r, err := CreateRepoSegments(rs, "repos/seq", 64, 64, cdc.DefaultParams(), false, 64)
	if err != nil {
		t.Fatal(err)
	}
	commitN(t, r, 1, 32)
	r2, err := OpenRepo(rs, "repos/seq")
	if err != nil {
		t.Fatal(err)
	}
	refs, err := r2.loadSegmentIndex(0)
	if err != nil || len(refs) != 1 {
		t.Fatalf("index: %v %v", refs, err)
	}
	segKey := r2.segmentKeyFor(refs[0].First, refs[0].Count)
	size := int64(len(mustSegment(t, base, segKey)))
	rs.reset()
	for rev := uint64(10); rev < 20; rev++ {
		if _, err := r2.readBlock(rev); err != nil {
			t.Fatalf("read %d: %v", rev, err)
		}
	}
	got, calls := rs.transferred(segKey)
	if calls > 3 {
		t.Fatalf("sequential scan made %d GETs for 10 revisions", calls)
	}
	if got < size {
		t.Fatalf("expected one whole-segment fetch, transferred %d of %d", got, size)
	}
}

// encodeSegmentV1 writes the pre-table format so the fallback stays tested.
func encodeSegmentV1(s *segmentData) []byte {
	out := make([]byte, 0, 128)
	out = append(out, segmentMagic...)
	out = binary.BigEndian.AppendUint32(out, 1)
	out = binary.BigEndian.AppendUint64(out, s.First)
	out = binary.BigEndian.AppendUint64(out, s.Count)
	out = binary.BigEndian.AppendUint64(out, s.Parent)
	for i := range s.Blocks {
		out = binary.BigEndian.AppendUint32(out, uint32(len(s.Blocks[i])))
		out = binary.BigEndian.AppendUint32(out, uint32(len(s.Props[i])))
		out = append(out, s.Blocks[i]...)
		out = append(out, s.Props[i]...)
	}
	return binary.BigEndian.AppendUint32(out, crc32.Checksum(out, segmentCRC))
}

// TestSegmentV1Fallback keeps old segment objects readable: the ranged path
// must notice there is no table and read the whole object.
func TestSegmentV1Fallback(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepoSegments(st, "repos/v1fall", 64, 64, cdc.DefaultParams(), false, 64)
	if err != nil {
		t.Fatal(err)
	}
	commitN(t, r, 1, 4)
	refs, err := r.loadSegmentIndex(0)
	if err != nil || len(refs) != 1 {
		t.Fatalf("index: %+v %v", refs, err)
	}
	ref := refs[0]
	key := r.segmentKeyFor(ref.First, ref.Count)
	seg, err := r.loadSegment(ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Put(context.Background(), key, encodeSegmentV1(seg)); err != nil {
		t.Fatal(err)
	}
	refs[0].SHA256 = ""
	if err := st.Put(context.Background(), r.keys.SegmentIndex(0), encodeSegmentIndex(refs)); err != nil {
		t.Fatal(err)
	}
	r2, err := OpenRepo(st, "repos/v1fall")
	if err != nil {
		t.Fatal(err)
	}
	for rev := uint64(1); rev <= 4; rev++ {
		if _, err := r2.readBlock(rev); err != nil {
			t.Fatalf("v1 fallback read %d: %v", rev, err)
		}
		if _, err := r2.loadRevProps(rev); err != nil {
			t.Fatalf("v1 fallback props %d: %v", rev, err)
		}
	}
}

// TestSegmentRangedCRC catches a corrupted entry byte on the ranged path.
func TestSegmentRangedCRC(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepoSegments(st, "repos/crc", 64, 64, cdc.DefaultParams(), false, 64)
	if err != nil {
		t.Fatal(err)
	}
	commitN(t, r, 1, 8)
	refs, err := r.loadSegmentIndex(0)
	if err != nil || len(refs) != 1 {
		t.Fatalf("index: %+v %v", refs, err)
	}
	key := r.segmentKeyFor(refs[0].First, refs[0].Count)
	raw := mustSegment(t, st, key)
	meta, err := decodeSegmentMeta(raw[:segHeaderLen+int(refs[0].Count)*segRowLen], refs[0].First, refs[0].Count)
	if err != nil {
		t.Fatal(err)
	}
	row := meta.rows[3]
	raw[row.off+8+uint64(row.blkLen)/2] ^= 0xff
	if err := st.Put(context.Background(), key, raw); err != nil {
		t.Fatal(err)
	}
	r2, err := OpenRepo(st, "repos/crc")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r2.readBlock(4); err == nil {
		t.Fatal("corrupt entry read succeeded")
	}
	// Another entry in the same segment must stay readable.
	if _, err := r2.readBlock(6); err != nil {
		t.Fatalf("intact entry unreadable: %v", err)
	}
}

// TestVerifyChecksIndexSHA ensures verify uses whole-object reads: a segment
// whose bytes no longer match the index digest must be reported.
func TestVerifyChecksIndexSHA(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepoSegments(st, "repos/shav", 64, 64, cdc.DefaultParams(), false, 64)
	if err != nil {
		t.Fatal(err)
	}
	commitN(t, r, 1, 8)
	refs, err := r.loadSegmentIndex(0)
	if err != nil || len(refs) != 1 {
		t.Fatalf("index: %+v %v", refs, err)
	}
	key := r.segmentKeyFor(refs[0].First, refs[0].Count)
	raw := mustSegment(t, st, key)
	refs[0].SHA256 = "0000000000000000000000000000000000000000000000000000000000000000"
	if err := st.Put(context.Background(), r.keys.SegmentIndex(0), encodeSegmentIndex(refs)); err != nil {
		t.Fatal(err)
	}
	r2, err := OpenRepo(st, "repos/shav")
	if err != nil {
		t.Fatal(err)
	}
	issues, err := r2.VerifyRange(1, 8, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) == 0 {
		t.Fatalf("verify missed index digest mismatch (segment %d bytes)", len(raw))
	}
	if !r2.fullReads.Load() == false {
		t.Fatal("verify left fullReads set")
	}
}

// TestVerifyReadsSegmentOnce guards the verify path: it must read each segment
// object once (to check the index digest), not once per revision.
func TestVerifyReadsSegmentOnce(t *testing.T) {
	base := store.NewMemoryStore()
	rs := &readSizer{Store: base}
	r, err := CreateRepoSegments(rs, "repos/vonce", 64, 64, cdc.DefaultParams(), false, 64)
	if err != nil {
		t.Fatal(err)
	}
	commitN(t, r, 1, 32)
	r2, err := OpenRepo(rs, "repos/vonce")
	if err != nil {
		t.Fatal(err)
	}
	refs, err := r2.loadSegmentIndex(0)
	if err != nil || len(refs) != 1 {
		t.Fatalf("index: %v %v", refs, err)
	}
	segKey := r2.segmentKeyFor(refs[0].First, refs[0].Count)
	rs.reset()
	issues, err := r2.VerifyRange(1, 32, false)
	if err != nil || len(issues) != 0 {
		t.Fatalf("verify issues=%v err=%v", issues, err)
	}
	if _, calls := rs.transferred(segKey); calls > 1 {
		t.Fatalf("verify read the segment %d times for 32 revisions", calls)
	}
}
