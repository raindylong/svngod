package fs

import (
	"bytes"
	"testing"

	"svngod/internal/fs/store"
)

func TestSegmentRoundTrip(t *testing.T) {
	seg := &segmentData{First: 7, Count: 3, Parent: 6}
	for i := 0; i < 3; i++ {
		seg.Blocks = append(seg.Blocks, bytes.Repeat([]byte{byte('a' + i)}, 100+i*50))
		seg.Props = append(seg.Props, []byte(`{"svn:log":"r`+string(rune('7'+i))+`"}`))
	}
	b := encodeSegment(seg)
	got, err := decodeSegment(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.First != 7 || got.Count != 3 || got.Parent != 6 {
		t.Fatalf("header=%+v", got)
	}
	for i := range seg.Blocks {
		if !bytes.Equal(got.Blocks[i], seg.Blocks[i]) || !bytes.Equal(got.Props[i], seg.Props[i]) {
			t.Fatalf("entry %d mismatch", i)
		}
	}
}

func TestSegmentCorruptionDetected(t *testing.T) {
	seg := &segmentData{First: 1, Count: 1, Parent: 0, Blocks: [][]byte{[]byte("block")}, Props: [][]byte{[]byte("props")}}
	b := encodeSegment(seg)
	b[len(b)/2] ^= 0xff
	if _, err := decodeSegment(b); err == nil {
		t.Fatal("corrupted segment accepted")
	}
	if _, err := decodeSegment(b[:len(b)-3]); err == nil {
		t.Fatal("truncated segment accepted")
	}
}

func TestFindSegment(t *testing.T) {
	refs := []segmentRef{
		{First: 1, Count: 10},
		{First: 11, Count: 5},
		{First: 20, Count: 1},
	}
	cases := map[uint64]uint64{1: 1, 10: 1, 11: 11, 15: 11, 20: 20}
	for rev, want := range cases {
		r, ok := findSegment(refs, rev)
		if !ok || r.First != want {
			t.Fatalf("rev %d -> %+v ok=%v, want first %d", rev, r, ok, want)
		}
	}
	for _, rev := range []uint64{0, 16, 19, 21, 100} {
		if _, ok := findSegment(refs, rev); ok {
			t.Fatalf("rev %d unexpectedly found", rev)
		}
	}
}

func TestSegmentIndexRoundTrip(t *testing.T) {
	refs := []segmentRef{{First: 1, Count: 5, Key: "k1"}, {First: 6, Count: 2, Key: "k2"}}
	got, err := decodeSegmentIndex(encodeSegmentIndex(refs))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[1].Key != "k2" {
		t.Fatalf("refs=%+v", got)
	}
	if _, err := decodeSegmentIndex(encodeSegmentIndex([]segmentRef{{First: 6}, {First: 3}})); err == nil {
		t.Fatal("unordered index accepted")
	}
}

func TestSegmentKeys(t *testing.T) {
	k := storeKeys("repos/demo")
	if got, want := k.Segment(2, 4097, 64), "repos/demo/segs/0000000002/000000004097-000000000064.seg"; got != want {
		t.Fatalf("segment key=%q want %q", got, want)
	}
	if got, want := k.SegmentIndex(2), "repos/demo/segs/0000000002.idx"; got != want {
		t.Fatalf("index key=%q want %q", got, want)
	}
}

func storeKeys(prefix string) *store.Keys { return store.NewKeys(prefix) }
