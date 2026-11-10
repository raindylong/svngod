package fs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"svngod/internal/cdc"
	"svngod/internal/fs/store"
)

// TestVerifyDetectsSwappedSegmentEntries swaps two revisions inside one segment
// and re-signs the index. Bytes and CRCs are valid; only the semantic check can
// notice that revision 1's block now says revision 2.
func TestVerifyDetectsSwappedSegmentEntries(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepoSegments(st, "repos/sem", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	commitBatches(t, r, 1, 2) // revs 1..2, one segment
	refs, err := r.loadSegmentIndex(0)
	if err != nil || len(refs) != 1 {
		t.Fatalf("index=%+v err=%v", refs, err)
	}
	seg, err := r.loadSegment(refs[0])
	if err != nil {
		t.Fatal(err)
	}
	if seg.Count != 2 {
		t.Fatalf("segment count=%d", seg.Count)
	}
	seg.Blocks[0], seg.Blocks[1] = seg.Blocks[1], seg.Blocks[0]
	seg.Props[0], seg.Props[1] = seg.Props[1], seg.Props[0]
	data := encodeSegment(seg)
	if err := st.Put(context.Background(), r.segmentKeyFor(refs[0].First, refs[0].Count), data); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	refs[0].SHA256 = hex.EncodeToString(sum[:])
	if err := st.Put(context.Background(), r.keys.SegmentIndex(0), encodeSegmentIndex(refs)); err != nil {
		t.Fatal(err)
	}
	r2, err := OpenRepo(st, "repos/sem")
	if err != nil {
		t.Fatal(err)
	}
	issues, err := r2.VerifyRange(1, 2, false)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, is := range issues {
		if strings.Contains(is.Err.Error(), "block belongs to revision") {
			found = true
		}
	}
	if !found {
		t.Fatalf("verify missed swapped entries: %+v", issues)
	}
}

// TestVerifyDetectsMissingRevPropOverride deletes an override object while the
// shard list still names it: reads must not silently revert to the committed
// value, and verify must report the loss.
func TestVerifyDetectsMissingRevPropOverride(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepoSegments(st, "repos/semprops", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	commitBatches(t, r, 1, 2)
	if err := r.UpdateRevProps(1, map[string]string{"svn:log": "edited"}, nil); err != nil {
		t.Fatal(err)
	}
	key := r.keys.SegRevPropsOverride(0, 1)
	if err := st.Delete(context.Background(), []string{key}); err != nil {
		t.Fatal(err)
	}
	r2, err := OpenRepo(st, "repos/semprops")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r2.GetRevProps(1); err == nil {
		t.Fatal("missing override silently fell back to the committed value")
	}
	issues, err := r2.VerifyRange(1, 2, false)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, is := range issues {
		if strings.Contains(is.Err.Error(), "property override is missing") {
			found = true
		}
	}
	if !found {
		t.Fatalf("verify missed the missing override: %+v", issues)
	}
}
