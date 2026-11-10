package fs

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"

	"svngod/internal/cdc"
	"svngod/internal/fs/store"
)

// TestVerifyTextsParallelRevision verifies a revision with many chunked files
// (the path that dispatches node checks to a worker pool) and checks that a
// corrupted object is reported once, not once per worker.
func TestVerifyTextsParallelRevision(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepoSegments(st, "repos/vpar", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	const files = 12
	changes := make([]Change, files)
	for i := range changes {
		data := make([]byte, 600<<10)
		rand.New(rand.NewSource(int64(i) + 1)).Read(data)
		manifest, total, merr := r.StoreChunkedReader(bytes.NewReader(data))
		if merr != nil {
			t.Fatal(merr)
		}
		sum := md5.Sum(data)
		changes[i] = Change{
			Path:          fmt.Sprintf("/f%02d.bin", i),
			Action:        ActionAdd,
			Kind:          KindFile,
			ChunkManifest: manifest,
			ChunkTotal:    uint64(total),
			DataMD5:       sum,
			HasMD5:        true,
		}
	}
	// One revision containing all files, so the per-revision node pool is used.
	if _, errs, cerr := r.CommitBatch([]BatchCommit{{Changes: changes}}); cerr != nil || errs[0] != nil {
		t.Fatalf("commit: %v %v", cerr, errs)
	}
	issues, err := r.VerifyRangeWith(1, 1, true, 1)
	if err != nil || len(issues) != 0 {
		t.Fatalf("clean verify: issues=%v err=%v", issues, err)
	}
	objs, _, lerr := st.ListPrefix(context.Background(), r.keys.ListingPrefix("chunks"), "", 100)
	if lerr != nil {
		t.Fatal(lerr)
	}
	if len(objs) == 0 {
		t.Fatal("no chunk objects created")
	}
	if err := st.Delete(context.Background(), []string{objs[0].Key}); err != nil {
		t.Fatal(err)
	}
	issues, err = r.VerifyRangeWith(1, 1, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) == 0 {
		t.Fatalf("corrupt chunk not reported (deleted %s)", objs[0].Key)
	}
	if len(issues) > 2 {
		t.Fatalf("duplicate reports for one corrupt chunk: %v", issues)
	}
}

// TestVerifyProgressCallback: the callback fires once per verified revision,
// including on the parallel path.
func TestVerifyProgressCallback(t *testing.T) {
	r := segRepo(t, "repos/vprog")
	commitBatches(t, r, 1, 4)
	var calls atomic.Int64
	issues, err := r.VerifyRangeWithProgress(1, 4, false, 1, func(uint64) { calls.Add(1) })
	if err != nil || len(issues) != 0 {
		t.Fatalf("issues=%v err=%v", issues, err)
	}
	if calls.Load() != 4 {
		t.Fatalf("progress calls=%d, want 4", calls.Load())
	}
}
