package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"svngod/internal/cdc"
	"svngod/internal/fs/store"
)

// TestGCDryRunReportsWithoutDeleting: a dry run must report the same candidates
// as a real pass while leaving every object in place.
func TestGCDryRunReportsWithoutDeleting(t *testing.T) {
	dir := t.TempDir()
	st, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	r, err := CreateRepoSegments(st, "repos/dry", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	commitBatches(t, r, 1, 2)
	ctx := context.Background()
	orphanSeg := r.keys.Segment(0, 9, 1)
	if err := st.Put(ctx, orphanSeg, []byte("orphan")); err != nil {
		t.Fatal(err)
	}
	orphanText := r.keys.Prefix + "/texts/" + "abababababababababababababababababababababababababababababababab"
	if err := st.Put(ctx, orphanText, []byte("orphan text")); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	for _, key := range []string{orphanSeg, orphanText} {
		p := filepath.Join(dir, filepath.FromSlash(key))
		if err := os.Chtimes(p, old, old); err != nil {
			t.Fatal(err)
		}
	}

	var segBytes, textBytes int64
	segProgress := func(phase string, done, total int, b int64) { segBytes = b }
	segN, err := r.CollectOrphanSegmentsProgress(ctx, 0, true, segProgress)
	if err != nil || segN != 1 {
		t.Fatalf("dry segments=%d err=%v, want 1", segN, err)
	}
	if segBytes != int64(len("orphan")) {
		t.Fatalf("dry segment bytes=%d, want %d", segBytes, len("orphan"))
	}
	textProgress := func(phase string, done, total int, b int64) { textBytes = b }
	textN, err := r.CollectTextsGraceProgress(ctx, 0, true, textProgress)
	if err != nil || textN != 1 {
		t.Fatalf("dry texts=%d err=%v, want 1", textN, err)
	}
	if textBytes != int64(len("orphan text")) {
		t.Fatalf("dry text bytes=%d, want %d", textBytes, len("orphan text"))
	}
	for _, key := range []string{orphanSeg, orphanText} {
		if ok, _ := st.Exists(ctx, key); !ok {
			t.Fatalf("dry run deleted %s", key)
		}
	}

	// A real pass removes exactly those objects.
	if _, err := r.CollectOrphanSegments(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := r.CollectTextsGrace(ctx, 0); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{orphanSeg, orphanText} {
		if ok, _ := st.Exists(ctx, key); ok {
			t.Fatalf("real pass left %s", key)
		}
	}
}

// TestGCCompactionDryRun: dry-run counts shards that need merging without
// rewriting any index.
func TestGCCompactionDryRun(t *testing.T) {
	st := store.NewMemoryStore()
	if _, err := CreateRepoSegments(st, "repos/dryc", 64, 64, cdc.DefaultParams(), false, 8); err != nil {
		t.Fatal(err)
	}
	r, err := OpenRepo(st, "repos/dryc")
	if err != nil {
		t.Fatal(err)
	}
	commitBatches(t, r, 32, 1) // 32 one-revision segments in one shard
	before, err := r.loadSegmentIndex(0)
	if err != nil {
		t.Fatal(err)
	}
	n, err := r.CompactSegmentsProgress(context.Background(), true, nil)
	if err != nil || n != 1 {
		t.Fatalf("dry compact=%d err=%v, want 1", n, err)
	}
	after, err := r.loadSegmentIndex(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("dry run changed the index: %d -> %d entries", len(before), len(after))
	}
}
