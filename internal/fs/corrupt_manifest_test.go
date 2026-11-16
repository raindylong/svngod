package fs

import (
	"context"
	"testing"

	"svngod/internal/cdc"
)

// TestCollectTextsKeepsChunksOfCorruptManifest drives a real repository through
// the failure that matters: a committed revision whose chunk manifest no
// longer parses. CollectTexts must refuse to collect chunks rather than treat
// the revision as referencing nothing.
//
// This is reachable in practice because CommitTxn accepts a caller-supplied
// ChunkManifest (an upload path writes chunks first, then commits the manifest
// it was given), so a truncated or bit-flipped manifest can be committed.
func TestCollectTextsKeepsChunksOfCorruptManifest(t *testing.T) {
	r, data := chunkedFixture(t, 5<<20)
	if _, err := r.CommitTxn(0, []Change{{Path: "/f", Action: ActionAdd, Kind: KindFile, Data: data}}, nil); err != nil {
		t.Fatal(err)
	}
	before := len(chunkKeys(t, r))
	if before == 0 {
		t.Fatal("fixture produced no chunks")
	}
	nr, err := r.Resolve(1, "/f")
	if err != nil {
		t.Fatal(err)
	}
	if _, perr := cdc.Parse(nr.Text); perr != nil {
		t.Fatalf("fixture manifest should parse: %v", perr)
	}
	bad := append([]byte(nil), nr.Text...)
	for i := 0; i < 8; i++ {
		bad[i] ^= 0xff
	}
	if _, perr := cdc.Parse(bad); perr == nil {
		t.Fatal("corrupted magic must not parse")
	}
	if _, err := r.CommitTxn(1, []Change{{
		Path: "/f", Action: ActionModify, Kind: KindFile,
		ChunkManifest: bad, ChunkTotal: nr.TotalSize,
	}}, nil); err != nil {
		t.Skipf("commit rejects malformed manifests, so the state is unreachable: %v", err)
	}

	// GC must now refuse rather than delete the chunks that revision needs.
	if _, err := r.CollectTexts(context.Background()); err == nil {
		t.Fatal("CollectTexts proceeded with an incomplete chunk reference set")
	}
	if after := len(chunkKeys(t, r)); after < before {
		t.Fatalf("GC deleted live chunks: %d -> %d", before, after)
	}
	// The intact file is still readable, which is the property that matters.
	if _, err := r.GetText(nr.Node); err != nil {
		t.Fatalf("file unreadable after GC refused: %v", err)
	}
}
