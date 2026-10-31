package fs

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"math/rand"
	"testing"

	"svngod/internal/fs/store"
)

func chunkedFixture(t *testing.T, size int64) (*Repo, []byte) {
	t.Helper()
	st := store.NewMemoryStore()
	r, err := CreateRepo(st, "chunk", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, size)
	rng := rand.New(rand.NewSource(42))
	rng.Read(data)
	return r, data
}

func chunkKeys(t *testing.T, r *Repo) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	next := ""
	for {
		objs, n, err := r.store.ListPrefix(context.Background(), r.keys.Prefix+"/chunks/", next, 500)
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range objs {
			out[o.Key] = true
		}
		if n == "" {
			break
		}
		next = n
	}
	return out
}

func TestChunkedTextRoundTripAndDedupe(t *testing.T) {
	r, data := chunkedFixture(t, 12<<20)
	if _, err := r.CommitTxn(0, []Change{{Path: "/big.bin", Action: ActionAdd, Kind: KindFile, Data: data}}, nil); err != nil {
		t.Fatal(err)
	}
	nr, err := r.Resolve(1, "/big.bin")
	if err != nil {
		t.Fatal(err)
	}
	if nr.TextRep != TextRepChunked {
		t.Fatalf("TextRep=%d, want chunked", nr.TextRep)
	}
	got, err := r.GetText(nr.Node)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(got) != sha256.Sum256(data) {
		t.Fatal("round-trip mismatch")
	}
	before := chunkKeys(t, r)
	if len(before) < 2 {
		t.Fatalf("chunks=%d, want multiple", len(before))
	}

	// Replace 1 MiB in the middle; only chunks around the edit should be new.
	data2 := append([]byte(nil), data...)
	rng := rand.New(rand.NewSource(7))
	rng.Read(data2[5<<20 : 6<<20])
	if _, err := r.CommitTxn(0, []Change{{Path: "/big.bin", Action: ActionModify, Kind: KindFile, Data: data2}}, nil); err != nil {
		t.Fatal(err)
	}
	nr2, err := r.Resolve(2, "/big.bin")
	if err != nil {
		t.Fatal(err)
	}
	if nr2.TextRep != TextRepChunked {
		t.Fatalf("edit TextRep=%d, want chunked", nr2.TextRep)
	}
	got2, err := r.GetText(nr2.Node)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(got2) != sha256.Sum256(data2) {
		t.Fatal("edited round-trip mismatch")
	}
	after := chunkKeys(t, r)
	newChunks := 0
	for k := range after {
		if !before[k] {
			newChunks++
		}
	}
	if newChunks > 6 {
		t.Fatalf("middle edit added %d chunks, want <= 6 (dedupe failed)", newChunks)
	}
}

func TestChunkedRangeAndVerify(t *testing.T) {
	r, data := chunkedFixture(t, 9<<20)
	if _, err := r.CommitTxn(0, []Change{{Path: "/f", Action: ActionAdd, Kind: KindFile, Data: data}}, nil); err != nil {
		t.Fatal(err)
	}
	nr, err := r.Resolve(1, "/f")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ off, length int64 }{
		{0, 100},
		{chunkMin - 10, 32},
		{3 << 20, 2 << 20},
		{int64(len(data)) - 7, 7},
	} {
		got, rerr := r.ReadTextRange(nr, c.off, c.length)
		if rerr != nil {
			t.Fatalf("range %d+%d: %v", c.off, c.length, rerr)
		}
		end := c.off + c.length
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		if !bytes.Equal(got, data[c.off:end]) {
			t.Fatalf("range %d+%d mismatch", c.off, c.length)
		}
	}
	if issues, err := r.VerifyRange(1, 1, true); err != nil || len(issues) != 0 {
		t.Fatalf("clean verify: %v %v", issues, err)
	}
	// Remove one chunk: metadata-only verification must notice.
	keys := chunkKeys(t, r)
	for k := range keys {
		if err := r.store.Delete(context.Background(), []string{k}); err != nil {
			t.Fatal(err)
		}
		break
	}
	issues, err := r.VerifyRange(1, 1, false)
	if err != nil || len(issues) == 0 {
		t.Fatalf("missing chunk not detected: issues=%v err=%v", issues, err)
	}
}

func TestCollectTextsKeepsChunks(t *testing.T) {
	r, data := chunkedFixture(t, 5<<20)
	if _, err := r.CommitTxn(0, []Change{{Path: "/f", Action: ActionAdd, Kind: KindFile, Data: data}}, nil); err != nil {
		t.Fatal(err)
	}
	before := len(chunkKeys(t, r))
	if _, err := r.CollectTexts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if after := len(chunkKeys(t, r)); after != before {
		t.Fatalf("GC removed referenced chunks: %d -> %d", before, after)
	}
	// An unreferenced chunk is collected.
	orphan := r.keys.Chunk("deadbeef")
	if err := r.store.Put(context.Background(), orphan, []byte("orphan")); err != nil {
		t.Fatal(err)
	}
	if _, err := r.CollectTexts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ok, _ := r.store.Exists(context.Background(), orphan); ok {
		t.Fatal("orphan chunk not collected")
	}
}

func TestChunkManifestNotReplacedByOldText(t *testing.T) {
	r, base := chunkedFixture(t, 5<<20)
	if _, err := r.CommitTxn(0, []Change{{Path: "/f", Action: ActionAdd, Kind: KindFile, Data: base}}, nil); err != nil {
		t.Fatal(err)
	}
	newData := bytes.Repeat([]byte("x"), 5<<20)
	manifest, total, err := r.StoreChunkedReader(bytes.NewReader(newData))
	if err != nil {
		t.Fatal(err)
	}
	md5sum := md5.Sum(newData)
	if _, err := r.CommitTxn(0, []Change{{
		Path: "/f", Action: ActionModify, Kind: KindFile,
		ChunkManifest: manifest, ChunkTotal: uint64(total),
		DataMD5: md5sum, HasMD5: true,
	}}, nil); err != nil {
		t.Fatal(err)
	}
	nr, err := r.Resolve(2, "/f")
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.GetText(nr.Node)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(got) != sha256.Sum256(newData) {
		t.Fatal("chunk manifest was replaced by the old text")
	}
}
