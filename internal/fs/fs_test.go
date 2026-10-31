package fs

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"errors"
	"strings"
	"testing"

	"svngod/internal/fs/store"
	"svngod/internal/svndiff"
)

func TestNodeRevRoundTrip(t *testing.T) {
	nr := &NodeRev{
		Kind:    KindFile,
		Node:    NodeID{Rev: 3, ID: 7},
		Pred:    NodeID{Rev: 2, ID: 4},
		Props:   map[string]string{"svn:mime-type": "text/plain", "k": "v"},
		TextRep: TextRepFull,
		Text:    []byte("hello world"),
	}
	got, err := UnmarshalNodeRev(nr.Marshal())
	if err != nil {
		t.Fatal(err)
	}
	if got.Node != nr.Node || got.Kind != nr.Kind || got.Pred != nr.Pred {
		t.Fatalf("mismatch: %+v", got)
	}
	if !bytes.Equal(got.Text, nr.Text) || got.Props["k"] != "v" {
		t.Fatal("text/props mismatch")
	}
	// delta rep with base
	nr2 := &NodeRev{Kind: KindFile, Node: NodeID{Rev: 5, ID: 1}, TextRep: TextRepDelta, Text: []byte{1, 2, 3}, Base: &NodeID{Rev: 4, ID: 9}, DeltaDepth: 3}
	g2, err := UnmarshalNodeRev(nr2.Marshal())
	if err != nil {
		t.Fatal(err)
	}
	if g2.Base == nil || *g2.Base != *nr2.Base || g2.DeltaDepth != 3 {
		t.Fatal("delta round-trip mismatch")
	}
	// dir with children
	dir := &NodeRev{Kind: KindDir, Node: NodeID{Rev: 5, ID: 2},
		Children: []Child{{Name: "a", ID: NodeID{Rev: 4, ID: 1}}, {Name: "b", ID: NodeID{Rev: 3, ID: 2}}}}
	gd, err := UnmarshalNodeRev(dir.Marshal())
	if err != nil {
		t.Fatal(err)
	}
	if len(gd.Children) != 2 || gd.Children[0].Name != "a" || gd.Children[1].ID.ID != 2 {
		t.Fatal("dir round-trip mismatch")
	}
}

func TestRevBlockRoundTrip(t *testing.T) {
	nodes := []*NodeRev{
		{Kind: KindDir, Node: NodeID{Rev: 1, ID: 1}, Children: []Child{{Name: "f", ID: NodeID{Rev: 1, ID: 2}}}},
		{Kind: KindFile, Node: NodeID{Rev: 1, ID: 2}, TextRep: TextRepFull, Text: []byte("data")},
	}
	rb := NewRevBlock(1, 0, NodeID{Rev: 1, ID: 1}, nodes)
	g, err := UnmarshalRevBlock(rb.Marshal())
	if err != nil {
		t.Fatal(err)
	}
	if g.Rev != 1 || g.PrevRev != 0 || g.Root != (NodeID{Rev: 1, ID: 1}) {
		t.Fatalf("block header mismatch: %+v", g)
	}
	n, err := g.GetNode(2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(n.Text, []byte("data")) {
		t.Fatal("node data mismatch")
	}
	if _, err := g.GetNode(99); err == nil {
		t.Fatal("expected not-found error")
	}
}

func TestShardIdxRoundTrip(t *testing.T) {
	idx := NewShardIdx(3, 12000, []revTableEntry{{BlockOffset: 0, BlockLen: 100}, {BlockOffset: 100, BlockLen: 250}})
	g, err := UnmarshalShardIdx(idx.Marshal())
	if err != nil {
		t.Fatal(err)
	}
	if g.Shard != 3 || g.FirstRev != 12000 || g.Count() != 2 {
		t.Fatalf("idx mismatch: %+v", g)
	}
	if off, ln, ok := g.Lookup(12001); !ok || off != 100 || ln != 250 {
		t.Fatalf("lookup mismatch: %d %d %v", off, ln, ok)
	}
	if _, _, ok := g.Lookup(11999); ok {
		t.Fatal("expected miss")
	}
}

func TestCurrentFormatRevPropsRoundTrip(t *testing.T) {
	c := &Current{Head: 42, Created: 123456}
	gc, err := UnmarshalCurrent(c.Marshal())
	if err != nil || gc.Head != 42 {
		t.Fatal("current round-trip failed")
	}
	f := &Format{Version: 1, ShardSize: 4096, MaxDeltaChain: 64}
	gf, err := UnmarshalFormat(f.Marshal())
	if err != nil || gf.ShardSize != 4096 || gf.MaxDeltaChain != 64 {
		t.Fatal("format round-trip failed")
	}
	rp := NewRevProps()
	rp.Props = map[string]string{"svn:log": "msg", "svn:author": "me"}
	raw := rp.Marshal()
	t.Logf("revprops bytes: %x", raw)
	grp, err := UnmarshalRevProps(raw)
	if err != nil {
		t.Fatalf("revprops unmarshal: %v", err)
	}
	t.Logf("revprops got: %v", grp.Props)
	if grp.Props["svn:log"] != "msg" {
		t.Fatalf("revprops round-trip failed: %v", grp.Props)
	}
}

// TestRepoCommitRead exercises the full storage path end to end against an
// in-memory store: create, commit, resolve, text reconstruction, listing.
func TestRepoCommitRead(t *testing.T) {
	st := store.NewMemoryStore()
	repo, err := CreateRepo(st, "repos/test", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	rev1, err := repo.CommitTxn(0, []Change{
		{Path: "/README.txt", Action: ActionAdd, Kind: KindFile, Data: []byte("hello svngod")},
		{Path: "/src/main.go", Action: ActionAdd, Kind: KindFile, Data: []byte("package main")},
	}, map[string]string{"svn:log": "initial"})
	if err != nil {
		t.Fatal(err)
	}
	if rev1 != 1 {
		t.Fatalf("expected rev 1, got %d", rev1)
	}

	readme, err := repo.Resolve(rev1, "/README.txt")
	if err != nil {
		t.Fatal(err)
	}
	if readme.Kind != KindFile {
		t.Fatal("not a file")
	}
	text, err := repo.GetText(readme.Node)
	if err != nil {
		t.Fatal(err)
	}
	if string(text) != "hello svngod" {
		t.Fatalf("text mismatch: %q", text)
	}

	entries, err := repo.ListDir(rev1, "/")
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, e := range entries {
		names[e.Name] = true
	}
	if !names["README.txt"] || !names["src"] {
		t.Fatalf("root listing mismatch: %v", names)
	}

	// Modify README → revision 2 (tests delta encoding + reconstruction).
	rev2, err := repo.CommitTxn(rev1, []Change{
		{Path: "/README.txt", Action: ActionModify, Kind: KindFile, Data: []byte("hello svngod, version 2, with a longer line to force a real delta and exercise the window codec path in svndiff")},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rev2 != 2 {
		t.Fatalf("expected rev 2, got %d", rev2)
	}
	nr2, err := repo.Resolve(rev2, "/README.txt")
	if err != nil {
		t.Fatal(err)
	}
	if nr2.TextRep != TextRepDelta {
		t.Fatal("expected delta representation")
	}
	text2, err := repo.GetText(nr2.Node)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(text2, []byte("version 2")) {
		t.Fatalf("delta reconstruction mismatch: %q", text2)
	}

	// Revision 1 must still yield the old text (immutability).
	old, err := repo.GetText(readme.Node)
	if err != nil || string(old) != "hello svngod" {
		t.Fatalf("old revision mutated: %q err=%v", old, err)
	}
}

// TestRepoShardBoundaryAndPack verifies cross-shard reads and the pack job.
func TestRepoShardBoundaryAndPack(t *testing.T) {
	st := store.NewMemoryStore()
	// tiny shard size to force a boundary quickly
	repo, err := CreateRepo(st, "repos/shard", 3, 64)
	if err != nil {
		t.Fatal(err)
	}
	var last uint64
	for i := 0; i < 8; i++ {
		path := "/f" + string(rune('a'+i))
		rev, err := repo.CommitTxn(last, []Change{{Path: path, Action: ActionAdd, Kind: KindFile, Data: []byte("content " + string(rune('a'+i)))}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		last = rev
	}
	if last != 8 {
		t.Fatalf("expected head 8, got %d", last)
	}
	// every revision readable, crossing shard 0/1/2 boundaries
	for rev := uint64(1); rev <= 8; rev++ {
		nr, err := repo.Resolve(rev, "/")
		if err != nil || nr.Kind != KindDir {
			t.Fatalf("rev %d root: %v", rev, err)
		}
	}
	// revprop of rev 1
	rp, err := repo.GetRevProps(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := rp.Props["svn:author"]; !ok {
		t.Fatal("revprops missing")
	}

	// Pack shard 0 (revisions 0..2) and 1 (3..5); reads must switch to packed.
	if err := repo.PackShard(0); err != nil {
		t.Fatal(err)
	}
	if err := repo.PackShard(1); err != nil {
		t.Fatal(err)
	}
	// After packing, loose objects in those shards are unreachable but reads
	// still succeed via pack + idx.
	for rev := uint64(1); rev <= 5; rev++ {
		nr, err := repo.Resolve(rev, "/")
		if err != nil {
			t.Fatalf("rev %d after pack: %v", rev, err)
		}
		_ = nr
	}
	// text still reconstructs from packed shard 1 (rev 4 file)
	nr4, err := repo.Resolve(4, "/f"+string(rune('d')))
	if err != nil {
		t.Fatal(err)
	}
	txt, err := repo.GetText(nr4.Node)
	if err != nil || string(txt) != "content d" {
		t.Fatalf("packed text mismatch: %q err=%v", txt, err)
	}
	// revprops from packed shard 1
	rp2, err := repo.GetRevProps(4)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := rp2.Props["svn:author"]; !ok {
		t.Fatal("packed revprops missing")
	}
}

// TestMemoryStoreCAS verifies the CAS contract used by commit serialization.
func TestMemoryStoreCAS(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemoryStore()
	if err := st.Put(ctx, "current", []byte("HEAD=1\n")); err != nil {
		t.Fatal(err)
	}
	etag, ok, err := st.Head(ctx, "current")
	if err != nil || !ok {
		t.Fatal("head failed")
	}
	if _, err := st.CAS(ctx, "current", "WRONG", []byte("HEAD=2\n")); err != store.ErrPreconditionFailed {
		t.Fatalf("expected precondition failure, got %v", err)
	}
	newEtag, err := st.CAS(ctx, "current", etag, []byte("HEAD=2\n"))
	if err != nil {
		t.Fatal(err)
	}
	got, _ := st.Get(ctx, "current", nil)
	if string(got) != "HEAD=2\n" || newEtag == etag {
		t.Fatalf("cas failed: %q %q", got, newEtag)
	}
}

func TestCommitCopyFromHistory(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepo(st, "repos/copyhist", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.CommitTxn(0, []Change{
		{Path: "/trunk", Action: ActionAdd, Kind: KindDir},
		{Path: "/trunk/a.txt", Action: ActionAdd, Kind: KindFile, Data: []byte("alpha")},
		{Path: "/trunk/sub", Action: ActionAdd, Kind: KindDir},
		{Path: "/trunk/sub/b.txt", Action: ActionAdd, Kind: KindFile, Data: []byte("beta")},
	}, nil); err != nil {
		t.Fatal(err)
	}
	rev1, _ := r.Head()

	// copy /trunk -> /branches/br from history rev1
	if _, err := r.CommitTxn(rev1, []Change{
		{Path: "/branches", Action: ActionAdd, Kind: KindDir},
		{Path: "/branches/br", Action: ActionCopy, Kind: KindDir, CopyFromPath: "/trunk", CopyFromRev: rev1},
	}, nil); err != nil {
		t.Fatal(err)
	}
	head, _ := r.Head()

	top, err := r.Resolve(head, "/branches/br")
	if err != nil {
		t.Fatal(err)
	}
	if top.CopyFrom == nil || top.CopyFrom.Rev != rev1 || top.CopyFromPath != "/trunk" {
		t.Fatalf("copyfrom wrong: %+v %q", top.CopyFrom, top.CopyFromPath)
	}
	// children materialized with content
	for _, tc := range []struct{ p, want string }{
		{"/branches/br/a.txt", "alpha"},
		{"/branches/br/sub/b.txt", "beta"},
	} {
		nr, err := r.Resolve(head, tc.p)
		if err != nil {
			t.Fatalf("missing %s: %v", tc.p, err)
		}
		txt, _ := r.GetText(nr.Node)
		if string(txt) != tc.want {
			t.Fatalf("%s=%q want %q", tc.p, txt, tc.want)
		}
	}
}

// TestCommitStaleBaseRev: a change coded against an older per-path base
// revision is rejected and names the path, instead of merging into newer text.
func TestCommitStaleBaseRev(t *testing.T) {
	st := store.NewMemoryStore()
	repo, err := CreateRepo(st, "r", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	r1, err := repo.CommitTxn(0, []Change{
		{Path: "/f.txt", Action: ActionAdd, Kind: KindFile, Data: []byte("v1")},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CommitTxn(r1, []Change{
		{Path: "/f.txt", Action: ActionModify, Kind: KindFile, Data: []byte("v2"), BaseRev: r1},
	}, nil); err != nil {
		t.Fatal(err)
	}
	// Client still believes it edits r1.
	_, err = repo.CommitTxn(2, []Change{
		{Path: "/f.txt", Action: ActionModify, Kind: KindFile, Data: []byte("stale"), BaseRev: r1},
	}, nil)
	var ood *OutOfDateError
	if !errors.As(err, &ood) {
		t.Fatalf("err = %v, want OutOfDateError", err)
	}
	if ood.Path != "/f.txt" {
		t.Fatalf("path = %q", ood.Path)
	}
	if nr, _ := repo.Resolve(2, "/f.txt"); nr == nil {
		t.Fatal("missing node")
	} else if txt, _ := repo.GetText(nr.Node); string(txt) != "v2" {
		t.Fatalf("content changed to %q, want v2", txt)
	}
}

// TestCollectLooseRemovesPackedShards: auto-packing writes the pack but used
// to leave the loose objects behind forever; CollectLoose must remove only
// objects of packed shards and reads must keep working from the pack.
func TestCollectLooseRemovesPackedShards(t *testing.T) {
	st := store.NewMemoryStore()
	repo, err := CreateRepo(st, "r", 4, 64)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		if _, err := repo.CommitTxn(uint64(i-1), []Change{
			{Path: "/f" + string(rune('0'+i)) + ".txt", Action: ActionAdd, Kind: KindFile, Data: []byte("x")},
		}, nil); err != nil {
			t.Fatal(err)
		}
	}
	// shard 0 (revs 0..3) is complete; pack it.
	if err := repo.PackShard(0); err != nil {
		t.Fatal(err)
	}
	n, err := repo.CollectLoose(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("no loose objects collected")
	}
	// Packed shard reads still work; the active shard keeps its loose objects.
	if nr, err := repo.Resolve(2, "/f2.txt"); err != nil {
		t.Fatalf("resolve packed: %v", err)
	} else if txt, _ := repo.GetText(nr.Node); string(txt) != "x" {
		t.Fatalf("packed text = %q", txt)
	}
	if nr, err := repo.Resolve(4, "/f4.txt"); err != nil {
		t.Fatalf("resolve loose: %v", err)
	} else if txt, _ := repo.GetText(nr.Node); string(txt) != "x" {
		t.Fatalf("loose text = %q", txt)
	}
}

// TestCollectTxnData: content-addressed commit bodies referenced by an activity
// manifest survive; orphaned ones (aborted commits) are removed.
func TestCollectTxnData(t *testing.T) {
	st := store.NewMemoryStore()
	repo, err := CreateRepo(st, "r", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	base := repo.Keys().Prefix + "/dav-txns/"
	ctx := context.Background()
	keep := strings.Repeat("a", 40)
	drop := strings.Repeat("b", 40)
	if err := st.Put(ctx, base+"data/"+keep, []byte("live")); err != nil {
		t.Fatal(err)
	}
	if err := st.Put(ctx, base+"data/"+drop, []byte("orphan")); err != nil {
		t.Fatal(err)
	}
	manifest := `{"id":"u-1","txn":{"id":"txn-u-1","changes":[{"path":"/a","dataRef":"` + keep + `"}]}}`
	if err := st.Put(ctx, base+"activity-u-1.json", []byte(manifest)); err != nil {
		t.Fatal(err)
	}
	n, err := repo.CollectTxnData(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("removed %d, want 1", n)
	}
	if _, ok, _ := st.Head(ctx, base+"data/"+keep); !ok {
		t.Fatal("referenced body was deleted")
	}
	if _, ok, _ := st.Head(ctx, base+"data/"+drop); ok {
		t.Fatal("orphan body was kept")
	}
}

// TestLargeTextReadableWithoutCache: texts above the cache bound are simply
// not retained; reads must still be correct and repeatable.
func TestLargeTextReadableWithoutCache(t *testing.T) {
	st := store.NewMemoryStore()
	repo, err := CreateRepo(st, "r", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("0123456789abcdef"), (5<<20)/16) // 5 MiB
	if _, err := repo.CommitTxn(0, []Change{
		{Path: "/big.bin", Action: ActionAdd, Kind: KindFile, Data: data},
	}, nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		nr, err := repo.Resolve(1, "/big.bin")
		if err != nil {
			t.Fatal(err)
		}
		got, err := repo.GetText(nr.Node)
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("read %d: content mismatch (%d bytes)", i, len(got))
		}
	}
}

// TestExternalTextObjects: full texts above the threshold live in a separate
// content-addressed object, so rev blocks stay small and reads can stream.
func TestExternalTextObjects(t *testing.T) {
	st := store.NewMemoryStore()
	repo, err := CreateRepo(st, "r", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("0123456789abcdef"), (2<<20)/16) // 2 MiB
	if _, err := repo.CommitTxn(0, []Change{
		{Path: "/big.bin", Action: ActionAdd, Kind: KindFile, Data: data},
	}, nil); err != nil {
		t.Fatal(err)
	}
	nr, err := repo.Resolve(1, "/big.bin")
	if err != nil {
		t.Fatal(err)
	}
	if nr.TextRep != TextRepExternal {
		t.Fatalf("TextRep = %d, want external", nr.TextRep)
	}
	if len(nr.Text) != 0 {
		t.Fatalf("external node still carries %d inline bytes", len(nr.Text))
	}
	if nr.TextSize != uint64(len(data)) || nr.TextRef == "" {
		t.Fatalf("text metadata = %q/%d", nr.TextRef, nr.TextSize)
	}
	// The rev block itself must not contain the payload.
	rb, err := repo.readBlock(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(rb.Marshal()) > 1<<20 {
		t.Fatalf("rev block is %d bytes, payload should be external", len(rb.Marshal()))
	}
	if got, err := repo.GetText(nr.Node); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("GetText: len=%d err=%v", len(got), err)
	}
	var buf bytes.Buffer
	if n, err := repo.StreamText(nr.Node, &buf); err != nil || n != uint64(len(data)) || !bytes.Equal(buf.Bytes(), data) {
		t.Fatalf("StreamText: n=%d err=%v", n, err)
	}
	part, err := repo.ReadTextRange(nr, 1000, 64)
	if err != nil || !bytes.Equal(part, data[1000:1064]) {
		t.Fatalf("ReadTextRange: %q err=%v", part, err)
	}
	// Reopening must resolve the external object too.
	repo2, err := OpenRepo(st, "r")
	if err != nil {
		t.Fatal(err)
	}
	nr2, err := repo2.Resolve(1, "/big.bin")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := repo2.GetText(nr2.Node); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("reopened GetText: len=%d err=%v", len(got), err)
	}
}

// TestSmallTextStaysInline keeps the common case (and delta chains) unchanged.
func TestSmallTextStaysInline(t *testing.T) {
	st := store.NewMemoryStore()
	repo, _ := CreateRepo(st, "r", 4096, 64)
	data := bytes.Repeat([]byte("x"), 1024)
	if _, err := repo.CommitTxn(0, []Change{
		{Path: "/small.txt", Action: ActionAdd, Kind: KindFile, Data: data},
	}, nil); err != nil {
		t.Fatal(err)
	}
	nr, err := repo.Resolve(1, "/small.txt")
	if err != nil {
		t.Fatal(err)
	}
	if nr.TextRep != TextRepFull || len(nr.Text) != len(data) {
		t.Fatalf("small text: rep=%d len=%d", nr.TextRep, len(nr.Text))
	}
}

// TestStreamDeltaAndTotalSize: a delta node's total size is stored, so
// Content-Length is O(1) and StreamText decodes window-by-window.
func TestStreamDeltaAndTotalSize(t *testing.T) {
	st := store.NewMemoryStore()
	repo, _ := CreateRepo(st, "r", 4096, 64)
	base := bytes.Repeat([]byte("abcdefgh"), (2<<20)/8) // 2 MiB, external on r1
	r1, err := repo.CommitTxn(0, []Change{
		{Path: "/f.bin", Action: ActionAdd, Kind: KindFile, Data: base},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	mod := append([]byte(nil), base...)
	copy(mod[5000:5010], []byte("0123456789"))
	r2, err := repo.CommitTxn(r1, []Change{
		{Path: "/f.bin", Action: ActionModify, Kind: KindFile, Data: mod},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	nr, err := repo.Resolve(r2, "/f.bin")
	if err != nil {
		t.Fatal(err)
	}
	if nr.TextRep != TextRepDelta {
		t.Fatalf("rep = %d, want delta", nr.TextRep)
	}
	if nr.TotalSize != uint64(len(mod)) {
		t.Fatalf("TotalSize = %d, want %d", nr.TotalSize, len(mod))
	}
	if size, err := repo.TextSizeOf(nr); err != nil || size != int64(len(mod)) {
		t.Fatalf("TextSizeOf = %d err=%v", size, err)
	}
	var buf bytes.Buffer
	if n, err := repo.StreamText(nr.Node, &buf); err != nil || n != uint64(len(mod)) || !bytes.Equal(buf.Bytes(), mod) {
		t.Fatalf("StreamText: n=%d err=%v", n, err)
	}
}

// TestCollectTexts keeps texts referenced by any revision and removes orphans.
func TestCollectTexts(t *testing.T) {
	st := store.NewMemoryStore()
	repo, _ := CreateRepo(st, "r", 4096, 64)
	a := bytes.Repeat([]byte("a"), (1<<20)+1)
	b := bytes.Repeat([]byte("b"), (1<<20)+1)
	if _, err := repo.CommitTxn(0, []Change{
		{Path: "/a.bin", Action: ActionAdd, Kind: KindFile, Data: a},
	}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CommitTxn(1, []Change{
		{Path: "/b.bin", Action: ActionAdd, Kind: KindFile, Data: b},
	}, nil); err != nil {
		t.Fatal(err)
	}
	orphan := strings.Repeat("f", 40)
	if err := st.Put(context.Background(), repo.Keys().Text(orphan), []byte("orphan")); err != nil {
		t.Fatal(err)
	}
	n, err := repo.CollectTexts(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("CollectTexts = %d, %v; want 1", n, err)
	}
	if _, ok, _ := st.Head(context.Background(), repo.Keys().Text(orphan)); ok {
		t.Fatal("orphan text kept")
	}
	for rev, want := range map[uint64][]byte{1: a, 2: b} {
		nr, err := repo.Resolve(rev, map[uint64]string{1: "/a.bin", 2: "/b.bin"}[rev])
		if err != nil {
			t.Fatal(err)
		}
		if got, err := repo.GetText(nr.Node); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("rev %d text wrong: len=%d err=%v", rev, len(got), err)
		}
	}
}

// TestCommitClientDelta stores a change as a client-supplied svndiff stream
// against an exact base node (large-binary delta storage) and checks the
// reconstructed text, the recorded hashes and the stale-base rejection.
func TestCommitClientDelta(t *testing.T) {
	st := store.NewMemoryStore()
	repo, err := CreateRepo(st, "repos/delta", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	base := bytes.Repeat([]byte("0123456789abcdef"), 4096) // 64 KiB
	rev1, err := repo.CommitTxn(0, []Change{{Path: "/big.bin", Action: ActionAdd, Kind: KindFile, Data: base}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	n1, err := repo.Resolve(rev1, "/big.bin")
	if err != nil {
		t.Fatal(err)
	}
	next := append([]byte(nil), base...)
	copy(next[100:], "CHANGED!")
	delta, err := svndiff.Encode(base, next)
	if err != nil {
		t.Fatal(err)
	}
	if len(delta) > len(next)/2 {
		t.Fatalf("delta too large for the test: %d", len(delta))
	}
	sum := md5.Sum(next)
	sha := sha1.Sum(next)
	rev2, err := repo.CommitTxn(rev1, []Change{{
		Path: "/big.bin", Action: ActionModify, Kind: KindFile,
		BaseRev: rev1, Delta: delta, DeltaBase: n1.Node,
		TextTotal: uint64(len(next)), DataMD5: sum, HasMD5: true,
		DataSHA1: sha, HasSHA1: true,
	}}, nil)
	if err != nil {
		t.Fatalf("commit delta: %v", err)
	}
	nr, err := repo.Resolve(rev2, "/big.bin")
	if err != nil {
		t.Fatal(err)
	}
	if nr.TextRep != TextRepDelta || nr.Base == nil || *nr.Base != n1.Node {
		t.Fatalf("node rep=%d base=%v, want inline delta on %v", nr.TextRep, nr.Base, n1.Node)
	}
	if nr.TotalSize != uint64(len(next)) || nr.TextSHA1 != sha || nr.TextMD5 != sum {
		t.Fatalf("metadata mismatch: size=%d sha=%x", nr.TotalSize, nr.TextSHA1)
	}
	got, err := repo.GetText(nr.Node)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, next) {
		t.Fatal("reconstructed text mismatch")
	}

	// A delta may also be taken against an older full-text ancestor rather
	// than the immediate parent (that is how large-text edits keep chains at
	// depth one); such a change is valid and must reconstruct correctly.
	third := append([]byte(nil), next...)
	copy(third[200:], "THIRD!")
	delta3, err := svndiff.Encode(base, third)
	if err != nil {
		t.Fatal(err)
	}
	sum3 := md5.Sum(third)
	sha3 := sha1.Sum(third)
	rev3, err := repo.CommitTxn(rev2, []Change{{
		Path: "/big.bin", Action: ActionModify, Kind: KindFile,
		BaseRev: rev2, Delta: delta3, DeltaBase: n1.Node,
		TextTotal: uint64(len(third)), DataMD5: sum3, HasMD5: true,
		DataSHA1: sha3, HasSHA1: true,
	}}, nil)
	if err != nil {
		t.Fatalf("commit delta against ancestor: %v", err)
	}
	nr3, err := repo.Resolve(rev3, "/big.bin")
	if err != nil {
		t.Fatal(err)
	}
	if nr3.Base == nil || *nr3.Base != n1.Node || nr3.DeltaDepth != 1 {
		t.Fatalf("ancestor-based delta: base=%v depth=%d", nr3.Base, nr3.DeltaDepth)
	}
	if got, _ := repo.GetText(nr3.Node); !bytes.Equal(got, third) {
		t.Fatal("ancestor-based delta content mismatch")
	}

	// A delta built against a node that is no longer the parent must be
	// refused, otherwise the new node would decode to the wrong content.
	_, err = repo.CommitTxn(rev2, []Change{{
		Path: "/big.bin", Action: ActionModify, Kind: KindFile,
		BaseRev: rev1, Delta: delta, DeltaBase: n1.Node,
		TextTotal: uint64(len(next)),
	}}, nil)
	if !errors.Is(err, ErrOutOfDate) {
		t.Fatalf("stale delta base: err=%v, want out of date", err)
	}
}

func TestVerifyRangeCoversHistory(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepo(st, "vr", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	big := make([]byte, 300<<10)
	for i := range big {
		big[i] = byte(i)
	}
	if _, err := r.CommitTxn(0, []Change{{Path: "/big.bin", Action: ActionAdd, Kind: KindFile, Data: big}}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := r.CommitTxn(0, []Change{
		{Path: "/big.bin", Action: ActionModify, Kind: KindFile, Data: append(big, 'x')},
		{Path: "/small.txt", Action: ActionAdd, Kind: KindFile, Data: []byte("s")},
	}, nil); err != nil {
		t.Fatal(err)
	}
	issues, err := r.VerifyRange(1, 2, true)
	if err != nil || len(issues) != 0 {
		t.Fatalf("clean verify: err=%v issues=%v", err, issues)
	}
	// Remove the external text object of the historical revision; both the
	// existence check and the text-hash mode must report it.
	nr, err := r.Resolve(1, "/big.bin")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Delete(context.Background(), []string{r.keys.Text(nr.TextRef)}); err != nil {
		t.Fatal(err)
	}
	for _, texts := range []bool{false, true} {
		issues, err = r.VerifyRange(1, 2, texts)
		if err != nil {
			t.Fatal(err)
		}
		if len(issues) == 0 {
			t.Fatalf("texts=%v: missing text object not detected", texts)
		}
	}
}

func TestNoopChangeEnforcesOutOfDate(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepo(st, "noop", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.CommitTxn(0, []Change{
		{Path: "/a", Action: ActionAdd, Kind: KindFile, Data: []byte("a1")},
		{Path: "/b", Action: ActionAdd, Kind: KindFile, Data: []byte("b1")},
	}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := r.CommitTxn(0, []Change{
		{Path: "/a", Action: ActionModify, Kind: KindFile, Data: []byte("a2")},
	}, nil); err != nil {
		t.Fatal(err)
	}
	// A check with a stale base must reject the whole commit.
	_, err = r.CommitTxn(0, []Change{
		{Path: "/b", Action: ActionModify, Kind: KindFile, Data: []byte("b2")},
		{Path: "/a", Action: ActionModify, Kind: KindFile, Noop: true, BaseRev: 1},
	}, nil)
	var ood *OutOfDateError
	if !errors.As(err, &ood) {
		t.Fatalf("stale check err=%v, want OutOfDate", err)
	}
	// With the current base it succeeds, changes nothing for /a, and still
	// applies the real change.
	rev, err := r.CommitTxn(0, []Change{
		{Path: "/b", Action: ActionModify, Kind: KindFile, Data: []byte("b2")},
		{Path: "/a", Action: ActionModify, Kind: KindFile, Noop: true, BaseRev: 2},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rev != 3 {
		t.Fatalf("rev=%d, want 3", rev)
	}
	nr, err := r.Resolve(3, "/a")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := r.GetText(nr.Node)
	if string(got) != "a2" {
		t.Fatalf("/a changed to %q", got)
	}
}
