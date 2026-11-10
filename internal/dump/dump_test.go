package dump

import (
	"bytes"
	"context"
	"testing"

	"svngod/internal/cdc"
	"svngod/internal/fs"
	"svngod/internal/fs/store"
)

func buildSourceRepo(t *testing.T) *fs.Repo {
	t.Helper()
	st := store.NewMemoryStore()
	r, err := fs.CreateRepo(st, "repos/src", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	commits := []struct {
		changes []fs.Change
		props   map[string]string
	}{
		{ // rev1
			[]fs.Change{
				{Path: "/trunk", Action: fs.ActionAdd, Kind: fs.KindDir},
				{Path: "/trunk/a.txt", Action: fs.ActionAdd, Kind: fs.KindFile, Data: []byte("hello svn\n")},
				{Path: "/trunk/sub", Action: fs.ActionAdd, Kind: fs.KindDir},
				{Path: "/trunk/sub/b.bin", Action: fs.ActionAdd, Kind: fs.KindFile, Data: []byte{0x00, 0x01, 0x02, 0xff, 0xfe}},
			},
			map[string]string{"svn:log": "initial", "svn:author": "alice"},
		},
		{ // rev2: modify + set dir prop
			[]fs.Change{
				{Path: "/trunk/a.txt", Action: fs.ActionModify, Kind: fs.KindFile, Data: []byte("hello svn, v2\nwith more content\n")},
				{Path: "/trunk", Action: fs.ActionModify, Kind: fs.KindDir, Props: map[string]string{"svn:ignore": "*.o"}},
			},
			map[string]string{"svn:log": "edit", "svn:author": "bob"},
		},
		{ // rev3: add + delete
			[]fs.Change{
				{Path: "/trunk/c.txt", Action: fs.ActionAdd, Kind: fs.KindFile, Data: []byte("c file")},
				{Path: "/trunk/sub/b.bin", Action: fs.ActionDelete, Kind: fs.KindFile},
			},
			map[string]string{"svn:log": "add/delete"},
		},
		{ // rev4: branch copy
			[]fs.Change{
				{Path: "/branches", Action: fs.ActionAdd, Kind: fs.KindDir},
				{Path: "/branches/br1", Action: fs.ActionCopy, Kind: fs.KindDir, CopyFromPath: "/trunk", CopyFromRev: 3},
			},
			map[string]string{"svn:log": "branch"},
		},
	}
	var last uint64
	for _, c := range commits {
		rev, err := r.CommitTxn(last, c.changes, c.props)
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
		last = rev
	}
	return r
}

func assertSameTree(t *testing.T, want, got *fs.Repo) {
	t.Helper()
	hw, _ := want.Head()
	hg, _ := got.Head()
	if hw != hg {
		t.Fatalf("head want=%d got=%d", hw, hg)
	}
	for rev := uint64(1); rev <= hw; rev++ {
		// revprops
		rw, _ := want.GetRevProps(rev)
		rg, _ := got.GetRevProps(rev)
		if len(rw.Props) != len(rg.Props) {
			t.Fatalf("rev%d props len want=%d got=%d", rev, len(rw.Props), len(rg.Props))
		}
		for k, v := range rw.Props {
			if rg.Props[k] != v {
				t.Fatalf("rev%d prop %s want=%q got=%q", rev, k, v, rg.Props[k])
			}
		}
		// tree
		wantPaths := collectPaths(t, want, rev)
		gotPaths := collectPaths(t, got, rev)
		if len(wantPaths) != len(gotPaths) {
			t.Fatalf("rev%d path count want=%d got=%d\nwant=%v\ngot=%v", rev, len(wantPaths), len(gotPaths), wantPaths, gotPaths)
		}
		for p, wk := range wantPaths {
			gk, ok := gotPaths[p]
			if !ok {
				t.Fatalf("rev%d path %s missing in loaded", rev, p)
			}
			if wk != gk {
				t.Fatalf("rev%d path %s kind want=%d got=%d", rev, p, wk, gk)
			}
			if wk == fs.KindFile {
				want, err := want.GetText(mustResolve(t, want, rev, p).Node)
				if err != nil {
					t.Fatal(err)
				}
				gotten, err := got.GetText(mustResolve(t, got, rev, p).Node)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(want, gotten) {
					t.Fatalf("rev%d %s content mismatch", rev, p)
				}
			}
			// copyfrom on the top copy node
			wn, _ := want.Resolve(rev, p)
			gn, _ := got.Resolve(rev, p)
			if (wn.CopyFrom == nil) != (gn.CopyFrom == nil) {
				t.Fatalf("rev%d %s copyfrom presence mismatch", rev, p)
			}
		}
	}
}

func collectPaths(t *testing.T, r *fs.Repo, rev uint64) map[string]byte {
	t.Helper()
	out := map[string]byte{}
	var walk func(path string)
	walk = func(path string) {
		nr, err := r.Resolve(rev, path)
		if err != nil {
			t.Fatalf("resolve %s @%d: %v", path, rev, err)
		}
		out[path] = nr.Kind
		if nr.Kind == fs.KindDir {
			children, _ := r.ListDir(rev, path)
			for _, c := range children {
				walk(path + "/" + c.Name)
			}
		}
	}
	walk("/")
	return out
}

func mustResolve(t *testing.T, r *fs.Repo, rev uint64, path string) *fs.NodeRev {
	t.Helper()
	nr, err := r.Resolve(rev, path)
	if err != nil {
		t.Fatal(err)
	}
	return nr
}

func TestDumpLoadRoundTrip(t *testing.T) {
	src := buildSourceRepo(t)

	var buf bytes.Buffer
	if err := Dump(src, &buf); err != nil {
		t.Fatal(err)
	}
	if buf.Len() == 0 {
		t.Fatal("dump empty")
	}

	st := store.NewMemoryStore()
	dst, err := fs.CreateRepo(st, "repos/dst", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	if err := Load(dst, bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("load: %v\ndump:\n%s", err, buf.String())
	}

	assertSameTree(t, src, dst)
}

// TestDumpLoadRoundTripSegmentsWithOverrides checks a format-v2 repository
// whose revision properties were edited (stored as overrides) dumps and loads
// faithfully into another v2 repository, and that the loaded repository carries
// the values in its segments rather than in overrides.
func TestDumpLoadRoundTripSegmentsWithOverrides(t *testing.T) {
	base := store.NewMemoryStore()
	src, err := fs.CreateRepoSegments(base, "repos/v2src", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	commit := func(base uint64, changes ...fs.Change) uint64 {
		t.Helper()
		revs, errs, cerr := src.CommitBatch([]fs.BatchCommit{{BaseRev: base, Changes: changes}})
		if cerr != nil || errs[0] != nil {
			t.Fatalf("commit: %v %v", cerr, errs)
		}
		return revs[0]
	}
	r1 := commit(0,
		fs.Change{Path: "/trunk", Action: fs.ActionAdd, Kind: fs.KindDir},
		fs.Change{Path: "/trunk/a.txt", Action: fs.ActionAdd, Kind: fs.KindFile, Data: []byte("a")})
	r2 := commit(r1,
		fs.Change{Path: "/trunk/b.txt", Action: fs.ActionAdd, Kind: fs.KindFile, Data: []byte("b")})
	commit(r2,
		fs.Change{Path: "/trunk/a.txt", Action: fs.ActionModify, Kind: fs.KindFile, Data: []byte("a2")})
	// Two edits: the second must merge into the first override.
	if err := src.UpdateRevProps(2, map[string]string{"svn:log": "edited log", "x:custom": "v1"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := src.UpdateRevProps(2, map[string]string{"x:custom": "v2"}, nil); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := Dump(src, &buf); err != nil {
		t.Fatal(err)
	}
	dst, err := fs.CreateRepoSegments(store.NewMemoryStore(), "repos/v2dst", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	if err := Load(dst, bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("load: %v", err)
	}
	assertSameTree(t, src, dst)
	rp, err := dst.GetRevProps(2)
	if err != nil {
		t.Fatal(err)
	}
	if rp.Props["svn:log"] != "edited log" || rp.Props["x:custom"] != "v2" {
		t.Fatalf("loaded props=%+v", rp.Props)
	}
	if !dst.SegmentsEnabled() {
		t.Fatal("destination is not a segment repository")
	}
	// The loaded revision properties are committed values; no override is
	// needed to serve them.
	if ok, _ := dst.Store().Exists(context.Background(), dst.Keys().SegRevPropsIndex(0)); ok {
		t.Fatal("loaded repository stores properties as an override")
	}
}
