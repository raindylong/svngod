package fs

import (
	"context"
	"fmt"
	"testing"

	"svngod/internal/cdc"
	"svngod/internal/fs/store"
)

func commitBigDir(t *testing.T, r *Repo, files int) uint64 {
	t.Helper()
	changes := make([]Change, 0, files+1)
	changes = append(changes, Change{Path: "/big", Action: ActionAdd, Kind: KindDir})
	for i := 0; i < files; i++ {
		changes = append(changes, Change{
			Path: fmt.Sprintf("/big/f%04d.txt", i), Action: ActionAdd, Kind: KindFile, Data: []byte("x"),
		})
	}
	rev, err := r.CommitTxn(0, changes, nil)
	if err != nil {
		t.Fatal(err)
	}
	return rev
}

func countDirObjects(t *testing.T, st store.Store, r *Repo) int {
	t.Helper()
	n := 0
	next := ""
	for {
		objs, cursor, err := st.ListPrefix(context.Background(), r.keys.Prefix+"/dirs/", next, 500)
		if err != nil {
			t.Fatal(err)
		}
		n += len(objs)
		if cursor == "" {
			return n
		}
		next = cursor
	}
}

func TestDirBucketsLargeDirectory(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepoFull(st, "db", 0, 0, cdc.DefaultParams(), true)
	if err != nil {
		t.Fatal(err)
	}
	rev := commitBigDir(t, r, 1500)
	nr, err := r.Resolve(rev, "/big")
	if err != nil {
		t.Fatal(err)
	}
	if nr.Kind != KindDir || nr.TextRep != TextRepExternal || len(nr.Children) != 0 {
		t.Fatalf("dir not paged: kind=%d rep=%d children=%d", nr.Kind, nr.TextRep, len(nr.Children))
	}
	kids, err := r.dirChildren(nr)
	if err != nil {
		t.Fatal(err)
	}
	if len(kids) != 1500 {
		t.Fatalf("children=%d", len(kids))
	}
	if _, ok, err := r.dirChild(nr, "f0999.txt"); err != nil || !ok {
		t.Fatalf("bucket lookup: ok=%v err=%v", ok, err)
	}
	if list, err := r.ListDir(rev, "/big"); err != nil || len(list) != 1500 {
		t.Fatalf("ListDir n=%d err=%v", len(list), err)
	}
	fnr, err := r.Resolve(rev, "/big/f0999.txt")
	if err != nil {
		t.Fatal(err)
	}
	if txt, _ := r.GetText(fnr.Node); string(txt) != "x" {
		t.Fatalf("text=%q", txt)
	}
	table1, err := r.dirTableOf(nr)
	if err != nil {
		t.Fatal(err)
	}

	// Modify one child: exactly the bucket containing it is rewritten.
	if _, err := r.CommitTxn(0, []Change{{
		Path: "/big/f0999.txt", Action: ActionModify, Kind: KindFile, Data: []byte("y"),
	}}, nil); err != nil {
		t.Fatal(err)
	}
	nr2, err := r.Resolve(2, "/big")
	if err != nil {
		t.Fatal(err)
	}
	table2, err := r.dirTableOf(nr2)
	if err != nil {
		t.Fatal(err)
	}
	changed := 0
	for i := 0; i < dirBucketCount; i++ {
		if table1.Refs[i] != table2.Refs[i] {
			changed++
		}
	}
	if changed != 1 {
		t.Fatalf("edit rewrote %d buckets, want 1", changed)
	}
	fnr, _ = r.Resolve(2, "/big/f0999.txt")
	if txt, _ := r.GetText(fnr.Node); string(txt) != "y" {
		t.Fatalf("edited text=%q", txt)
	}

	// Adding a child rewrites one bucket and updates the total.
	if _, err := r.CommitTxn(0, []Change{{
		Path: "/big/new.txt", Action: ActionAdd, Kind: KindFile, Data: []byte("n"),
	}}, nil); err != nil {
		t.Fatal(err)
	}
	nr3, _ := r.Resolve(3, "/big")
	if list, err := r.ListDir(3, "/big"); err != nil || len(list) != 1501 {
		t.Fatalf("after add n=%d err=%v", len(list), err)
	}
	if _, ok, _ := r.dirChild(nr3, "new.txt"); !ok {
		t.Fatal("new child not found")
	}

	if issues, err := r.VerifyRange(1, 3, true); err != nil || len(issues) != 0 {
		t.Fatalf("verify: issues=%v err=%v", issues, err)
	}

	// GC keeps referenced directory objects and removes orphans.
	before := countDirObjects(t, st, r)
	if _, err := r.CollectTexts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if after := countDirObjects(t, st, r); after != before {
		t.Fatalf("GC removed referenced dir objects: %d -> %d", before, after)
	}
	orphan := r.keys.Dir("deadbeef")
	if err := st.Put(context.Background(), orphan, []byte("orphan")); err != nil {
		t.Fatal(err)
	}
	if _, err := r.CollectTexts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ok, _ := st.Exists(context.Background(), orphan); ok {
		t.Fatal("orphan dir object not collected")
	}
}

func TestDirBucketsMissingObjectDetected(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepoFull(st, "db2", 0, 0, cdc.DefaultParams(), true)
	if err != nil {
		t.Fatal(err)
	}
	rev := commitBigDir(t, r, 1200)
	nr, _ := r.Resolve(rev, "/big")
	table, err := r.dirTableOf(nr)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < dirBucketCount; i++ {
		if table.Refs[i] != "" {
			if err := st.Delete(context.Background(), []string{r.keys.Dir(table.Refs[i])}); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	issues, err := r.VerifyRange(rev, rev, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) == 0 {
		t.Fatal("missing dir bucket not detected")
	}
}

func TestLargeDirectoryInlineWhenDisabled(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepo(st, "inline", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	rev := commitBigDir(t, r, 1100)
	nr, err := r.Resolve(rev, "/big")
	if err != nil {
		t.Fatal(err)
	}
	if nr.TextRep == TextRepExternal || len(nr.Children) != 1100 {
		t.Fatalf("inline dir: rep=%d children=%d", nr.TextRep, len(nr.Children))
	}
	if list, err := r.ListDir(rev, "/big"); err != nil || len(list) != 1100 {
		t.Fatalf("ListDir n=%d err=%v", len(list), err)
	}
}
