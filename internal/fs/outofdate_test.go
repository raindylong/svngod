package fs

import (
	"errors"
	"testing"

	"svngod/internal/cdc"
	"svngod/internal/fs/store"
)

func segRepo(t *testing.T, name string) *Repo {
	t.Helper()
	r, err := CreateRepoSegments(store.NewMemoryStore(), name, 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func mustCommit(t *testing.T, r *Repo, base uint64, changes ...Change) uint64 {
	t.Helper()
	revs, errs, err := r.CommitBatch([]BatchCommit{{BaseRev: base, Changes: changes}})
	if err != nil {
		t.Fatal(err)
	}
	if errs[0] != nil {
		t.Fatal(errs[0])
	}
	return revs[0]
}

// TestStaleDirectoryDeleteRejected: deleting a directory that another writer
// added content to must conflict, not silently discard that content.
func TestStaleDirectoryDeleteRejected(t *testing.T) {
	r := segRepo(t, "repos/ooddir")
	mustCommit(t, r, 0,
		Change{Path: "/d", Action: ActionAdd, Kind: KindDir},
		Change{Path: "/d/f", Action: ActionAdd, Kind: KindFile, Data: []byte("f")})
	// Another writer adds a file under /d at revision 2.
	mustCommit(t, r, 1, Change{Path: "/d/new", Action: ActionAdd, Kind: KindFile, Data: []byte("new")})
	// A client still at revision 1 deletes /d.
	_, errs, err := r.CommitBatch([]BatchCommit{{BaseRev: 1, Changes: []Change{
		{Path: "/d", Action: ActionDelete, Kind: KindDir},
	}}})
	if err == nil && errs[0] == nil {
		t.Fatal("stale directory delete was accepted")
	}
	var ood *OutOfDateError
	if !errors.As(errs[0], &ood) && !errors.As(err, &ood) {
		t.Fatalf("err=%v errs=%v, want OutOfDateError", err, errs)
	}
	// The other writer's file must still exist.
	if _, err := r.Resolve(2, "/d/new"); err != nil {
		t.Fatalf("concurrent file lost: %v", err)
	}
}

// TestCurrentDirectoryDeleteAllowed: deleting at the current base is fine.
func TestCurrentDirectoryDeleteAllowed(t *testing.T) {
	r := segRepo(t, "repos/oodok")
	mustCommit(t, r, 0,
		Change{Path: "/d", Action: ActionAdd, Kind: KindDir},
		Change{Path: "/d/f", Action: ActionAdd, Kind: KindFile, Data: []byte("f")})
	mustCommit(t, r, 1, Change{Path: "/d/new", Action: ActionAdd, Kind: KindFile, Data: []byte("new")})
	rev := mustCommit(t, r, 2, Change{Path: "/d", Action: ActionDelete, Kind: KindDir})
	if rev != 3 {
		t.Fatalf("rev=%d, want 3", rev)
	}
	if _, err := r.Resolve(3, "/d"); err == nil {
		t.Fatal("directory still present after delete")
	}
}

// TestDirectoryPropChangeNotConflicted: a property-only change on a directory
// whose children moved must not be treated as out of date.
func TestDirectoryPropChangeNotConflicted(t *testing.T) {
	r := segRepo(t, "repos/oodprop")
	mustCommit(t, r, 0,
		Change{Path: "/d", Action: ActionAdd, Kind: KindDir},
		Change{Path: "/d/f", Action: ActionAdd, Kind: KindFile, Data: []byte("f")})
	mustCommit(t, r, 1, Change{Path: "/d/new", Action: ActionAdd, Kind: KindFile, Data: []byte("new")})
	rev := mustCommit(t, r, 1, Change{Path: "/d", Action: ActionModify, Kind: KindDir,
		Props: map[string]string{"svn:ignore": "*.tmp"}})
	if rev != 3 {
		t.Fatalf("rev=%d, want 3", rev)
	}
}
