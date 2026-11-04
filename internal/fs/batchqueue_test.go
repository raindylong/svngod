package fs

import (
	"errors"
	"sync"
	"testing"

	"svngod/internal/fs/store"
)

func TestCommitBatchCoordinator(t *testing.T) {
	ConfigureCommitBatch(16, 5)
	defer ConfigureCommitBatch(0, 0)
	st := store.NewMemoryStore()
	if _, err := CreateRepo(st, "coord", 4096, 64); err != nil {
		t.Fatal(err)
	}
	r, err := OpenRepo(st, "coord")
	if err != nil {
		t.Fatal(err)
	}
	const n = 24
	var wg sync.WaitGroup
	revs := make([]uint64, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			revs[i], errs[i] = r.CommitTxn(0, []Change{{
				Path: "/f" + string(rune('a'+i)), Action: ActionAdd, Kind: KindFile,
				Data: []byte("x"),
			}}, nil)
		}(i)
	}
	wg.Wait()
	seen := map[uint64]bool{}
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("commit %d: %v", i, errs[i])
		}
		if revs[i] == 0 || seen[revs[i]] {
			t.Fatalf("duplicate/zero revision %d", revs[i])
		}
		seen[revs[i]] = true
	}
	if h, _ := r.HeadFresh(); h != n {
		t.Fatalf("head=%d, want %d", h, n)
	}
}

func TestCommitBatchCoordinatorSamePath(t *testing.T) {
	ConfigureCommitBatch(16, 10)
	defer ConfigureCommitBatch(0, 0)
	st := store.NewMemoryStore()
	if _, err := CreateRepo(st, "coordsame", 4096, 64); err != nil {
		t.Fatal(err)
	}
	r, err := OpenRepo(st, "coordsame")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = r.CommitTxn(0, []Change{{Path: "/same", Action: ActionAdd, Kind: KindFile,
				Data: []byte("x")}}, nil)
		}(i)
	}
	wg.Wait()
	ok, conflict := 0, 0
	for _, e := range errs {
		switch {
		case e == nil:
			ok++
		case errors.Is(e, ErrOutOfDate):
			conflict++
		default:
			t.Fatalf("unexpected error: %v", e)
		}
	}
	if ok != 1 || conflict != 1 {
		t.Fatalf("ok=%d conflict=%d, want 1/1", ok, conflict)
	}
}

func TestCommitBatchCoordinatorBaseRev(t *testing.T) {
	ConfigureCommitBatch(16, 5)
	defer ConfigureCommitBatch(0, 0)
	st := store.NewMemoryStore()
	if _, err := CreateRepo(st, "coordbase", 4096, 64); err != nil {
		t.Fatal(err)
	}
	r, err := OpenRepo(st, "coordbase")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.CommitTxn(0, []Change{{Path: "/f", Action: ActionAdd, Kind: KindFile, Data: []byte("1")}}, nil); err != nil {
		t.Fatal(err)
	}
	// A modify based on revision 1 is accepted.
	if rev, err := r.CommitTxn(1, []Change{{Path: "/f", Action: ActionModify, Kind: KindFile, Data: []byte("2")}}, nil); err != nil || rev != 2 {
		t.Fatalf("modify rev=%d err=%v", rev, err)
	}
	// The same base is now stale.
	if _, err := r.CommitTxn(1, []Change{{Path: "/f", Action: ActionModify, Kind: KindFile, Data: []byte("3")}}, nil); !errors.Is(err, ErrOutOfDate) {
		t.Fatalf("stale modify err=%v, want out-of-date", err)
	}
}
