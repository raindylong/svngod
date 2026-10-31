package fs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"svngod/internal/fs/store"
)

func TestCommitLeaseSerialisesReplicas(t *testing.T) {
	st := store.NewMemoryStore()
	if _, err := CreateRepo(st, "lease", 4096, 64); err != nil {
		t.Fatal(err)
	}
	a, _ := OpenRepo(st, "lease")
	b, _ := OpenRepo(st, "lease")
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, r := range []*Repo{a, b} {
		wg.Add(1)
		go func(i int, r *Repo) {
			defer wg.Done()
			_, errs[i] = r.CommitTxn(0, []Change{{
				Path: "/f" + string(rune('a'+i)) + ".txt", Action: ActionAdd, Kind: KindFile, Data: []byte("x"),
			}}, nil)
		}(i, r)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("replica %d commit: %v", i, err)
		}
	}
	if h, _ := a.HeadFresh(); h != 2 {
		t.Fatalf("head=%d, want 2", h)
	}
}

func TestCommitLeaseStaleIsBroken(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepo(st, "stale", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a crashed holder: an old lease object.
	old := time.Now().Add(-3 * commitLeaseTTL)
	if err := st.Put(context.Background(), r.keys.CommitLock(), []byte("crashed  "+time.Now().Add(-3*commitLeaseTTL).Format(time.RFC3339))); err != nil {
		t.Fatal(err)
	}
	_ = old
	done := make(chan error, 1)
	go func() {
		_, err := r.CommitTxn(0, []Change{{Path: "/a", Action: ActionAdd, Kind: KindFile, Data: []byte("x")}}, nil)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("commit after stale lease: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("commit blocked by a stale lease")
	}
}

func TestCommitLeaseBusy(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepo(st, "busy", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreateIfAbsent(context.Background(), r.keys.CommitLock(), []byte(fmt.Sprintf("%d other", time.Now().UnixNano()))); err != nil {
		t.Fatal(err)
	}
	oldWait := commitLeaseWait
	commitLeaseWait = 200 * time.Millisecond
	defer func() { commitLeaseWait = oldWait }()
	_, err = r.CommitTxn(0, []Change{{Path: "/a", Action: ActionAdd, Kind: KindFile, Data: []byte("x")}}, nil)
	if !errors.Is(err, ErrCommitBusy) {
		t.Fatalf("err=%v, want ErrCommitBusy", err)
	}
}

func TestCommitLeaseFileStore(t *testing.T) {
	st, err := store.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateRepo(st, "fl", 4096, 64); err != nil {
		t.Fatal(err)
	}
	a, _ := OpenRepo(st, "fl")
	b, _ := OpenRepo(st, "fl")
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, r := range []*Repo{a, b} {
		wg.Add(1)
		go func(i int, r *Repo) {
			defer wg.Done()
			_, errs[i] = r.CommitTxn(0, []Change{{
				Path: "/g" + string(rune('a'+i)) + ".txt", Action: ActionAdd, Kind: KindFile, Data: []byte("x"),
			}}, nil)
		}(i, r)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("replica %d: %v", i, err)
		}
	}
	if h, _ := a.HeadFresh(); h != 2 {
		t.Fatalf("head=%d", h)
	}
}
