package fs

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
	if err := st.CreateIfAbsent(context.Background(), r.keys.CommitLock(), []byte(fmt.Sprintf("other %d", time.Now().UnixNano()))); err != nil {
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

func TestLeaseReleaseDoesNotDeleteNewHolder(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemoryStore()
	key := "locks/commit"
	if err := st.Put(ctx, key, []byte("old-token 1")); err != nil {
		t.Fatal(err)
	}
	// The old lease is broken and another replica acquires it.
	if err := st.Delete(ctx, []string{key}); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateIfAbsent(ctx, key, []byte("new-token 2")); err != nil {
		t.Fatal(err)
	}
	// The stale holder wakes up and releases; it must not remove the new lock.
	releaseCommitLease(st, key, "old-token")
	cur, err := st.Get(ctx, key, nil)
	if err != nil {
		t.Fatalf("new lease disappeared: %v", err)
	}
	if !strings.HasPrefix(string(cur), "new-token ") {
		t.Fatalf("lease=%q", cur)
	}
}

func TestLeaseRenewalKeepsLockLive(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepo(st, "renew", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	oldTTL, oldWait, oldPoll := commitLeaseTTL, commitLeaseWait, commitLeasePoll
	commitLeaseTTL, commitLeaseWait, commitLeasePoll = 150*time.Millisecond, 300*time.Millisecond, 10*time.Millisecond
	defer func() { commitLeaseTTL, commitLeaseWait, commitLeasePoll = oldTTL, oldWait, oldPoll }()

	l, err := r.acquireCommitLease(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Longer than the TTL: the heartbeat must keep it from being considered
	// stale, and breakCommitLease must refuse to evict it.
	time.Sleep(400 * time.Millisecond)
	r.breakCommitLease(context.Background(), l.key, "other")
	if err := l.Verify(context.Background(), st); err != nil {
		t.Fatalf("lease lost despite renewal: %v", err)
	}
	if err := st.CreateIfAbsent(context.Background(), l.key, []byte("intruder 9")); err == nil {
		t.Fatal("another writer acquired a live lease")
	}
	l.Release(st)
	_ = r
}

func TestLeaseVerifyDetectsReplacement(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepo(st, "lost", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	l, err := r.acquireCommitLease(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Delete(context.Background(), []string{l.key}); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateIfAbsent(context.Background(), l.key, []byte("other 1")); err != nil {
		t.Fatal(err)
	}
	if err := l.Verify(context.Background(), st); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Verify=%v, want ErrLeaseLost", err)
	}
	l.Release(st)
}
