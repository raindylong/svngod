package fs

import (
	"context"
	"testing"
	"time"

	"svngod/internal/fs/store"
)

// TestRepoOverGeoStore: commits through the geo store ack on the local
// quorum, the WORM journal chain forms normally, and the remote region ends
// up with a fully readable repository (HEAD, revision content, props).
func TestRepoOverGeoStore(t *testing.T) {
	local := store.NewMirror([]store.Store{store.NewMemoryStore(), store.NewMemoryStore()},
		store.WithWriteQuorum(2), store.WithRepoPrefix("repos/geo"))
	remote := store.NewMirror([]store.Store{store.NewMemoryStore(), store.NewMemoryStore()},
		store.WithWriteQuorum(2), store.WithRepoPrefix("repos/geo"))
	g := store.NewGeoStore(store.GeoConfig{
		Local:       local,
		Remote:      remote,
		ControlName: "repos/geo/current",
		Interval:    10 * time.Millisecond,
	})
	defer g.Close()

	r, err := CreateRepo(g, "repos/geo", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	const n = 5
	for i := 0; i < n; i++ {
		if _, err := r.CommitTxn(uint64(i), []Change{{
			Path: "/g.txt", Action: ActionModify, Kind: KindFile, Data: []byte("v"),
		}}, nil); err != nil {
			t.Fatal(err)
		}
	}
	// Local chain and content are intact.
	if err := r.VerifyJournalChain(1, n); err != nil {
		t.Fatalf("local chain: %v", err)
	}
	// Wait for the shipper to drain everything (repo creation + n commits,
	// each writing several objects).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if g.Status().Pending == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if st := g.Status(); st.Pending != 0 {
		t.Fatalf("geo shipper stuck: %+v", st)
	}

	// Open the remote region read-only: it must serve the same history.
	r2, err := OpenRepo(remote, "repos/geo")
	if err != nil {
		t.Fatal(err)
	}
	head, err := r2.HeadFresh()
	if err != nil || head != n {
		t.Fatalf("remote head=%d (err %v), want %d", head, err, n)
	}
	nr, err := r2.Resolve(head, "/g.txt")
	if err != nil || nr == nil {
		t.Fatalf("remote resolve: %v", err)
	}
	if err := r2.VerifyJournalChain(1, n); err != nil {
		t.Fatalf("remote chain: %v", err)
	}
	issues, err := r2.VerifyRange(1, head, true)
	if err != nil || len(issues) > 0 {
		t.Fatalf("remote verify: err=%v issues=%v", err, issues)
	}
	_ = context.Background()
}
