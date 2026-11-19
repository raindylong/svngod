package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"svngod/internal/cdc"
	"svngod/internal/fs"
	"svngod/internal/fs/store"
)

// The bundle chunk paths repeat the pattern fixed in state.go for fragment
// reads, with an extra wrinkle: work reaches the workers over a bounded channel,
// so there are two places to block, not one.
//
//   - handleBundleChunks: 16 workers call Store().Exists, fed by ch <- h
//   - storeChunksParallel: 8 workers call Store().Put, fed by jobs <- job
//
// Both run under the ctx from handleBundleCommit, which is
// context.Background(), and http.Server sets no ReadTimeout/WriteTimeout (large
// uploads are expected). Nothing bounds them: once every worker is parked in the
// store the channel buffer fills, the producer blocks on the send, and it never
// reaches wg.Wait() - so bounding the wait alone would be dead code, exactly as
// with the fragment semaphore.
//
// storeChunksParallel adds a second hazard: five of its six wg.Wait() calls are
// error-path cleanup (close(jobs) then wait for workers to drain). A checksum
// mismatch is detected while workers are mid-Put, so even the early-return path
// hangs.

// bundleStallStore blocks Put/Exists until released and ignores ctx, modelling a
// store that does not honour cancellation.
type bundleStallStore struct {
	store.Store
	release chan struct{}
	hit     chan struct{}
	once    sync.Once
	// armed gates the stall. fs.CreateRepo writes repo config through Put, so a
	// store that blocks from construction would deadlock the test before it
	// reaches the code under test. The tests arm it after creating the repo.
	armed atomic.Bool
}

func newBundleStallStore(inner store.Store) *bundleStallStore {
	return &bundleStallStore{
		Store:   inner,
		release: make(chan struct{}),
		hit:     make(chan struct{}, 64),
	}
}

func (s *bundleStallStore) arm() { s.armed.Store(true) }

func (s *bundleStallStore) block() {
	if !s.armed.Load() {
		return
	}
	select {
	case s.hit <- struct{}{}:
	default:
	}
	<-s.release
}

func (s *bundleStallStore) Put(ctx context.Context, key string, data []byte) error {
	s.block()
	return s.Store.Put(context.Background(), key, data)
}

func (s *bundleStallStore) Exists(ctx context.Context, key string) (bool, error) {
	s.block()
	return s.Store.Exists(context.Background(), key)
}

func (s *bundleStallStore) unblock() { s.once.Do(func() { close(s.release) }) }

// chunkRefs builds n distinct chunks and the concatenated body that carries them.
// When corruptLast is set the final chunk's bytes are replaced so the checksum
// check fails.
func chunkRefs(n int, corruptLast bool) ([]cdc.Ref, map[string]bool, []byte) {
	refs := make([]cdc.Ref, n)
	missing := make(map[string]bool, n)
	var body []byte
	for i := 0; i < n; i++ {
		data := []byte(fmt.Sprintf("chunk%05d", i))
		sum := sha256.Sum256(data)
		refs[i] = cdc.Ref{Size: uint32(len(data)), Hash: sum}
		missing[hex.EncodeToString(sum[:])] = true
		if corruptLast && i == n-1 {
			body = append(body, bytes.Repeat([]byte("X"), len(data))...)
			continue
		}
		body = append(body, data...)
	}
	return refs, missing, body
}

func TestStoreChunksParallelDoesNotHangOnStalledStore(t *testing.T) {
	st := newBundleStallStore(store.NewMemoryStore())
	defer st.unblock()
	repo, err := fs.CreateRepo(st, "bstall", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	st.arm()

	// More chunks than workers+buffer, so the producer must wait for a free slot.
	refs, missing, body := chunkRefs(40, false)

	done := make(chan error, 1)
	go func() {
		done <- storeChunksParallel(context.Background(), repo, refs, missing, bytes.NewReader(body))
	}()

	select {
	case <-st.hit:
	case <-time.After(2 * time.Second):
		t.Fatal("chunk store never attempted")
	}
	select {
	case <-done:
	case <-time.After(bundleChunkTimeout + 10*time.Second):
		t.Fatal("storeChunksParallel never returned: producer blocked on a full job channel")
	}
}

func TestStoreChunksParallelErrorPathDoesNotHang(t *testing.T) {
	st := newBundleStallStore(store.NewMemoryStore())
	defer st.unblock()
	repo, err := fs.CreateRepo(st, "bstall3", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	st.arm()

	refs, missing, body := chunkRefs(20, true)

	done := make(chan error, 1)
	go func() {
		done <- storeChunksParallel(context.Background(), repo, refs, missing, bytes.NewReader(body))
	}()

	select {
	case <-st.hit:
	case <-time.After(2 * time.Second):
		t.Fatal("chunk store never attempted")
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("corrupt chunk was accepted")
		}
	case <-time.After(bundleChunkTimeout + 10*time.Second):
		t.Fatal("error path never returned: cleanup wait blocked on stuck workers")
	}
}

func TestHandleBundleChunksDoesNotHangOnStalledStore(t *testing.T) {
	st := newBundleStallStore(store.NewMemoryStore())
	defer st.unblock()
	repo, err := fs.CreateRepo(st, "bstall2", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	st.arm()
	srv := New(Config{
		BasePath: "/svn",
		OpenRepo: func(name string) (*fs.Repo, error) {
			if name != "bstall2" {
				return nil, fs.ErrRepoNotFound
			}
			return repo, nil
		},
	})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	hashes := make([]string, 40)
	for i := range hashes {
		hashes[i] = fmt.Sprintf("%064x", i)
	}
	q, _ := json.Marshal(map[string]interface{}{"hashes": hashes})

	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, err := http.Post(ts.URL+"/svn/bstall2/!svngod/chunks", "application/json", bytes.NewReader(q))
		if err == nil {
			resp.Body.Close()
		}
	}()

	select {
	case <-st.hit:
	case <-time.After(2 * time.Second):
		t.Fatal("chunk existence check never started")
	}
	select {
	case <-done:
	case <-time.After(bundleChunkTimeout + 10*time.Second):
		t.Fatal("handleBundleChunks never returned on a stalled store")
	}
}

// TestStoreChunksParallelHealthyStillWorks guards against a fix that stalls the
// normal path.
func TestStoreChunksParallelHealthyStillWorks(t *testing.T) {
	mem := store.NewMemoryStore()
	repo, err := fs.CreateRepo(mem, "bok", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	refs, missing, body := chunkRefs(12, false)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := storeChunksParallel(ctx, repo, refs, missing, bytes.NewReader(body)); err != nil {
		t.Fatalf("healthy store failed: %v", err)
	}
	for _, ref := range refs {
		key := repo.Keys().Chunk(cdc.HashHex(ref))
		ok, err := mem.Exists(context.Background(), key)
		if err != nil || !ok {
			t.Fatalf("chunk %s not stored (ok=%v err=%v)", cdc.HashHex(ref), ok, err)
		}
	}
}

// TestHandleBundleChunksHealthyStillWorks is the same guard for the query pool.
func TestHandleBundleChunksHealthyStillWorks(t *testing.T) {
	mem := store.NewMemoryStore()
	repo, err := fs.CreateRepo(mem, "bok2", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	srv := New(Config{
		BasePath: "/svn",
		OpenRepo: func(name string) (*fs.Repo, error) {
			if name != "bok2" {
				return nil, fs.ErrRepoNotFound
			}
			return repo, nil
		},
	})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	hashes := make([]string, 40)
	for i := range hashes {
		hashes[i] = fmt.Sprintf("%064x", i)
	}
	q, _ := json.Marshal(map[string]interface{}{"hashes": hashes})

	resp, err := http.Post(ts.URL+"/svn/bok2/!svngod/chunks", "application/json", bytes.NewReader(q))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Missing []string `json:"missing"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.Missing) != len(hashes) {
		t.Fatalf("missing=%d, want %d (nothing is stored yet)", len(out.Missing), len(hashes))
	}
}
