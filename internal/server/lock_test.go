package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"svngod/internal/fs"
)

const lockBody = `<?xml version="1.0"?>` +
	`<D:lockinfo xmlns:D="DAV:">` +
	`<D:lockscope><D:exclusive/></D:lockscope>` +
	`<D:locktype><D:write/></D:locktype>` +
	`<D:owner>alice</D:owner>` +
	`</D:lockinfo>`

func TestLockUnlock(t *testing.T) {
	ts, _, base := testServer(t, "", "")
	defer ts.Close()
	u := base + "/svn/demo/trunk/README.txt"

	// LOCK
	resp := doRequest(t, "LOCK", u, lockBody, "", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("LOCK=%d body=%s", resp.StatusCode, bodyOf(t, resp))
	}
	tok := resp.Header.Get("Lock-Token")
	if tok == "" || !strings.Contains(tok, "urn:uuid:") {
		t.Fatalf("no Lock-Token header: %q", tok)
	}
	if !strings.Contains(bodyOf(t, resp), "lockdiscovery") {
		t.Fatal("LOCK response missing lockdiscovery")
	}
	resp.Body.Close()
	tok = strings.Trim(tok, "<>")

	// second LOCK without token → 423
	resp = doRequest(t, "LOCK", u, lockBody, "", "", nil)
	if resp.StatusCode != http.StatusLocked {
		t.Fatalf("second LOCK=%d want 423", resp.StatusCode)
	}
	resp.Body.Close()

	// PROPFIND shows the lock
	pf := `<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:prop><D:lockdiscovery/></D:prop></D:propfind>`
	resp = doRequest(t, "PROPFIND", u, pf, "", "", nil)
	if resp.StatusCode != 207 {
		t.Fatalf("PROPFIND=%d", resp.StatusCode)
	}
	if !strings.Contains(bodyOf(t, resp), "lockdiscovery") {
		t.Fatal("PROPFIND missing lockdiscovery")
	}
	resp.Body.Close()

	// UNLOCK
	resp = doRequest(t, "UNLOCK", u, "", "", "", map[string]string{"Lock-Token": tok})
	if resp.StatusCode != 204 {
		t.Fatalf("UNLOCK=%d body=%s", resp.StatusCode, bodyOf(t, resp))
	}
	resp.Body.Close()

	// unlock again → 409 (no lock)
	resp = doRequest(t, "UNLOCK", u, "", "", "", map[string]string{"Lock-Token": tok})
	if resp.StatusCode != 409 {
		t.Fatalf("second UNLOCK=%d want 409", resp.StatusCode)
	}
	resp.Body.Close()

	// lock again works
	resp = doRequest(t, "LOCK", u, lockBody, "", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("re-LOCK=%d", resp.StatusCode)
	}
	resp.Body.Close()
}

// twoReplicas builds two Server instances over one repository store, the way
// two svngod processes share an object store.
func twoReplicas(t *testing.T) (*httptest.Server, *httptest.Server, *fs.Repo, *Server, *Server) {
	t.Helper()
	repo := buildRepo(t)
	mk := func() *Server {
		return New(Config{
			BasePath: "/svn",
			OpenRepo: func(name string) (*fs.Repo, error) {
				if name == "demo" {
					return repo, nil
				}
				return nil, fs.ErrRepoNotFound
			},
		})
	}
	a, b := mk(), mk()
	ta := httptest.NewServer(a.Handler())
	tb := httptest.NewServer(b.Handler())
	return ta, tb, repo, a, b
}

func TestLockVisibleAcrossReplicas(t *testing.T) {
	ta, tb, repo, a, _ := twoReplicas(t)
	defer ta.Close()
	defer tb.Close()
	urlA := ta.URL + "/svn/demo/trunk/README.txt"
	urlB := tb.URL + "/svn/demo/trunk/README.txt"

	resp := doRequest(t, "LOCK", urlA, lockBody, "", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("A LOCK=%d", resp.StatusCode)
	}
	tok := strings.Trim(resp.Header.Get("Lock-Token"), "<>")
	resp.Body.Close()

	// B has never seen this lock in memory; its mutation reads the shared
	// table and must refuse.
	resp = doRequest(t, "LOCK", urlB, lockBody, "", "", nil)
	if resp.StatusCode != http.StatusLocked {
		t.Fatalf("B LOCK=%d, want 423", resp.StatusCode)
	}
	resp.Body.Close()
	if a.getLock("demo", "/trunk/README.txt") == nil {
		t.Fatal("A lost its own lock")
	}

	// After A unlocks, B can lock.
	resp = doRequest(t, "UNLOCK", urlA, "", "", "", map[string]string{"Lock-Token": tok})
	if resp.StatusCode != 204 {
		t.Fatalf("A UNLOCK=%d", resp.StatusCode)
	}
	resp.Body.Close()
	resp = doRequest(t, "LOCK", urlB, lockBody, "", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("B LOCK after unlock=%d", resp.StatusCode)
	}
	resp.Body.Close()
	_ = repo
}

func TestConcurrentLocksOnDifferentPathsSurvive(t *testing.T) {
	ta, tb, repo, a, b := twoReplicas(t)
	defer ta.Close()
	defer tb.Close()
	var wg sync.WaitGroup
	codes := make([]int, 2)
	urls := []string{
		ta.URL + "/svn/demo/trunk/README.txt",
		tb.URL + "/svn/demo/trunk/doc/guide.txt",
	}
	for i := range urls {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp := doRequest(t, "LOCK", urls[i], lockBody, "", "", nil)
			codes[i] = resp.StatusCode
			resp.Body.Close()
		}(i)
	}
	wg.Wait()
	for i, c := range codes {
		if c != 200 {
			t.Fatalf("lock %d status=%d", i, c)
		}
	}
	list, _, err := readLockState(repo)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 {
		t.Fatalf("persisted locks=%d, want 2 (lost update)", len(list))
	}
	a.refreshLocks("demo", repo)
	b.refreshLocks("demo", repo)
	if a.getLock("demo", "/trunk/README.txt") == nil || a.getLock("demo", "/trunk/doc/guide.txt") == nil {
		t.Fatal("replica A does not see both locks")
	}
	if b.getLock("demo", "/trunk/README.txt") == nil || b.getLock("demo", "/trunk/doc/guide.txt") == nil {
		t.Fatal("replica B does not see both locks")
	}
}
