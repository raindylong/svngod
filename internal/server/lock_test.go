package server

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

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

// TestLockResponseWellFormed guards the LOCK response: it used to declare only
// the DAV namespace while emitting S:* elements, so the body was not
// well-formed XML. svn printed the lock as successful but stored no token,
// making a later `svn unlock` fail with "not locked in this working copy".
func TestLockResponseWellFormed(t *testing.T) {
	ts, _, base := testServer(t, "", "")
	defer ts.Close()
	resp := doRequest(t, "LOCK", base+"/svn/demo/trunk/README.txt", lockBody, "", "", nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("LOCK=%d", resp.StatusCode)
	}
	b := bodyOf(t, resp)
	var doc struct {
		XMLName xml.Name
	}
	if err := xml.Unmarshal([]byte(b), &doc); err != nil {
		t.Fatalf("LOCK response is not well-formed XML: %v\n%s", err, b)
	}
}

// TestBlockingLockReloadsAfterRemoteUnlock reproduces the cross-replica case:
// replica B caches a lock, replica A unlocks it, and B must reload the shared
// table instead of rejecting the write with a stale 423.
func TestBlockingLockReloadsAfterRemoteUnlock(t *testing.T) {
	ta, tb, repo, a, b := twoReplicas(t)
	defer ta.Close()
	defer tb.Close()
	urlA := ta.URL + "/svn/demo/trunk/README.txt"

	resp := doRequest(t, "LOCK", urlA, lockBody, "", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("LOCK=%d", resp.StatusCode)
	}
	tok := strings.Trim(resp.Header.Get("Lock-Token"), "<>")
	resp.Body.Close()

	// B learns about the lock the way a MERGE would.
	b.refreshLocks("demo", repo)
	if b.getLock("demo", "/trunk/README.txt") == nil {
		t.Fatal("replica B did not see the lock")
	}
	if e := b.blockingLock("demo", repo, "/trunk/README.txt", ""); e == nil {
		t.Fatal("locked path was not blocked")
	}

	resp = doRequest(t, "UNLOCK", urlA, "", "", "", map[string]string{"Lock-Token": tok})
	if resp.StatusCode != 204 {
		t.Fatalf("UNLOCK=%d", resp.StatusCode)
	}
	resp.Body.Close()
	_ = a

	if e := b.blockingLock("demo", repo, "/trunk/README.txt", ""); e != nil {
		t.Fatal("stale cached lock still blocks after remote unlock")
	}
}

// TestDepthInfinityLockCoversChildren: a directory lock must protect the
// files below it, exactly as a DAV depth-infinity lock does.
func TestDepthInfinityLockCoversChildren(t *testing.T) {
	_, _, repo, a, _ := twoReplicas(t)
	out, err := a.mutateLocks("demo", repo, func(list []lockedJSON) ([]lockedJSON, interface{}, error) {
		return append(list, lockedJSON{
			Path: "/trunk", Token: "urn:uuid:dirlock", Owner: "alice",
			Depth: "infinity", Created: time.Now().Unix(), Timeout: 3600,
		}), nil, nil
	})
	if err != nil || out != nil {
		t.Fatalf("seed lock: %v", err)
	}
	if e := a.blockingLock("demo", repo, "/trunk/README.txt", ""); e == nil {
		t.Fatal("child of a depth-infinity locked dir was not blocked")
	}
	if e := a.blockingLock("demo", repo, "/trunk/README.txt", "urn:uuid:dirlock"); e != nil {
		t.Fatal("child write with the directory lock token was rejected")
	}
}

// TestParseMergeLocks parses the lock-token-list svn sends on commit. Apache's
// MERGE body is reproduced verbatim.
func TestParseMergeLocks(t *testing.T) {
	body := `<?xml version="1.0" encoding="utf-8"?><D:merge xmlns:D="DAV:">` +
		`<D:source><D:href>/svn/ref/!svn/act/abc</D:href></D:source>` +
		`<S:lock-token-list xmlns:S="svn:">` +
		`<S:lock><lock-path>f.txt</lock-path><lock-token>opaquelocktoken:01</lock-token></S:lock>` +
		`<S:lock><lock-path>dir/c.txt</lock-path><lock-token>opaquelocktoken:02</lock-token></S:lock>` +
		`</S:lock-token-list></D:merge>`
	locks := parseMergeLocks([]byte(body))
	if len(locks) != 2 {
		t.Fatalf("parsed %d locks, want 2", len(locks))
	}
	if !mergeLockCovers(locks, "/f.txt", "opaquelocktoken:01") {
		t.Fatal("leading-slash lock path not matched")
	}
	if mergeLockCovers(locks, "/f.txt", "opaquelocktoken:02") {
		t.Fatal("token matched the wrong path")
	}
	if !mergeLockCovers(locks, "/dir/c.txt", "opaquelocktoken:02") {
		t.Fatal("nested lock path not matched")
	}
}

// TestReleaseCommittedLocks: a commit with release-locks (the default) drops
// exactly the locks it used; --no-unlock sends no such list and keeps them.
func TestReleaseCommittedLocks(t *testing.T) {
	_, _, repo, a, _ := twoReplicas(t)
	if _, err := a.mutateLocks("demo", repo, func(list []lockedJSON) ([]lockedJSON, interface{}, error) {
		return append(list,
			lockedJSON{Path: "/trunk/README.txt", Token: "urn:uuid:aa", Owner: "alice", Depth: "infinity", Created: time.Now().Unix(), Timeout: 3600},
			lockedJSON{Path: "/trunk/doc/guide.txt", Token: "urn:uuid:bb", Owner: "bob", Depth: "infinity", Created: time.Now().Unix(), Timeout: 3600},
		), nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	a.releaseCommittedLocks("demo", repo, []mergeLock{{Path: "trunk/README.txt", Token: "urn:uuid:aa"}})
	list, _, err := readLockState(repo)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].Path != "/trunk/doc/guide.txt" {
		t.Fatalf("locks after release=%+v, want only guide.txt", list)
	}
}
