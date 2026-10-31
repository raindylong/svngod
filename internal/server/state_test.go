package server

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"svngod/internal/fs"
	"svngod/internal/fs/store"
	"svngod/internal/svndiff"
)

// TestTxnPersistsAcrossServerInstances: a commit session (activity, txn, file
// body, revprops) must survive a restart, so a second server instance can
// finalize the commit instead of failing with 409.
func TestTxnPersistsAcrossServerInstances(t *testing.T) {
	st := store.NewMemoryStore()
	if _, err := fs.CreateRepo(st, "demo", 4096, 64); err != nil {
		t.Fatal(err)
	}
	newServer := func() (*httptest.Server, *fs.Repo) {
		repo, err := fs.OpenRepo(st, "demo")
		if err != nil {
			t.Fatal(err)
		}
		srv := New(Config{
			BasePath: "/svn",
			OpenRepo: func(name string) (*fs.Repo, error) {
				if name != "demo" {
					return nil, fs.ErrRepoNotFound
				}
				return repo, nil
			},
		})
		return httptest.NewServer(srv.Handler()), repo
	}

	ts1, _ := newServer()
	base := ts1.URL + "/svn/demo"
	user, pass := "", ""

	resp := doRequest(t, "MKACTIVITY", base+"/!svn/act/u-9", "", user, pass, nil)
	if resp.StatusCode != 201 {
		t.Fatalf("MKACTIVITY=%d", resp.StatusCode)
	}
	resp.Body.Close()
	checkout := `<?xml version="1.0"?><D:checkout xmlns:D="DAV:"><D:activity-set><D:href>` +
		base + `/!svn/act/u-9</D:href></D:activity-set></D:checkout>`
	resp = doRequest(t, "CHECKOUT", base+"/!svn/vcc/default", checkout, user, pass, nil)
	if resp.StatusCode != 201 {
		t.Fatalf("CHECKOUT=%d", resp.StatusCode)
	}
	txn := strings.TrimSuffix(resp.Header.Get("Location"), "/")
	resp.Body.Close()

	pp := `<?xml version="1.0"?><D:propertyupdate xmlns:D="DAV:" xmlns:S="http://subversion.tigris.org/xmlns/svn/">` +
		`<D:set><D:prop><S:log>survives restart</S:log></D:prop></D:set></D:propertyupdate>`
	resp = doRequest(t, "PROPPATCH", txn, pp, user, pass, nil)
	if resp.StatusCode != 207 {
		t.Fatalf("PROPPATCH=%d", resp.StatusCode)
	}
	resp.Body.Close()
	resp = doRequest(t, "PUT", txn+"/trunk/a.txt", "payload\n", user, pass, nil)
	if resp.StatusCode != 201 {
		t.Fatalf("PUT=%d", resp.StatusCode)
	}
	resp.Body.Close()

	// "Restart": a brand-new server over the same store.
	ts1.Close()
	ts2, repo2 := newServer()
	defer ts2.Close()
	base2 := ts2.URL + "/svn/demo"
	merge := `<?xml version="1.0"?><D:merge xmlns:D="DAV:"><D:source><D:href>` +
		base2 + `/!svn/act/u-9</D:href></D:source></D:merge>`
	resp = doRequest(t, "MERGE", base2+"/!svn/vcc/default", merge, user, pass, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("MERGE after restart=%d body=%s", resp.StatusCode, bodyOf(t, resp))
	}
	resp.Body.Close()

	head, err := repo2.Head()
	if err != nil || head != 1 {
		t.Fatalf("head=%d err=%v, want 1", head, err)
	}
	nr, err := repo2.Resolve(1, "/trunk/a.txt")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if txt, _ := repo2.GetText(nr.Node); string(txt) != "payload\n" {
		t.Fatalf("content = %q", txt)
	}
	if rp, _ := repo2.GetRevProps(1); rp == nil || rp.Props["svn:log"] != "survives restart" {
		t.Fatalf("revprops = %+v", rp)
	}
}

// TestLargeStreamedTxnPersistsAcrossServerInstances covers the upload path
// that stores a large body as an external text object before MERGE: after a
// restart the session must resume from the stored reference (and its SHA-1),
// not from an in-memory body that no longer exists.
func TestLargeStreamedTxnPersistsAcrossServerInstances(t *testing.T) {
	st := store.NewMemoryStore()
	if _, err := fs.CreateRepo(st, "big", 4096, 64); err != nil {
		t.Fatal(err)
	}
	newServer := func() (*httptest.Server, *fs.Repo) {
		repo, err := fs.OpenRepo(st, "big")
		if err != nil {
			t.Fatal(err)
		}
		srv := New(Config{
			BasePath: "/svn",
			OpenRepo: func(name string) (*fs.Repo, error) {
				if name != "big" {
					return nil, fs.ErrRepoNotFound
				}
				return repo, nil
			},
		})
		return httptest.NewServer(srv.Handler()), repo
	}

	payload := strings.Repeat("0123456789abcdef", (2<<20)/16) // 2 MiB
	sum := sha1.Sum([]byte(payload))

	ts1, _ := newServer()
	base := ts1.URL + "/svn/big"
	resp := doRequest(t, "MKACTIVITY", base+"/!svn/act/u-big", "", "", "", nil)
	if resp.StatusCode != 201 {
		t.Fatalf("MKACTIVITY=%d", resp.StatusCode)
	}
	resp.Body.Close()
	checkout := `<?xml version="1.0"?><D:checkout xmlns:D="DAV:"><D:activity-set><D:href>` +
		base + `/!svn/act/u-big</D:href></D:activity-set></D:checkout>`
	resp = doRequest(t, "CHECKOUT", base+"/!svn/vcc/default", checkout, "", "", nil)
	if resp.StatusCode != 201 {
		t.Fatalf("CHECKOUT=%d", resp.StatusCode)
	}
	txn := strings.TrimSuffix(resp.Header.Get("Location"), "/")
	resp.Body.Close()

	pp := `<?xml version="1.0"?><D:propertyupdate xmlns:D="DAV:" xmlns:S="http://subversion.tigris.org/xmlns/svn/">` +
		`<D:set><D:prop><S:log>big survives restart</S:log></D:prop></D:set></D:propertyupdate>`
	resp = doRequest(t, "PROPPATCH", txn, pp, "", "", nil)
	if resp.StatusCode != 207 {
		t.Fatalf("PROPPATCH=%d", resp.StatusCode)
	}
	resp.Body.Close()
	resp = doRequest(t, "PUT", txn+"/big.bin", payload, "", "", nil)
	if resp.StatusCode != 201 {
		t.Fatalf("PUT=%d", resp.StatusCode)
	}
	resp.Body.Close()

	ts1.Close()
	ts2, repo2 := newServer()
	defer ts2.Close()
	base2 := ts2.URL + "/svn/big"
	merge := `<?xml version="1.0"?><D:merge xmlns:D="DAV:"><D:source><D:href>` +
		base2 + `/!svn/act/u-big</D:href></D:source></D:merge>`
	resp = doRequest(t, "MERGE", base2+"/!svn/vcc/default", merge, "", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("MERGE after restart=%d body=%s", resp.StatusCode, bodyOf(t, resp))
	}
	resp.Body.Close()

	nr, err := repo2.Resolve(1, "/big.bin")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if nr.TextRep != fs.TextRepExternal || nr.TextRef == "" {
		t.Fatalf("text rep=%d ref=%q, want external", nr.TextRep, nr.TextRef)
	}
	if nr.TextSHA1 != sum {
		t.Fatalf("stored sha1 mismatch: got %x want %x", nr.TextSHA1, sum)
	}
	if txt, _ := repo2.GetText(nr.Node); string(txt) != payload {
		t.Fatalf("content mismatch: %d bytes", len(txt))
	}
}

// chunkedReader hides Length so the request body is sent chunked, which is
// how ra_serf uploads PUT bodies and the path that streams the delta.
type chunkedReader struct{ io.Reader }

// TestDeltaTxnPersistsAcrossServerInstances: a large-file edit sent as a
// small svndiff stream must persist (as an activity data object plus the exact
// base node) and still commit after a restart, without a full text copy.
func TestDeltaTxnPersistsAcrossServerInstances(t *testing.T) {
	st := store.NewMemoryStore()
	repo0, err := fs.CreateRepo(st, "bigd", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	base := bytes.Repeat([]byte("0123456789abcdef"), (2<<20)/16) // 2 MiB
	next := append([]byte(nil), base...)
	copy(next[1234:], "a real edit")
	if _, err := repo0.CommitTxn(0, []fs.Change{{Path: "/big.bin", Action: fs.ActionAdd, Kind: fs.KindFile, Data: base}}, nil); err != nil {
		t.Fatal(err)
	}
	delta, err := svndiff.Encode(base, next)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha1.Sum(next)
	newServer := func() (*httptest.Server, *fs.Repo) {
		repo, err := fs.OpenRepo(st, "bigd")
		if err != nil {
			t.Fatal(err)
		}
		srv := New(Config{
			BasePath: "/svn",
			OpenRepo: func(name string) (*fs.Repo, error) {
				if name != "bigd" {
					return nil, fs.ErrRepoNotFound
				}
				return repo, nil
			},
		})
		return httptest.NewServer(srv.Handler()), repo
	}

	ts1, _ := newServer()
	baseURL := ts1.URL + "/svn/bigd"
	resp := doRequest(t, "MKACTIVITY", baseURL+"/!svn/act/u-delta", "", "", "", nil)
	if resp.StatusCode != 201 {
		t.Fatalf("MKACTIVITY=%d", resp.StatusCode)
	}
	resp.Body.Close()
	checkout := `<?xml version="1.0"?><D:checkout xmlns:D="DAV:"><D:activity-set><D:href>` +
		baseURL + `/!svn/act/u-delta</D:href></D:activity-set></D:checkout>`
	resp = doRequest(t, "CHECKOUT", baseURL+"/!svn/vcc/default", checkout, "", "", nil)
	if resp.StatusCode != 201 {
		t.Fatalf("CHECKOUT=%d", resp.StatusCode)
	}
	txn := strings.TrimSuffix(resp.Header.Get("Location"), "/")
	resp.Body.Close()

	req, err := http.NewRequest("PUT", txn+"/big.bin", io.NopCloser(&chunkedReader{bytes.NewReader(delta)}))
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = -1
	req.Header.Set("Content-Type", "application/vnd.svn-svndiff")
	req.Header.Set("X-SVN-Version-Name", "1")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 204 && resp.StatusCode != 201 {
		t.Fatalf("PUT=%d body=%s", resp.StatusCode, bodyOf(t, resp))
	}
	resp.Body.Close()

	ts1.Close()
	ts2, repo2 := newServer()
	defer ts2.Close()
	base2 := ts2.URL + "/svn/bigd"
	merge := `<?xml version="1.0"?><D:merge xmlns:D="DAV:"><D:source><D:href>` +
		base2 + `/!svn/act/u-delta</D:href></D:source></D:merge>`
	resp = doRequest(t, "MERGE", base2+"/!svn/vcc/default", merge, "", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("MERGE after restart=%d body=%s", resp.StatusCode, bodyOf(t, resp))
	}
	resp.Body.Close()

	nr, err := repo2.Resolve(2, "/big.bin")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if nr.TextRep != fs.TextRepDelta || nr.Base == nil {
		t.Fatalf("text rep=%d base=%v, want inline delta", nr.TextRep, nr.Base)
	}
	if nr.TotalSize != uint64(len(next)) || nr.TextSHA1 != sum {
		t.Fatalf("metadata mismatch: size=%d", nr.TotalSize)
	}
	if got, _ := repo2.GetText(nr.Node); !bytes.Equal(got, next) {
		t.Fatal("content mismatch after restart")
	}
}

// TestFragmentsRestoreAllChangeKinds stages an add, a mkdir, a delete and a
// property change in one transaction, restarts the server, and checks that
// every change survives (session state is per-change fragments, not a
// monolithic manifest).
func TestFragmentsRestoreAllChangeKinds(t *testing.T) {
	st := store.NewMemoryStore()
	repo0, err := fs.CreateRepo(st, "frag", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo0.CommitTxn(0, []fs.Change{
		{Path: "/keep.txt", Action: fs.ActionAdd, Kind: fs.KindFile, Data: []byte("keep")},
		{Path: "/gone.txt", Action: fs.ActionAdd, Kind: fs.KindFile, Data: []byte("gone")},
	}, nil); err != nil {
		t.Fatal(err)
	}
	newServer := func() (*httptest.Server, *fs.Repo) {
		repo, err := fs.OpenRepo(st, "frag")
		if err != nil {
			t.Fatal(err)
		}
		srv := New(Config{
			BasePath: "/svn",
			OpenRepo: func(name string) (*fs.Repo, error) {
				if name != "frag" {
					return nil, fs.ErrRepoNotFound
				}
				return repo, nil
			},
		})
		return httptest.NewServer(srv.Handler()), repo
	}

	ts1, _ := newServer()
	base := ts1.URL + "/svn/frag"
	resp := doRequest(t, "MKACTIVITY", base+"/!svn/act/u-frag", "", "", "", nil)
	if resp.StatusCode != 201 {
		t.Fatalf("MKACTIVITY=%d", resp.StatusCode)
	}
	resp.Body.Close()
	checkout := `<?xml version="1.0"?><D:checkout xmlns:D="DAV:"><D:activity-set><D:href>` +
		base + `/!svn/act/u-frag</D:href></D:activity-set></D:checkout>`
	resp = doRequest(t, "CHECKOUT", base+"/!svn/vcc/default", checkout, "", "", nil)
	if resp.StatusCode != 201 {
		t.Fatalf("CHECKOUT=%d", resp.StatusCode)
	}
	txn := strings.TrimSuffix(resp.Header.Get("Location"), "/")
	resp.Body.Close()

	resp = doRequest(t, "PUT", txn+"/new.txt", "hello new", "", "", nil)
	if resp.StatusCode != 201 {
		t.Fatalf("PUT=%d", resp.StatusCode)
	}
	resp.Body.Close()
	resp = doRequest(t, "MKCOL", txn+"/newdir", "", "", "", nil)
	if resp.StatusCode != 201 {
		t.Fatalf("MKCOL=%d", resp.StatusCode)
	}
	resp.Body.Close()
	resp = doRequest(t, "DELETE", txn+"/gone.txt", "", "", "", nil)
	if resp.StatusCode != 204 {
		t.Fatalf("DELETE=%d", resp.StatusCode)
	}
	resp.Body.Close()
	pp := `<?xml version="1.0"?><D:propertyupdate xmlns:D="DAV:" xmlns:C="http://subversion.tigris.org/xmlns/custom/">` +
		`<D:set><D:prop><C:flag>on</C:flag></D:prop></D:set></D:propertyupdate>`
	resp = doRequest(t, "PROPPATCH", txn+"/keep.txt", pp, "", "", nil)
	if resp.StatusCode != 207 {
		t.Fatalf("PROPPATCH=%d", resp.StatusCode)
	}
	resp.Body.Close()

	ts1.Close()
	ts2, repo2 := newServer()
	defer ts2.Close()
	base2 := ts2.URL + "/svn/frag"
	merge := `<?xml version="1.0"?><D:merge xmlns:D="DAV:"><D:source><D:href>` +
		base2 + `/!svn/act/u-frag</D:href></D:source></D:merge>`
	resp = doRequest(t, "MERGE", base2+"/!svn/vcc/default", merge, "", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("MERGE after restart=%d body=%s", resp.StatusCode, bodyOf(t, resp))
	}
	resp.Body.Close()

	if nr, err := repo2.Resolve(2, "/new.txt"); err != nil {
		t.Fatalf("new.txt: %v", err)
	} else if txt, _ := repo2.GetText(nr.Node); string(txt) != "hello new" {
		t.Fatalf("new.txt content=%q", txt)
	}
	if nr, err := repo2.Resolve(2, "/newdir"); err != nil || nr.Kind != fs.KindDir {
		t.Fatalf("newdir: nr=%v err=%v", nr, err)
	}
	if _, err := repo2.Resolve(2, "/gone.txt"); err == nil {
		t.Fatal("gone.txt still present after delete")
	}
	nr, err := repo2.Resolve(2, "/keep.txt")
	if err != nil {
		t.Fatalf("keep.txt: %v", err)
	}
	if nr.Props["flag"] != "on" {
		t.Fatalf("keep.txt props=%v", nr.Props)
	}
}

// TestExpiredSessionReapedOnLoad: a session abandoned by its client must not
// keep its staged fragments forever. Loading a repository drops manifests
// older than sessionTTL together with their change fragments.
func TestExpiredSessionReapedOnLoad(t *testing.T) {
	st := store.NewMemoryStore()
	if _, err := fs.CreateRepo(st, "reap", 4096, 64); err != nil {
		t.Fatal(err)
	}
	newServer := func() *httptest.Server {
		repo, err := fs.OpenRepo(st, "reap")
		if err != nil {
			t.Fatal(err)
		}
		srv := New(Config{
			BasePath: "/svn",
			OpenRepo: func(name string) (*fs.Repo, error) {
				if name != "reap" {
					return nil, fs.ErrRepoNotFound
				}
				return repo, nil
			},
		})
		return httptest.NewServer(srv.Handler())
	}
	ts1 := newServer()
	base := ts1.URL + "/svn/reap"
	resp := doRequest(t, "MKACTIVITY", base+"/!svn/act/u-old", "", "", "", nil)
	if resp.StatusCode != 201 {
		t.Fatalf("MKACTIVITY=%d", resp.StatusCode)
	}
	resp.Body.Close()
	checkout := `<?xml version="1.0"?><D:checkout xmlns:D="DAV:"><D:activity-set><D:href>` +
		base + `/!svn/act/u-old</D:href></D:activity-set></D:checkout>`
	resp = doRequest(t, "CHECKOUT", base+"/!svn/vcc/default", checkout, "", "", nil)
	if resp.StatusCode != 201 {
		t.Fatalf("CHECKOUT=%d", resp.StatusCode)
	}
	txn := strings.TrimSuffix(resp.Header.Get("Location"), "/")
	resp.Body.Close()
	resp = doRequest(t, "PUT", txn+"/a.txt", "staged", "", "", nil)
	if resp.StatusCode != 201 {
		t.Fatalf("PUT=%d", resp.StatusCode)
	}
	resp.Body.Close()
	ts1.Close()

	repo, err := fs.OpenRepo(st, "reap")
	if err != nil {
		t.Fatal(err)
	}
	old := persistedActivity{ID: "u-old", Created: time.Now().Add(-2 * sessionTTL).Unix()}
	data, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Put(context.Background(), activityStateKey(repo, "u-old"), data); err != nil {
		t.Fatal(err)
	}
	objs, _, _ := st.ListPrefix(context.Background(), activityChangesPrefix(repo, "u-old"), "", 100)
	if len(objs) == 0 {
		t.Fatal("test setup: expected staged fragments")
	}

	ts2 := newServer()
	defer ts2.Close()
	// Any request for the repo triggers the one-time session load.
	resp = doRequest(t, "OPTIONS", ts2.URL+"/svn/reap", "", "", "", nil)
	resp.Body.Close()

	objs, _, err = st.ListPrefix(context.Background(), activityChangesPrefix(repo, "u-old"), "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 0 {
		t.Fatalf("fragments survived: %d objects", len(objs))
	}
	if _, ok, _ := st.Head(context.Background(), activityStateKey(repo, "u-old")); ok {
		t.Fatal("manifest survived")
	}
}
