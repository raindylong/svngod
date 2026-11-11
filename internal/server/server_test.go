package server

import (
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"svngod/internal/auth"
	"svngod/internal/authz"
	"svngod/internal/cdc"
	"svngod/internal/fs"
	"svngod/internal/fs/store"
)

// buildRepo creates a memory-backed repo with some commits and returns it.
func buildRepo(t *testing.T) *fs.Repo {
	t.Helper()
	st := store.NewMemoryStore()
	r, err := fs.CreateRepo(st, "demo", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	rev1, err := r.CommitTxn(0, []fs.Change{
		{Path: "/trunk", Action: fs.ActionAdd, Kind: fs.KindDir},
		{Path: "/trunk/README.txt", Action: fs.ActionAdd, Kind: fs.KindFile, Data: []byte("hello svn\n")},
		{Path: "/trunk/doc", Action: fs.ActionAdd, Kind: fs.KindDir},
		{Path: "/trunk/doc/guide.txt", Action: fs.ActionAdd, Kind: fs.KindFile, Data: []byte("guide content\n")},
	}, map[string]string{"svn:log": "initial import", "svn:author": "alice", "svn:date": "2026-10-28T10:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	if rev1 != 1 {
		t.Fatalf("head=%d", rev1)
	}
	return r
}

// testServer builds an httptest server with auth+authz wired in.
func testServer(t *testing.T, passwdContents, authzContents string) (*httptest.Server, *fs.Repo, string) {
	t.Helper()
	repo := buildRepo(t)
	repos := map[string]*fs.Repo{"demo": repo}

	var chain *auth.Chain
	if passwdContents != "" {
		dir := t.TempDir()
		p := filepath.Join(dir, "passwd")
		if err := os.WriteFile(p, []byte(passwdContents), 0o600); err != nil {
			t.Fatal(err)
		}
		chain = auth.NewChain(auth.NewLocalPasswordFile(p))
	}

	var az *authz.Config
	if authzContents != "" {
		c, err := authz.Parse([]byte(authzContents))
		if err != nil {
			t.Fatal(err)
		}
		az = c
	}

	srv := New(Config{
		BasePath:     "/svn",
		Authenticate: chain,
		Authorize:    az,
		OpenRepo: func(name string) (*fs.Repo, error) {
			if r, ok := repos[name]; ok {
				return r, nil
			}
			return nil, fs.ErrRepoNotFound
		},
	})
	ts := httptest.NewServer(srv.Handler())
	return ts, repo, ts.URL
}

// shaPass returns a passwd-line value for password using {SHA}.
func shaPass(pass string) string {
	h := sha1.Sum([]byte(pass))
	return "{SHA}" + base64.StdEncoding.EncodeToString(h[:])
}

// doRequest sends a request and returns the response.
func doRequest(t *testing.T, method, url, body, user, pass string, hdrs map[string]string) *http.Response {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "text/xml")
	}
	if user != "" {
		req.SetBasicAuth(user, pass)
	}
	for k, v := range hdrs {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func bodyOf(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return string(b)
}

func TestOptionsCapabilities(t *testing.T) {
	t.Setenv("SVNGOD_HTTPV2", "0")
	ts, repo, base := testServer(t, "", "")
	defer ts.Close()
	resp := doRequest(t, "OPTIONS", base+"/svn/demo/", "", "", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	// Capabilities are advertised as several DAV: header lines (as mod_dav_svn
	// does), so join them before checking.
	dav := strings.Join(resp.Header.Values("DAV"), ",")
	if !strings.Contains(dav, "version-control") || !strings.Contains(dav, "merge") || !strings.Contains(dav, "svn/mergeinfo") {
		t.Fatalf("DAV header wrong: %q", dav)
	}
	uuid, _ := repo.UUID()
	if resp.Header.Get("SVN-Repository-UUID") != uuid {
		t.Fatalf("uuid mismatch: %q vs %q", resp.Header.Get("SVN-Repository-UUID"), uuid)
	}
	// HTTPv1 OPTIONS must NOT advertise SVN-Repository-Root (v2-only) and
	// should include SVN-Youngest-Rev instead.
	if resp.Header.Get("SVN-Repository-Root") != "" {
		t.Fatalf("v1 OPTIONS must not send SVN-Repository-Root, got %q", resp.Header.Get("SVN-Repository-Root"))
	}
	head, _ := repo.Head()
	if resp.Header.Get("SVN-Youngest-Rev") != strconv.FormatUint(head, 10) {
		t.Fatalf("SVN-Youngest-Rev=%q want %d", resp.Header.Get("SVN-Youngest-Rev"), head)
	}
	resp.Body.Close()
}

func TestPropfindVccDiscovery(t *testing.T) {
	ts, _, base := testServer(t, "", "")
	defer ts.Close()
	body := `<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:prop><D:version-controlled-configuration/><D:resourcetype/><D:baseline-relative-path/><D:repository-uuid/></D:prop></D:propfind>`
	resp := doRequest(t, "PROPFIND", base+"/svn/demo/", body, "", "", map[string]string{"Depth": "0"})
	if resp.StatusCode != 207 {
		t.Fatalf("status=%d body=%s", resp.StatusCode, bodyOf(t, resp))
	}
	out := bodyOf(t, resp)
	for _, want := range []string{
		"multistatus", "!svn/vcc/default", "version-controlled-configuration", "repository-uuid",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("propfind missing %q:\n%s", want, out)
		}
	}
}

func TestPropfindListDirDepth1(t *testing.T) {
	ts, _, base := testServer(t, "", "")
	defer ts.Close()
	body := `<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:prop><D:resourcetype/><D:getetag/><D:getcontentlength/><D:version-name/></D:prop></D:propfind>`
	resp := doRequest(t, "PROPFIND", base+"/svn/demo/trunk", body, "", "", map[string]string{"Depth": "1"})
	if resp.StatusCode != 207 {
		t.Fatalf("status=%d body=%s", resp.StatusCode, bodyOf(t, resp))
	}
	out := bodyOf(t, resp)
	for _, want := range []string{
		"README.txt", "doc", "collection", "getetag",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("list missing %q:\n%s", want, out)
		}
	}
}

func TestGetFileContent(t *testing.T) {
	ts, _, base := testServer(t, "", "")
	defer ts.Close()
	resp := doRequest(t, "GET", base+"/svn/demo/trunk/README.txt", "", "", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	out := bodyOf(t, resp)
	if out != "hello svn\n" {
		t.Fatalf("content=%q", out)
	}
}

func TestAuthRequired(t *testing.T) {
	ts, _, base := testServer(t, "alice:"+shaPass("s3cret"), "")
	defer ts.Close()
	// bad password -> 401
	resp := doRequest(t, "PROPFIND", base+"/svn/demo/", "", "alice", "wrong", map[string]string{"Depth": "0"})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad pass status=%d", resp.StatusCode)
	}
	resp.Body.Close()
	// good password -> 200
	resp = doRequest(t, "PROPFIND", base+"/svn/demo/", "", "alice", "s3cret", map[string]string{"Depth": "0"})
	if resp.StatusCode != 207 {
		t.Fatalf("good pass status=%d", resp.StatusCode)
	}
	resp.Body.Close()
	// no credentials, no authz -> anonymous read allowed
	resp = doRequest(t, "PROPFIND", base+"/svn/demo/", "", "", "", map[string]string{"Depth": "0"})
	if resp.StatusCode != 207 {
		t.Fatalf("anon status=%d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestAuthzWriteDenied(t *testing.T) {
	authzCfg := `
[demo:/]
alice = rw
bob = r
`
	ts, _, base := testServer(t, "alice:"+shaPass("a")+"\nbob:"+shaPass("b"), authzCfg)
	defer ts.Close()
	// bob cannot write: PROPPATCH /trunk -> 403
	resp := doRequest(t, "PROPPATCH", base+"/svn/demo/trunk/README.txt", "<x/>", "bob", "b", nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("bob write status=%d want 403", resp.StatusCode)
	}
	resp.Body.Close()
	// alice can write (still 501 not-implemented, but not 403)
	resp = doRequest(t, "PROPPATCH", base+"/svn/demo/trunk/README.txt", "<x/>", "alice", "a", nil)
	if resp.StatusCode == http.StatusForbidden {
		t.Fatal("alice should not be forbidden")
	}
	resp.Body.Close()
	// anonymous read: no rule matches anonymous -> challenged with 401 so the
	// SVN client retries with credentials (not a terminal 403).
	resp = doRequest(t, "PROPFIND", base+"/svn/demo/", "", "", "", map[string]string{"Depth": "0"})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("anonymous read status=%d want 401 (challenge, no anon rule)", resp.StatusCode)
	}
	if resp.Header.Get("WWW-Authenticate") == "" {
		t.Fatal("401 must carry WWW-Authenticate")
	}
	resp.Body.Close()
}

func TestUrlParsing(t *testing.T) {
	cases := []struct {
		in   string
		typ  resourceType
		repo string
		rev  uint64
		path string
		uuid string
	}{
		{"/svn/demo/", resRoot, "demo", 0, "", ""},
		{"/svn/demo", resRoot, "demo", 0, "", ""},
		{"/svn/demo/trunk/README.txt", resPublic, "demo", 0, "/trunk/README.txt", ""},
		{"/svn/demo/!svn/vcc/default", resVcc, "demo", 0, "", ""},
		{"/svn/demo/!svn/bc/12/trunk/doc", resBaselineColl, "demo", 12, "/trunk/doc", ""},
		{"/svn/demo/!svn/bln/9", resBaseline, "demo", 9, "", ""},
		{"/svn/demo/!svn/act/abc-123", resActivity, "demo", 0, "", "abc-123"},
		{"/svn/demo/!svn/me", resMe, "demo", 0, "", ""},
		{"/svn/demo/!svn/txn/txn-1", resTxn, "demo", 0, "/", "txn-1"},
		{"/svn/demo/!svn/rev/5/trunk", resRev, "demo", 5, "/trunk", ""},
		{"/svn/demo/!svn/ver/3/trunk/a.txt", resVersion, "demo", 3, "/trunk/a.txt", ""},
		{"/svn/demo/!svn/wc/wc-1/trunk", resWorking, "demo", 0, "/trunk", "wc-1"},
	}
	for _, c := range cases {
		got := parseResource("/svn", c.in)
		if got.typ != c.typ || got.repo != c.repo || got.rev != c.rev || got.path != c.path || got.uuid != c.uuid {
			t.Errorf("parse(%q) = %+v want {typ=%d repo=%q rev=%d path=%q uuid=%q}",
				c.in, got, c.typ, c.repo, c.rev, c.path, c.uuid)
		}
	}
	_ = fmt.Sprintf
}

// TestAuthzSubtreeWrite: a user whose only write grant is on a subtree must be
// able to run the commit protocol (MKACTIVITY/CHECKOUT/PROPPATCH/DELETE used
// to be authorized against "/", which denied the whole commit), while node
// writes outside the grant stay forbidden.
func TestAuthzSubtreeWrite(t *testing.T) {
	authz := "[/]\nalice = r\n[/trunk]\nalice = rw\n"
	ts, _, base := testServer(t, "alice:"+shaPass("pw")+"\n", authz)
	defer ts.Close()
	user, pass := "alice", "pw"

	resp := doRequest(t, "MKACTIVITY", base+"/svn/demo/!svn/act/u-1", "", user, pass, nil)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("MKACTIVITY status=%d body=%s", resp.StatusCode, bodyOf(t, resp))
	}
	resp.Body.Close()

	checkout := `<?xml version="1.0"?><D:checkout xmlns:D="DAV:"><D:activity-set><D:href>` +
		base + `/svn/demo/!svn/act/u-1</D:href></D:activity-set></D:checkout>`
	resp = doRequest(t, "CHECKOUT", base+"/svn/demo/!svn/vcc/default", checkout, user, pass, nil)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("CHECKOUT status=%d body=%s", resp.StatusCode, bodyOf(t, resp))
	}
	loc := resp.Header.Get("Location")
	resp.Body.Close()
	txn := strings.TrimSuffix(loc, "/")
	if !strings.Contains(txn, "/!svn/txn/") {
		t.Fatalf("unexpected txn location %q", loc)
	}

	pp := `<?xml version="1.0"?><D:propertyupdate xmlns:D="DAV:" xmlns:S="http://subversion.tigris.org/xmlns/svn/">` +
		`<D:set><D:prop><S:log>msg</S:log></D:prop></D:set></D:propertyupdate>`
	resp = doRequest(t, "PROPPATCH", txn, pp, user, pass, nil)
	if resp.StatusCode != http.StatusMultiStatus {
		t.Fatalf("root PROPPATCH status=%d body=%s", resp.StatusCode, bodyOf(t, resp))
	}
	resp.Body.Close()

	resp = doRequest(t, "PUT", txn+"/trunk/new.txt", "data", user, pass, nil)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT trunk status=%d body=%s", resp.StatusCode, bodyOf(t, resp))
	}
	resp.Body.Close()

	resp = doRequest(t, "PUT", txn+"/secret/x.txt", "data", user, pass, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("PUT secret status=%d, want 403", resp.StatusCode)
	}
	resp.Body.Close()

	resp = doRequest(t, "DELETE", base+"/svn/demo/!svn/act/u-1", "", user, pass, nil)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("activity DELETE status=%d", resp.StatusCode)
	}
	resp.Body.Close()
}

// TestAuthzCommitRechecksCopyDestination: COPY is authorized against its
// source, so a transaction could carry a change under a path the client never
// addressed (server-side copy). MERGE must re-check every changed path.
func TestAuthzCommitRechecksCopyDestination(t *testing.T) {
	authz := "[/]\nalice = r\n[/trunk]\nalice = rw\n"
	ts, _, base := testServer(t, "alice:"+shaPass("pw")+"\n", authz)
	defer ts.Close()
	user, pass := "alice", "pw"

	startTxn := func(act string) string {
		resp := doRequest(t, "MKACTIVITY", base+"/svn/demo/!svn/act/"+act, "", user, pass, nil)
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("MKACTIVITY status=%d", resp.StatusCode)
		}
		resp.Body.Close()
		body := `<?xml version="1.0"?><D:checkout xmlns:D="DAV:"><D:activity-set><D:href>` +
			base + `/svn/demo/!svn/act/` + act + `</D:href></D:activity-set></D:checkout>`
		resp = doRequest(t, "CHECKOUT", base+"/svn/demo/!svn/vcc/default", body, user, pass, nil)
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("CHECKOUT status=%d", resp.StatusCode)
		}
		loc := strings.TrimSuffix(resp.Header.Get("Location"), "/")
		resp.Body.Close()
		return loc
	}
	merge := func(act string) *http.Response {
		body := `<?xml version="1.0"?><D:merge xmlns:D="DAV:"><D:source><D:href>` +
			base + `/svn/demo/!svn/act/` + act + `</D:href></D:source></D:merge>`
		return doRequest(t, "MERGE", base+"/svn/demo/!svn/vcc/default", body, user, pass, nil)
	}

	// Denied destination: the copy request itself only checks the source.
	txn := startTxn("u-2")
	resp := doRequest(t, "COPY", base+"/svn/demo/!svn/bln/1/trunk/README.txt", "", user, pass,
		map[string]string{"Destination": txn + "/secret/leak.txt"})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("COPY status=%d body=%s", resp.StatusCode, bodyOf(t, resp))
	}
	resp.Body.Close()
	resp = merge("u-2")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("MERGE into /secret status=%d, want 403: %s", resp.StatusCode, bodyOf(t, resp))
	}
	resp.Body.Close()

	// Allowed destination: same flow under /trunk commits.
	txn = startTxn("u-3")
	resp = doRequest(t, "COPY", base+"/svn/demo/!svn/bln/1/trunk/README.txt", "", user, pass,
		map[string]string{"Destination": txn + "/trunk/copy.txt"})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("COPY status=%d body=%s", resp.StatusCode, bodyOf(t, resp))
	}
	resp.Body.Close()
	resp = merge("u-3")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("MERGE into /trunk status=%d, want 200: %s", resp.StatusCode, bodyOf(t, resp))
	}
	resp.Body.Close()
}

func TestOptionsAdvertiseChunkParams(t *testing.T) {
	st := store.NewMemoryStore()
	cp := cdc.Params{Min: 128 << 10, Max: 2 << 20, Mask: (1 << 20) - 1}
	repo, err := fs.CreateRepoParams(st, "demo", 0, 0, cp)
	if err != nil {
		t.Fatal(err)
	}
	srv := New(Config{
		BasePath: "/svn",
		OpenRepo: func(name string) (*fs.Repo, error) {
			if name == "demo" {
				return repo, nil
			}
			return nil, fs.ErrRepoNotFound
		},
	})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	resp := doRequest(t, "OPTIONS", ts.URL+"/svn/demo", "", "", "", nil)
	resp.Body.Close()
	want := map[string]string{
		"X-Svngod-Chunk-Min":  "131072",
		"X-Svngod-Chunk-Max":  "2097152",
		"X-Svngod-Chunk-Mask": "1048575",
	}
	for k, v := range want {
		if got := resp.Header.Get(k); got != v {
			t.Fatalf("%s=%q, want %q", k, got, v)
		}
	}
}

func TestPerUserUploadLimit(t *testing.T) {
	s := New(Config{MaxConcurrentUploads: 8, MaxUserUploads: 1})
	req := httptest.NewRequest("PUT", "/", nil)
	w1 := httptest.NewRecorder()
	relA, ok := s.acquireUpload(w1, req, "alice")
	if !ok {
		t.Fatal("first alice upload refused")
	}
	w2 := httptest.NewRecorder()
	if _, ok := s.acquireUpload(w2, req, "alice"); ok {
		t.Fatal("second alice upload allowed")
	}
	if w2.Code != http.StatusTooManyRequests {
		t.Fatalf("status=%d, want 429", w2.Code)
	}
	w3 := httptest.NewRecorder()
	relB, ok := s.acquireUpload(w3, req, "bob")
	if !ok {
		t.Fatal("bob upload refused")
	}
	relA()
	relB()
	w4 := httptest.NewRecorder()
	relA2, ok := s.acquireUpload(w4, req, "alice")
	if !ok {
		t.Fatal("alice refused after release")
	}
	relA2()
}

func TestUploadLimitDisabled(t *testing.T) {
	s := New(Config{})
	req := httptest.NewRequest("PUT", "/", nil)
	var rels []func()
	for i := 0; i < 50; i++ {
		rel, ok := s.acquireUpload(httptest.NewRecorder(), req, "alice")
		if !ok {
			t.Fatal("unlimited uploads refused")
		}
		rels = append(rels, rel)
	}
	for _, rel := range rels {
		rel()
	}
}

func TestMetricsEndpoint(t *testing.T) {
	ts, _, base := testServer(t, "", "")
	defer ts.Close()
	resp := doRequest(t, "OPTIONS", base+"/svn/demo", "", "", "", nil)
	resp.Body.Close()
	mr, err := http.Get(ts.URL + "/_metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Body.Close()
	b, _ := io.ReadAll(mr.Body)
	body := string(b)
	for _, want := range []string{
		`svngod_http_requests_total{method="OPTIONS"`,
		"svngod_http_request_duration_seconds_sum",
		"svngod_http_request_bytes_total",
		"svngod_upload_inflight",
		"svngod_upload_limit",
		"svngod_commits_total",
		"svngod_bundle_uploads_total",
		"svngod_bundle_replays_total",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}
}

func TestMetricsDisabled(t *testing.T) {
	srv := New(Config{DisableMetrics: true})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/_metrics")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d, want 404", resp.StatusCode)
	}
}

func TestMetricsCountsCommitsAndReplays(t *testing.T) {
	ts, repo := newBundleTestServer(t)
	defer ts.Close()
	b := &testBundle{Token: strings.Repeat("9a", 16), Message: "m"}
	body := b.body("hello")
	if code, out := postBundle(t, ts, body); code != http.StatusOK {
		t.Fatalf("upload status=%d out=%v", code, out)
	}
	// Same token again: replay.
	if code, out := postBundle(t, ts, body); code != http.StatusOK {
		t.Fatalf("replay status=%d out=%v", code, out)
	}
	head, _ := repo.HeadFresh()
	if head != 1 {
		t.Fatalf("head=%d", head)
	}
	resp, err := http.Get(ts.URL + "/_metrics")
	if err != nil {
		t.Fatal(err)
	}
	bts, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	bodyStr := string(bts)
	if !strings.Contains(bodyStr, "svngod_commits_total 1") {
		t.Fatalf("commits metric wrong:\n%s", bodyStr)
	}
	if !strings.Contains(bodyStr, "svngod_bundle_uploads_total 1") {
		t.Fatalf("uploads metric wrong:\n%s", bodyStr)
	}
	if !strings.Contains(bodyStr, "svngod_bundle_replays_total 1") {
		t.Fatalf("replays metric wrong:\n%s", bodyStr)
	}
}

func hookServer(t *testing.T, script string) (*httptest.Server, *fs.Repo) {
	t.Helper()
	repo := buildRepo(t)
	dir := t.TempDir()
	if script != "" {
		if err := os.WriteFile(filepath.Join(dir, "pre-revprop-change"), []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	srv := New(Config{
		BasePath: "/svn",
		HooksDir: dir,
		OpenRepo: func(name string) (*fs.Repo, error) {
			if name == "demo" {
				return repo, nil
			}
			return nil, fs.ErrRepoNotFound
		},
	})
	return httptest.NewServer(srv.Handler()), repo
}

const revPropBody = `<?xml version="1.0" encoding="utf-8"?>` +
	`<D:propertyupdate xmlns:D="DAV:" xmlns:S="http://subversion.tigris.org/xmlns/svn/">` +
	`<D:set><D:prop><S:log>edited by test</S:log></D:prop></D:set>` +
	`</D:propertyupdate>`

func TestRevPropChangeRequiresHook(t *testing.T) {
	ts, repo := hookServer(t, "")
	defer ts.Close()
	resp := doRequest(t, "PROPPATCH", ts.URL+"/svn/demo/!svn/rev/1", revPropBody, "", "", nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status=%d, want 403", resp.StatusCode)
	}
	body := bodyOf(t, resp)
	if !strings.Contains(body, "165006") {
		t.Fatalf("missing disabled-feature errcode: %s", body)
	}
	if rp, _ := repo.GetRevProps(1); rp.Props["svn:log"] != "initial import" {
		t.Fatalf("log changed without hook: %q", rp.Props["svn:log"])
	}
}

func TestRevPropChangeWithHook(t *testing.T) {
	ts, repo := hookServer(t, "#!/bin/sh\nexit 0\n")
	defer ts.Close()
	resp := doRequest(t, "PROPPATCH", ts.URL+"/svn/demo/!svn/rev/1", revPropBody, "", "", nil)
	if resp.StatusCode != http.StatusMultiStatus {
		t.Fatalf("status=%d body=%s", resp.StatusCode, bodyOf(t, resp))
	}
	resp.Body.Close()
	if rp, _ := repo.GetRevProps(1); rp.Props["svn:log"] != "edited by test" {
		t.Fatalf("log=%q", rp.Props["svn:log"])
	}

	remove := `<?xml version="1.0"?><D:propertyupdate xmlns:D="DAV:" xmlns:S="http://subversion.tigris.org/xmlns/svn/">` +
		`<D:remove><D:prop><S:log/></D:prop></D:remove></D:propertyupdate>`
	resp = doRequest(t, "PROPPATCH", ts.URL+"/svn/demo/!svn/rev/1", remove, "", "", nil)
	if resp.StatusCode != http.StatusMultiStatus {
		t.Fatalf("remove status=%d body=%s", resp.StatusCode, bodyOf(t, resp))
	}
	resp.Body.Close()
	if rp, _ := repo.GetRevProps(1); rp.Props["svn:log"] != "" {
		t.Fatalf("log not removed: %q", rp.Props["svn:log"])
	}
}

func TestRevPropChangeHookCanReject(t *testing.T) {
	ts, repo := hookServer(t, "#!/bin/sh\necho denied by policy >&2\nexit 1\n")
	defer ts.Close()
	resp := doRequest(t, "PROPPATCH", ts.URL+"/svn/demo/!svn/rev/1", revPropBody, "", "", nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status=%d, want 403", resp.StatusCode)
	}
	body := bodyOf(t, resp)
	if !strings.Contains(body, "denied by policy") {
		t.Fatalf("hook message missing: %s", body)
	}
	if rp, _ := repo.GetRevProps(1); rp.Props["svn:log"] != "initial import" {
		t.Fatalf("log changed despite rejection: %q", rp.Props["svn:log"])
	}
}

func TestMetricsMirrorStatus(t *testing.T) {
	a, b := store.NewMemoryStore(), store.NewMemoryStore()
	ms := store.NewMirror([]store.Store{a, b})
	defer ms.Close()
	repo, err := fs.CreateRepo(ms, "demo", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	srv := New(Config{
		BasePath: "/svn",
		OpenRepo: func(name string) (*fs.Repo, error) {
			if name == "demo" {
				return repo, nil
			}
			return nil, fs.ErrRepoNotFound
		},
	})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	// Populate the server's repo cache (the response itself does not matter).
	http.Get(ts.URL + "/svn/demo")
	resp, err := http.Get(ts.URL + "/_metrics")
	if err != nil {
		t.Fatal(err)
	}
	bts, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	body := string(bts)
	if !strings.Contains(body, `svngod_mirror_healthy{repo="demo",replica="0"} 1`) {
		t.Fatalf("mirror metric missing:\n%s", body)
	}
	if !strings.Contains(body, `svngod_mirror_primary{repo="demo",replica="0"} 1`) {
		t.Fatalf("primary metric missing:\n%s", body)
	}
}

// TestMetricsCommitHistogram checks the commit latency histogram and the
// failure-reason counter are exposed in the Prometheus text.
func TestMetricsCommitHistogram(t *testing.T) {
	repo := buildRepo(t)
	srv := New(Config{
		BasePath: "/svn",
		OpenRepo: func(name string) (*fs.Repo, error) {
			if name == "demo" {
				return repo, nil
			}
			return nil, fs.ErrRepoNotFound
		},
	})
	srv.m.observeCommit(120 * time.Millisecond)
	srv.m.observeCommit(2 * time.Second)
	srv.m.noteCommitError("out_of_date")
	srv.m.noteCommitError("lease_busy")
	rec := httptest.NewRecorder()
	srv.serveMetrics(rec)
	body := rec.Body.String()
	for _, want := range []string{
		`svngod_commit_duration_seconds_bucket{le="0.25"} 1`,
		`svngod_commit_duration_seconds_bucket{le="2.5"} 2`,
		`svngod_commit_duration_seconds_bucket{le="+Inf"} 2`,
		`svngod_commit_duration_seconds_count 2`,
		`svngod_commit_errors_total{reason="lease_busy"} 1`,
		`svngod_commit_errors_total{reason="out_of_date"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}
}
