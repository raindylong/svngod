package server

import (
	"strconv"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"svngod/internal/auth"
	"svngod/internal/authz"
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
		in      string
		typ     resourceType
		repo    string
		rev     uint64
		path    string
		uuid    string
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
