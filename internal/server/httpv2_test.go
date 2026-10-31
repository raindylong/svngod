package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"svngod/internal/fs"
)

func TestSkelParse(t *testing.T) {
	cases := []struct {
		in   string
		want []any
	}{
		{"(create-txn)", []any{"create-txn"}},
		{"(svn:log 5 hello)", []any{"svn:log", "hello"}},
		{"(svn:log 5:hello)", []any{"svn:log", "hello"}},
		{"(a 11 hello world b bare)", []any{"a", "hello world", "b", "bare"}},
	}
	for _, c := range cases {
		got, err := parseSkel([]byte(c.in))
		if err != nil {
			t.Fatalf("%q: %v", c.in, err)
		}
		list, ok := got.([]any)
		if !ok || len(list) != len(c.want) {
			t.Fatalf("%q: got %#v", c.in, got)
		}
		for i := range c.want {
			if list[i] != c.want[i] {
				t.Fatalf("%q[%d]: got %#v want %#v", c.in, i, list[i], c.want[i])
			}
		}
	}
}

// TestHTTPv2Commit exercises the transaction channel the way ra_serf does:
// OPTIONS advertises v2, the client POSTs a skel create-txn, uploads into the
// txr stub and finalises with MERGE naming the txn URL.
func TestHTTPv2Commit(t *testing.T) {
	t.Setenv("SVNGOD_HTTPV2", "1")
	ts, repo, _ := testServer(t, "alice:"+shaPass("secret")+"\n", "")
	defer ts.Close()

	resp := doRequest(t, "OPTIONS", ts.URL+"/svn/demo",
		`<D:options xmlns:D="DAV:"><D:activity-collection-set/></D:options>`,
		"alice", "secret", nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("OPTIONS status %d", resp.StatusCode)
	}
	// Stub values must be canonical svn paths (no trailing slash): ra_serf
	// asserts on that before it will use the transaction channel.
	headers := map[string]string{
		"SVN-Repository-Root": "/svn/demo",
		"SVN-Me-Resource":     "/svn/demo/!svn/me",
		"SVN-Rev-Root-Stub":   "/svn/demo/!svn/rvr",
		"SVN-Rev-Stub":        "/svn/demo/!svn/rev",
		"SVN-Txn-Root-Stub":   "/svn/demo/!svn/txr",
		"SVN-Txn-Stub":        "/svn/demo/!svn/txn",
	}
	for k, want := range headers {
		if got := resp.Header.Get(k); got != want {
			t.Fatalf("%s = %q, want %q", k, got, want)
		}
	}

	resp = doRequest(t, "POST", ts.URL+"/svn/demo/!svn/me",
		"(create-txn-with-props (svn:log 5 hello))", "alice", "secret",
		map[string]string{"Content-Type": "application/vnd.svn-skel"})
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create-txn status %d", resp.StatusCode)
	}
	txn := resp.Header.Get("SVN-Txn-Name")
	if !strings.HasPrefix(txn, "txn-") {
		t.Fatalf("SVN-Txn-Name = %q", txn)
	}

	resp = doRequest(t, "PUT", ts.URL+"/svn/demo/!svn/txr/"+txn+"/trunk/v2.txt",
		"hello v2\n", "alice", "secret", map[string]string{"Content-Type": "text/plain"})
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT status %d", resp.StatusCode)
	}

	merge := `<?xml version="1.0"?><D:merge xmlns:D="DAV:"><D:source><D:href>` +
		ts.URL + `/svn/demo/!svn/txn/` + txn + `</D:href></D:source></D:merge>`
	resp = doRequest(t, "MERGE", ts.URL+"/svn/demo", merge, "alice", "secret",
		map[string]string{"Content-Type": "text/xml"})
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("MERGE status %d", resp.StatusCode)
	}

	head, err := repo.HeadFresh()
	if err != nil || head != 2 {
		t.Fatalf("head=%d err=%v", head, err)
	}
	nr, err := repo.Resolve(2, "/trunk/v2.txt")
	if err != nil {
		t.Fatal(err)
	}
	text, err := repo.GetText(nr.Node)
	if err != nil || string(text) != "hello v2\n" {
		t.Fatalf("text=%q err=%v", text, err)
	}
	rp, _ := repo.GetRevProps(2)
	if rp == nil || rp.Props["svn:log"] != "hello" {
		t.Fatalf("revprops=%v", rp)
	}
	if rp.Props["svn:author"] != "alice" {
		t.Fatalf("author=%q", rp.Props["svn:author"])
	}
}

// TestHTTPv2Disabled keeps v1 clients unaffected: no v2 headers, and the
// transaction endpoint stays closed.
func TestHTTPv2Disabled(t *testing.T) {
	t.Setenv("SVNGOD_HTTPV2", "0")
	ts, _, _ := testServer(t, "alice:"+shaPass("secret")+"\n", "")
	defer ts.Close()
	resp := doRequest(t, "OPTIONS", ts.URL+"/svn/demo",
		`<D:options xmlns:D="DAV:"><D:activity-collection-set/></D:options>`,
		"alice", "secret", nil)
	resp.Body.Close()
	if got := resp.Header.Get("SVN-Repository-Root"); got != "" {
		t.Fatalf("v1 OPTIONS sent SVN-Repository-Root %q", got)
	}
	resp = doRequest(t, "POST", ts.URL+"/svn/demo/!svn/me", "(create-txn)",
		"alice", "secret", map[string]string{"Content-Type": "application/vnd.svn-skel"})
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("disabled POST status %d", resp.StatusCode)
	}
}

var _ = fs.KindFile

// TestCrossReplicaTransactionRestore covers a load-balanced commit: replica B
// had already loaded its activity list before replica A created the
// transaction, so the later PUT/MERGE on B must restore the session on demand
// from the shared store instead of answering 409.
func TestCrossReplicaTransactionRestore(t *testing.T) {
	repo := buildRepo(t)
	open := func(name string) (*fs.Repo, error) { return repo, nil }
	a := New(Config{BasePath: "/svn", OpenRepo: open})
	b := New(Config{BasePath: "/svn", OpenRepo: open})
	ta := httptest.NewServer(a.Handler())
	tb := httptest.NewServer(b.Handler())
	defer ta.Close()
	defer tb.Close()

	// B sees the repository before the transaction exists (stateLoaded=true).
	resp := doRequest(t, "PROPFIND", tb.URL+"/svn/demo/!svn/vcc/default",
		`<?xml version="1.0"?><propfind xmlns="DAV:"><prop><version-name/></prop></propfind>`,
		"", "", map[string]string{"Depth": "0"})
	resp.Body.Close()

	resp = doRequest(t, "POST", ta.URL+"/svn/demo/!svn/me",
		"(create-txn-with-props (svn:log 5 cross))", "", "",
		map[string]string{"Content-Type": "application/vnd.svn-skel"})
	resp.Body.Close()
	txn := resp.Header.Get("SVN-Txn-Name")
	if txn == "" {
		t.Fatalf("create-txn status %d, no txn name", resp.StatusCode)
	}

	for _, c := range []struct{ url, path, body string }{
		{ta.URL, "/trunk/fromA.txt", "from A\n"},
		{tb.URL, "/trunk/fromB.txt", "from B\n"},
	} {
		r := doRequest(t, "PUT", c.url+"/svn/demo/!svn/txr/"+txn+c.path, c.body,
			"", "", map[string]string{"Content-Type": "text/plain"})
		r.Body.Close()
		if r.StatusCode != http.StatusCreated && r.StatusCode != http.StatusNoContent {
			t.Fatalf("PUT %s status %d", c.url, r.StatusCode)
		}
	}

	merge := `<?xml version="1.0"?><D:merge xmlns:D="DAV:"><D:source><D:href>` +
		tb.URL + `/svn/demo/!svn/txn/` + txn + `</D:href></D:source></D:merge>`
	resp = doRequest(t, "MERGE", tb.URL+"/svn/demo", merge, "", "",
		map[string]string{"Content-Type": "text/xml"})
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("MERGE on replica B status %d", resp.StatusCode)
	}
	head, _ := repo.HeadFresh()
	if head != 2 {
		t.Fatalf("head=%d, want 2", head)
	}
	for path, want := range map[string]string{
		"/trunk/fromA.txt": "from A\n",
		"/trunk/fromB.txt": "from B\n",
	} {
		nr, err := repo.Resolve(2, path)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		text, _ := repo.GetText(nr.Node)
		if string(text) != want {
			t.Fatalf("%s = %q, want %q", path, text, want)
		}
	}
}
