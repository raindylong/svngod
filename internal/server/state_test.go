package server

import (
	"net/http/httptest"
	"strings"
	"testing"

	"svngod/internal/fs"
	"svngod/internal/fs/store"
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
