package server

import (
	"net/http"
	"strings"
	"testing"
)

func commitRequest(t *testing.T, base, repo, uuid string) {
	actBase := base + "/svn/" + repo

	// 1. MKACTIVITY
	resp := doRequest(t, "MKACTIVITY", actBase+"/!svn/act/"+uuid, "", "", "", nil)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("MKACTIVITY status=%d", resp.StatusCode)
	}
	resp.Body.Close()

	// 2. PROPPATCH the activity: set svn:log / author / date
	pp := `<?xml version="1.0"?>` +
		`<D:propertyupdate xmlns:D="DAV:" xmlns:V="http://subversion.tigris.org/xmlns/svn/">` +
		`<D:set><D:prop><V:log>my commit</V:log><V:author>alice</V:author></D:prop></D:set>` +
		`</D:propertyupdate>`
	resp = doRequest(t, "PROPPATCH", actBase+"/!svn/act/"+uuid, pp, "", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PROPPATCH activity status=%d body=%s", resp.StatusCode, bodyOf(t, resp))
	}
	resp.Body.Close()

	// 3. CHECKOUT the vcc into the activity
	resp = doRequest(t, "CHECKOUT", actBase+"/!svn/vcc/default", "", "", "", map[string]string{
		"Destination": actBase + "/!svn/act/" + uuid + "/",
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("CHECKOUT status=%d body=%s", resp.StatusCode, bodyOf(t, resp))
	}
	loc := resp.Header.Get("Location")
	if !strings.Contains(loc, "/!svn/txn/") {
		t.Fatalf("CHECKOUT Location=%q missing !svn/txn/", loc)
	}
	resp.Body.Close()
	txnRoot := strings.TrimSuffix(loc, "/")


	// 4. PUT a modified file
	resp = doRequest(t, "PUT", txnRoot+"/trunk/README.txt", "hello svn v2\n", "", "", nil)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT status=%d", resp.StatusCode)
	}
	resp.Body.Close()

	// 5. PUT a new file
	resp = doRequest(t, "PUT", txnRoot+"/trunk/new.txt", "brand new\n", "", "", nil)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT new status=%d", resp.StatusCode)
	}
	resp.Body.Close()

	// 6. DELETE an existing file
	resp = doRequest(t, "DELETE", txnRoot+"/trunk/doc/guide.txt", "", "", "", nil)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status=%d", resp.StatusCode)
	}
	resp.Body.Close()

	// 7. MERGE the activity into the vcc
	merge := `<?xml version="1.0"?>` +
		`<D:merge xmlns:D="DAV:">` +
		`<D:source><D:href>` + actBase + `/!svn/act/` + uuid + `/</D:href></D:source>` +
		`</D:merge>`
	resp = doRequest(t, "MERGE", actBase+"/!svn/vcc/default", merge, "", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("MERGE status=%d body=%s", resp.StatusCode, bodyOf(t, resp))
	}
	out := bodyOf(t, resp)
	if !strings.Contains(out, "merge-response") {
		t.Fatalf("MERGE missing merge-response:\n%s", out)
	}
}

func TestCommitFlow(t *testing.T) {
	ts, repo, base := testServer(t, "", "")
	defer ts.Close()
	before, _ := repo.Head()

	commitRequest(t, base, "demo", "commit-1")

	after, _ := repo.Head()
	if after != before+1 {
		t.Fatalf("head=%d want %d", after, before+1)
	}

	// verify: README updated, new.txt added, guide.txt deleted, revprops set
	nr, err := repo.Resolve(after, "/trunk/README.txt")
	if err != nil {
		t.Fatal(err)
	}
	txt, _ := repo.GetText(nr.Node)
	if string(txt) != "hello svn v2\n" {
		t.Fatalf("README=%q", txt)
	}
	if _, err := repo.Resolve(after, "/trunk/new.txt"); err != nil {
		t.Fatal("new.txt not created:", err)
	}
	if _, err := repo.Resolve(after, "/trunk/doc/guide.txt"); err == nil {
		t.Fatal("guide.txt should be deleted")
	}
	rp, err := repo.GetRevProps(after)
	if err != nil || rp.Props["svn:log"] != "my commit" || rp.Props["svn:author"] != "alice" {
		t.Fatalf("revprops=%+v err=%v", rp, err)
	}

	// the committed change must be readable through the DAV read path too
	resp := doRequest(t, "GET", base+"/svn/demo/trunk/new.txt", "", "", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("GET new.txt status=%d", resp.StatusCode)
	}
	if bodyOf(t, resp) != "brand new\n" {
		t.Fatal("GET new.txt content mismatch")
	}
}


// copyMoveSetup drives MKACTIVITY→PROPPATCH→CHECKOUT and returns the txn root URL.
func copyMoveSetup(t *testing.T, base, repo, uuid string) string {
	actBase := base + "/svn/" + repo
	resp := doRequest(t, "MKACTIVITY", actBase+"/!svn/act/"+uuid, "", "", "", nil)
	if resp.StatusCode != 201 {
		t.Fatalf("MKACTIVITY=%d", resp.StatusCode)
	}
	resp.Body.Close()
	pp := `<?xml version="1.0"?><D:propertyupdate xmlns:D="DAV:" xmlns:V="http://subversion.tigris.org/xmlns/svn/"><D:set><D:prop><V:log>cp mv</V:log><V:author>alice</V:author></D:prop></D:set></D:propertyupdate>`
	resp = doRequest(t, "PROPPATCH", actBase+"/!svn/act/"+uuid, pp, "", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("PROPPATCH=%d", resp.StatusCode)
	}
	resp.Body.Close()
	resp = doRequest(t, "CHECKOUT", actBase+"/!svn/vcc/default", "", "", "", map[string]string{
		"Destination": actBase + "/!svn/act/" + uuid + "/",
	})
	if resp.StatusCode != 201 {
		t.Fatalf("CHECKOUT=%d", resp.StatusCode)
	}
	loc := resp.Header.Get("Location")
	resp.Body.Close()
	return strings.TrimSuffix(loc, "/")
}

func TestCommitCopyFromBaseline(t *testing.T) {
	ts, repo, base := testServer(t, "", "")
	defer ts.Close()
	before, _ := repo.Head() // rev 1

	txnRoot := copyMoveSetup(t, base, "demo", "copy-1")

	// COPY a directory from baseline rev1 into the txn as /branches/br1
	dst := txnRoot + "/branches/br1"
	resp := doRequest(t, "COPY", base+"/svn/demo/!svn/bc/1/trunk", "", "", "", map[string]string{
		"Destination": dst, "Overwrite": "F",
	})
	if resp.StatusCode != 201 {
		t.Fatalf("COPY dir=%d body=%s", resp.StatusCode, bodyOf(t, resp))
	}
	resp.Body.Close()

	// MERGE
	merge := `<?xml version="1.0"?><D:merge xmlns:D="DAV:"><D:source><D:href>` +
		base + `/svn/demo/!svn/act/copy-1/</D:href></D:source></D:merge>`
	resp = doRequest(t, "MERGE", base+"/svn/demo/!svn/vcc/default", merge, "", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("MERGE=%d body=%s", resp.StatusCode, bodyOf(t, resp))
	}
	resp.Body.Close()

	after, _ := repo.Head()
	if after != before+1 {
		t.Fatalf("head=%d want %d", after, before+1)
	}
	top, err := repo.Resolve(after, "/branches/br1")
	if err != nil {
		t.Fatal(err)
	}
	if top.CopyFrom == nil || top.CopyFromPath != "/trunk" || top.CopyFrom.Rev != 1 {
		t.Fatalf("branch root copyfrom wrong: %+v path=%q", top.CopyFrom, top.CopyFromPath)
	}
	nr, err := repo.Resolve(after, "/branches/br1/README.txt")
	if err != nil {
		t.Fatalf("branch file missing: %v", err)
	}
	txt, _ := repo.GetText(nr.Node)
	if string(txt) != "hello svn\n" {
		t.Fatalf("branch README=%q", txt)
	}
}

func TestCommitMoveWithinTxn(t *testing.T) {
	ts, repo, base := testServer(t, "", "")
	defer ts.Close()

	txnRoot := copyMoveSetup(t, base, "demo", "move-1")

	// COPY file from baseline, then MOVE it to a new name within the txn
	resp := doRequest(t, "COPY", base+"/svn/demo/!svn/bc/1/trunk/README.txt", "", "", "", map[string]string{
		"Destination": txnRoot + "/trunk/moved.txt", "Overwrite": "F",
	})
	if resp.StatusCode != 201 {
		t.Fatalf("COPY file=%d", resp.StatusCode)
	}
	resp.Body.Close()
	resp = doRequest(t, "MOVE", txnRoot+"/trunk/moved.txt", "", "", "", map[string]string{
		"Destination": txnRoot + "/trunk/renamed.txt", "Overwrite": "F",
	})
	if resp.StatusCode != 201 {
		t.Fatalf("MOVE=%d body=%s", resp.StatusCode, bodyOf(t, resp))
	}
	resp.Body.Close()

	merge := `<?xml version="1.0"?><D:merge xmlns:D="DAV:"><D:source><D:href>` +
		base + `/svn/demo/!svn/act/move-1/</D:href></D:source></D:merge>`
	resp = doRequest(t, "MERGE", base+"/svn/demo/!svn/vcc/default", merge, "", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("MERGE=%d", resp.StatusCode)
	}
	resp.Body.Close()

	head, _ := repo.Head()
	if _, err := repo.Resolve(head, "/trunk/renamed.txt"); err != nil {
		t.Fatal("renamed.txt missing:", err)
	}
	if _, err := repo.Resolve(head, "/trunk/moved.txt"); err == nil {
		t.Fatal("moved.txt should be gone after MOVE")
	}
}
