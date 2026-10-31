package server

import (
	"bytes"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"svngod/internal/fs"
	"svngod/internal/svndiff"
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

	// 4. PUT a modified file (returns 204; the path exists at the base rev)
	resp = doRequest(t, "PUT", txnRoot+"/trunk/README.txt", "hello svn v2\n", "", "", nil)
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
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

// TestLargeBaseSmallDeltaIsStoredAsDelta covers the case that used to store a
// full copy: a small svndiff against a base larger than the stream threshold
// must be decoded window-by-window and kept as a delta, and a second edit must
// chain (depth 2) rather than falling back to a full text.
func TestLargeBaseSmallDeltaIsStoredAsDelta(t *testing.T) {
	ts, repo, _ := testServer(t, "", "")
	defer ts.Close()
	repoURL := ts.URL + "/svn/demo"

	base := make([]byte, 3<<20)
	for i := range base {
		base[i] = byte(i * 7)
	}
	// r2: the large full text.
	if _, err := repo.CommitTxn(0, []fs.Change{{
		Path: "/big.bin", Action: fs.ActionAdd, Kind: fs.KindFile, Data: base,
	}}, map[string]string{"svn:log": "base", "svn:author": "u", "svn:date": "2026-01-01T00:00:00Z"}); err != nil {
		t.Fatal(err)
	}

	var txn string
	cur := base
	put := func(baseRev uint64, text []byte) {
		t.Helper()
		delta, err := svndiff.Encode(cur, text)
		if err != nil {
			t.Fatal(err)
		}
		resp := doRequest(t, "PUT", repoURL+"/!svn/txr/"+txn+"/big.bin", string(delta), "", "",
			map[string]string{"Content-Type": "application/vnd.svn-svndiff",
				"X-SVN-Version-Name": strconv.FormatUint(baseRev, 10)})
		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			t.Fatalf("PUT status %d: %s", resp.StatusCode, b)
		}
		resp.Body.Close()
		merge := `<?xml version="1.0"?><D:merge xmlns:D="DAV:"><D:source><D:href>` +
			repoURL + `/!svn/txn/` + txn + `</D:href></D:source></D:merge>`
		mr := doRequest(t, "MERGE", repoURL, merge, "", "", map[string]string{"Content-Type": "text/xml"})
		mr.Body.Close()
		if mr.StatusCode != http.StatusOK {
			t.Fatalf("MERGE status %d", mr.StatusCode)
		}
		cur = text
	}

	// r3, r4: small edits of the 3 MiB base; each needs a new transaction.
	newTxn := func() {
		t.Helper()
		resp := doRequest(t, "POST", repoURL+"/!svn/me", "(create-txn)", "", "",
			map[string]string{"Content-Type": "application/vnd.svn-skel"})
		resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("create-txn status %d", resp.StatusCode)
		}
		txn = resp.Header.Get("SVN-Txn-Name")
	}

	newTxn()
	edit1 := append(append([]byte(nil), base...), []byte(strings.Repeat("first edit\n", 64))...)
	put(2, edit1)
	newTxn()
	edit2 := append(append([]byte(nil), edit1...), []byte(strings.Repeat("second edit\n", 64))...)
	put(3, edit2)

	nr, err := repo.Resolve(4, "/big.bin")
	if err != nil {
		t.Fatal(err)
	}
	if nr.DeltaDepth != 2 || nr.TextRep != fs.TextRepDelta {
		t.Fatalf("store=%v depth=%d, want delta depth 2", nr.TextRep, nr.DeltaDepth)
	}
	got, err := repo.GetText(nr.Node)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(edit2) || string(got) != string(edit2) {
		t.Fatalf("text mismatch: got %d want %d", len(got), len(edit2))
	}
}

// TestChunkedDeltaPUT covers the direct decode-to-chunks path: a small delta
// against a multi-megabyte chunked base must be committed as a chunked node
// with exactly the client's text.
func TestChunkedDeltaPUT(t *testing.T) {
	ts, repo, _ := testServer(t, "", "")
	defer ts.Close()
	repoURL := ts.URL + "/svn/demo"

	base := make([]byte, 6<<20)
	for i := range base {
		base[i] = byte(i * 13)
	}
	if _, err := repo.CommitTxn(0, []fs.Change{{
		Path: "/big.bin", Action: fs.ActionAdd, Kind: fs.KindFile, Data: base,
	}}, map[string]string{"svn:log": "base", "svn:author": "u", "svn:date": "2026-01-01T00:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	edited := append([]byte(nil), base...)
	for i := 2 << 20; i < 3<<20; i++ {
		edited[i] = byte(i * 7)
	}
	delta, err := svndiff.Encode(base, edited)
	if err != nil {
		t.Fatal(err)
	}
	resp := doRequest(t, "POST", repoURL+"/!svn/me", "(create-txn)", "", "",
		map[string]string{"Content-Type": "application/vnd.svn-skel"})
	resp.Body.Close()
	txn := resp.Header.Get("SVN-Txn-Name")
	if txn == "" {
		t.Fatalf("create-txn status %d", resp.StatusCode)
	}
	resp = doRequest(t, "PUT", repoURL+"/!svn/txr/"+txn+"/big.bin", string(delta), "", "",
		map[string]string{"Content-Type": "application/vnd.svn-svndiff", "X-SVN-Version-Name": "2"})
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("PUT status %d: %s", resp.StatusCode, b)
	}
	resp.Body.Close()
	merge := `<?xml version="1.0"?><D:merge xmlns:D="DAV:"><D:source><D:href>` +
		repoURL + `/!svn/txn/` + txn + `</D:href></D:source></D:merge>`
	resp = doRequest(t, "MERGE", repoURL, merge, "", "", map[string]string{"Content-Type": "text/xml"})
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("MERGE status %d", resp.StatusCode)
	}
	nr, err := repo.Resolve(3, "/big.bin")
	if err != nil {
		t.Fatal(err)
	}
	if nr.TextRep != fs.TextRepChunked {
		t.Fatalf("TextRep=%d, want chunked", nr.TextRep)
	}
	got, err := repo.GetText(nr.Node)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, edited) {
		t.Fatalf("committed text differs from client (len %d vs %d)", len(got), len(edited))
	}
}
