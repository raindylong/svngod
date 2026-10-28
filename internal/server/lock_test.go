package server

import (
	"net/http"
	"strings"
	"testing"
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
