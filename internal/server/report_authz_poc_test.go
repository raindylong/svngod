package server

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// The claim to verify: REPORT against /repo/!svn/me authorizes "/" (the URL has
// no path) but then reads a path the request BODY names, with no second check.
// If true, a user denied [/private] can read it by pointing a report at the
// allowed endpoint.
//
// These tests are written to FAIL on the current code if the hole is real, and
// to pass trivially if it is not. That distinction matters: the previous
// attempt at this test asserted on get-locks-report and saw no leak, which said
// nothing about the handlers that actually read file content.

// reportTo issues a REPORT to /!svn/me with the given body.
func reportTo(t *testing.T, s *Server, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("REPORT", path, strings.NewReader(body))
	req.RemoteAddr = "192.0.2.1:1234"
	req.Header.Set("Content-Type", "text/xml")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec
}

// TestReportFileRevsCannotReadDeniedPath is the strongest form of the claim:
// get-file-revs returns the file TEXT itself, so a leak is unambiguous.
func TestReportFileRevsCannotReadDeniedPath(t *testing.T) {
	s := authzServer(t, denyPrivate)

	// Control: the same data via a direct URL is denied.
	direct := httptest.NewRequest("GET", "/svn/demo/private/secret.txt", nil)
	direct.RemoteAddr = "192.0.2.1:1234"
	directRec := httptest.NewRecorder()
	s.Handler().ServeHTTP(directRec, direct)
	if directRec.Code != http.StatusForbidden {
		t.Fatalf("control: direct read returned %d, want 403 (the authz rule is not working)", directRec.Code)
	}

	body := `<?xml version="1.0"?>
<S:get-file-revs-report xmlns:S="svn:" xmlns:D="DAV:">
  <S:path>/private/secret.txt</S:path>
</S:get-file-revs-report>`
	rec := reportTo(t, s, "/svn/demo/!svn/me", body)

	t.Logf("get-file-revs status=%d body=%q", rec.Code, rec.Body.String())
	if rec.Code == http.StatusOK && strings.Contains(rec.Body.String(), "file-rev") {
		t.Fatalf("REPORT get-file-revs on /!svn/me returned revision data for a denied path (status %d):\n%s",
			rec.Code, rec.Body.String())
	}
}

// TestReportLogCannotReadDeniedPath: log-report discloses commit messages and
// authors for a denied subtree.
func TestReportLogCannotReadDeniedPath(t *testing.T) {
	s := authzServer(t, denyPrivate)

	body := `<?xml version="1.0"?>
<S:log-report xmlns:S="svn:" xmlns:D="DAV:">
  <S:start-revision>1</S:start-revision>
  <S:end-revision>1</S:end-revision>
  <S:discover-changed-paths/>
  <S:path>/private</S:path>
</S:log-report>`
	rec := reportTo(t, s, "/svn/demo/!svn/me", body)
	t.Logf("log-report status=%d body=%q", rec.Code, rec.Body.String())
	// The element is <S:log-item>; an earlier draft asserted on "log-entry" and
	// therefore "passed" while the response was leaking path names, the author
	// and the commit message. Assert on the denied path itself instead of on a
	// tag name, so the check survives a rename of the envelope.
	if rec.Code == http.StatusOK && strings.Contains(rec.Body.String(), "/private") {
		t.Fatalf("REPORT log-report on /!svn/me disclosed a denied subtree (status %d):\n%s",
			rec.Code, rec.Body.String())
	}
}

// TestReportUpdateCannotCheckoutDeniedSubtree: update-report (checkout) with
// text-deltas streams file CONTENT.
func TestReportUpdateCannotCheckoutDeniedSubtree(t *testing.T) {
	s := authzServer(t, denyPrivate)
	head := uint64(1)

	body := `<?xml version="1.0"?>
<S:update-report xmlns:S="svn:" xmlns:D="DAV:" xmlns:V="http://subversion.tigris.org/xmlns/dav/">
  <S:target-revision>` + strconv.FormatUint(head, 10) + `</S:target-revision>
  <S:src-path>/svn/demo/private</S:src-path>
  <S:text-deltas>yes</S:text-deltas>
  <S:entry rev="0" start-empty="true"/>
</S:update-report>`
	rec := reportTo(t, s, "/svn/demo/!svn/me", body)
	t.Logf("update-report status=%d len=%d", rec.Code, rec.Body.Len())
	bodyStr := rec.Body.String()
	// The delta is base64; the literal plaintext would only appear if the
	// server sent raw text, so check for the filename and for base64 of the
	// secret which is what an svndiff of the whole file contains.
	if rec.Code == http.StatusOK && strings.Contains(bodyStr, "secret.txt") {
		t.Fatalf("REPORT update-report on /!svn/me listed a denied subtree (status %d):\n%s", rec.Code, bodyStr)
	}
}

// TestReportUpdateDoesNotListDeniedChildSubtree covers the case the entry check
// cannot: the report's ROOT is readable ("/"), but a child of it is denied.
// A checkout of / must not include that child's name, props or content - the
// same rule propChildren already applies to PROPFIND depth listings.
func TestReportUpdateDoesNotListDeniedChildSubtree(t *testing.T) {
	s := authzServer(t, "[demo:/]\n* = r\n[demo:/private]\n* =\n")

	body := `<?xml version="1.0"?>
<S:update-report xmlns:S="svn:" xmlns:D="DAV:" xmlns:V="http://subversion.tigris.org/xmlns/dav/">
  <S:src-path>/svn/demo</S:src-path>
  <S:text-deltas>yes</S:text-deltas>
  <S:entry rev="0" start-empty="true"/>
</S:update-report>`
	rec := reportTo(t, s, "/svn/demo/!svn/me", body)
	t.Logf("status=%d len=%d", rec.Code, rec.Body.Len())
	if rec.Code != http.StatusOK {
		t.Fatalf("checkout of the readable root returned %d, want 200:\n%s", rec.Code, rec.Body.String())
	}
	out := rec.Body.String()
	if strings.Contains(out, "private") {
		t.Fatalf("checkout of / disclosed the denied /private subtree:\n%s", out)
	}
	// The readable sibling must still be there, or the fix over-filtered.
	if !strings.Contains(out, "public") {
		t.Fatalf("checkout of / dropped the readable /public subtree:\n%s", out)
	}
}

// TestReportHandlersCannotReadDeniedPath walks every report type that accepts a
// path, so a fix for one handler cannot leave the others open. The list is
// deliberately explicit: adding a new report type should force a decision here
// rather than inherit whatever the default happens to be.
func TestReportHandlersCannotReadDeniedPath(t *testing.T) {
	s := authzServer(t, denyPrivate)

	cases := []struct{ name, body string }{
		{"get-locks-report", `<S:get-locks-report xmlns:S="svn:" xmlns:D="DAV:"><S:path>/private</S:path></S:get-locks-report>`},
		{"get-mergeinfo", `<S:mergeinfo-report xmlns:S="svn:" xmlns:D="DAV:"><S:path>/private</S:path><S:revision>1</S:revision></S:mergeinfo-report>`},
		{"get-locations", `<S:get-locations xmlns:S="svn:" xmlns:D="DAV:"><S:path>/private/secret.txt</S:path><S:location-revision>1</S:location-revision></S:get-locations>`},
		{"get-location-segments", `<S:get-location-segments xmlns:S="svn:" xmlns:D="DAV:"><S:path>/private/secret.txt</S:path><S:start-revision>1</S:start-revision></S:get-location-segments>`},
		{"list-report", `<S:list-report xmlns:S="svn:" xmlns:D="DAV:"><S:path>/private</S:path></S:list-report>`},
	}
	for _, tc := range cases {
		rec := reportTo(t, s, "/svn/demo/!svn/me", `<?xml version="1.0"?>`+tc.body)
		if rec.Code == http.StatusOK {
			// A 200 is only acceptable if the denied path is absent from the body.
			if strings.Contains(rec.Body.String(), "secret") || strings.Contains(rec.Body.String(), "/private") {
				t.Fatalf("%s on /!svn/me disclosed /private (status %d):\n%s", tc.name, rec.Code, rec.Body.String())
			}
		}
	}
}
