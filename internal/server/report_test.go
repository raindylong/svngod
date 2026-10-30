package server

import (
	"encoding/base64"
	"encoding/xml"
	"strings"
	"testing"

	"svngod/internal/svndiff"
)

// doReport sends a REPORT to /!svn/me and returns the body.
func doReport(t *testing.T, base, body string) string {
	t.Helper()
	resp := doRequest(t, "REPORT", base+"/svn/demo/!svn/me", body, "", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("REPORT status=%d body=%s", resp.StatusCode, bodyOf(t, resp))
	}
	return bodyOf(t, resp)
}

func TestReportUpdateCheckout(t *testing.T) {
	ts, _, base := testServer(t, "", "")
	defer ts.Close()
	body := `<?xml version="1.0" encoding="utf-8"?>` +
		`<S:update-report xmlns:S="http://subversion.tigris.org/xmlns/dav/" xmlns:V="DAV:">` +
		`<S:src-path>` + base + `/svn/demo/</S:src-path>` +
		`<S:recurse>yes</S:recurse>` +
		`<S:target-revision>1</S:target-revision>` +
		`<S:update-target rev="0"/>` +
		`<S:ignore-ancestry>no</S:ignore-ancestry>` +
		`<S:text-deltas>yes</S:text-deltas>` +
		`</S:update-report>`
	out := doReport(t, base, body)
	for _, want := range []string{
		"S:update-report", `target-revision rev="1"`,
		"S:open-directory rev=\"1\"", "add-directory name=\"trunk\"",
		"add-file name=\"README.txt\"", "D:checked-in", "S:txdelta",
		"V:md5-checksum",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("update-report missing %q:\n%s", want, out)
		}
	}
	// decode the README txdelta and confirm it reconstructs the file
	delta := extractTxdelta(t, out, "README.txt")
	full, err := svndiff.Apply(nil, delta)
	if err != nil {
		t.Fatalf("apply svndiff: %v", err)
	}
	if string(full) != "hello svn\n" {
		t.Fatalf("decoded content=%q want %q", full, "hello svn\n")
	}
}

// extractTxdelta returns the base64-decoded <S:txdelta> payload for a file.
func extractTxdelta(t *testing.T, out, fileName string) []byte {
	t.Helper()
	idx := strings.Index(out, `add-file name="`+fileName+`"`)
	if idx < 0 {
		t.Fatalf("add-file %q not found", fileName)
	}
	seg := out[idx:]
	start := strings.Index(seg, "<S:txdelta>")
	if start < 0 {
		t.Fatalf("txdelta not found for %q", fileName)
	}
	rest := seg[start+len("<S:txdelta>"):]
	end := strings.Index(rest, "</S:txdelta>")
	if end < 0 {
		t.Fatalf("txdelta close not found")
	}
	b64 := rest[:end]
	data, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		t.Fatalf("decode base64: %v", err)
	}
	return data
}

func TestReportLog(t *testing.T) {
	ts, _, base := testServer(t, "", "")
	defer ts.Close()
	body := `<?xml version="1.0" encoding="utf-8"?>` +
		`<S:log-report xmlns:S="http://subversion.tigris.org/xmlns/dav/">` +
		`<S:start-revision>1</S:start-revision>` +
		`<S:end-revision>1</S:end-revision>` +
		`<S:path>/</S:path>` +
		`<S:revprop name="svn:author"/>` +
		`<S:revprop name="svn:log"/>` +
		`<S:discover-changed-paths/>` +
		`</S:log-report>`
	out := doReport(t, base, body)
	for _, want := range []string{
		"S:log-report", "S:log-item", "<D:version-name>1</D:version-name>",
		"D:creator-displayname>alice<", "D:comment>initial import<",
		"S:added-path", "/trunk/README.txt",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("log-report missing %q:\n%s", want, out)
		}
	}
	// validate well-formed XML
	if err := xml.Unmarshal([]byte(out), &xmlEl{}); err != nil {
		t.Fatalf("log-report not well-formed: %v\n%s", err, out)
	}
}
