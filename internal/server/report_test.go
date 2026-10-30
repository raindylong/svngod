package server

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"strings"
	"testing"

	"svngod/internal/fs"
	"svngod/internal/fs/store"
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

// TestLineageFollowsCopyPoint pins the copy-point semantics used by
// get-location-segments: a branch copied in r2 maps to its own path for
// r2..HEAD and only falls back to the copy source for r1 (copyFrom.Rev is the
// source revision, not the revision where the copy happened).
func TestLineageFollowsCopyPoint(t *testing.T) {
	st := store.NewMemoryStore()
	repo, err := fs.CreateRepo(st, "r", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	r1, err := repo.CommitTxn(0, []fs.Change{
		{Path: "/trunk", Action: fs.ActionAdd, Kind: fs.KindDir, Props: map[string]string{}},
		{Path: "/trunk/a.txt", Action: fs.ActionAdd, Kind: fs.KindFile, Data: []byte("a\n")},
	}, map[string]string{"svn:log": "r1"})
	if err != nil {
		t.Fatal(err)
	}
	r2, err := repo.CommitTxn(r1, []fs.Change{
		{Path: "/branch", Action: fs.ActionCopy, Kind: fs.KindDir, CopyFromPath: "/trunk", CopyFromRev: r1},
	}, map[string]string{"svn:log": "r2"})
	if err != nil {
		t.Fatal(err)
	}
	r3, err := repo.CommitTxn(r2, []fs.Change{
		{Path: "/branch/a.txt", Action: fs.ActionModify, Kind: fs.KindFile, Data: []byte("b\n")},
	}, map[string]string{"svn:log": "r3"})
	if err != nil {
		t.Fatal(err)
	}
	got := lineagePaths(repo, "/branch", r3, r1)
	want := map[uint64]string{r3: "/branch", r2: "/branch", r1: "/trunk"}
	for rev, p := range want {
		if got[rev] != p {
			t.Errorf("rev %d path = %q, want %q (all: %v)", rev, got[rev], p, got)
		}
	}
}

func doReportAt(t *testing.T, url, body string) string {
	t.Helper()
	resp := doRequest(t, "REPORT", url, body, "", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("REPORT %s status=%d body=%s", url, resp.StatusCode, bodyOf(t, resp))
	}
	return bodyOf(t, resp)
}

// TestReportList checks the list-report shape svn ls depends on: items in the
// svn protocol namespace with node-kind attributes and absolute paths.
func TestReportList(t *testing.T) {
	ts, _, base := testServer(t, "", "")
	defer ts.Close()
	body := `<?xml version="1.0"?><S:list-report xmlns:S="svn:">` +
		`<S:path></S:path><S:revision>1</S:revision><S:depth>infinity</S:depth>` +
		`<S:prop>DAV:resourcetype</S:prop></S:list-report>`
	out := doReportAt(t, base+"/svn/demo/!svn/bln/1/trunk", body)
	for _, want := range []string{
		`<S:list-report xmlns:S="svn:"`,
		`<S:item node-kind="dir">/trunk</S:item>`,
		`<S:item node-kind="file">/trunk/README.txt</S:item>`,
		`<S:item node-kind="file">/trunk/doc/guide.txt</S:item>`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("list-report missing %q:\n%s", want, out)
		}
	}
}

// TestReportLogPathFilter: a log report for a path must only include
// revisions that touched it (or a parent/child of it).
func TestReportLogPathFilter(t *testing.T) {
	ts, repo, base := testServer(t, "", "")
	defer ts.Close()
	if _, err := repo.CommitTxn(1, []fs.Change{
		{Path: "/trunk/doc/guide.txt", Action: fs.ActionModify, Kind: fs.KindFile, Data: []byte("changed\n")},
	}, map[string]string{"svn:log": "doc edit", "svn:author": "alice", "svn:date": "2026-10-28T11:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	body := `<?xml version="1.0"?><S:log-report xmlns:S="svn:">` +
		`<S:start-revision>2</S:start-revision><S:end-revision>0</S:end-revision>` +
		`<S:discover-changed-paths/><S:no-revprops/><S:path></S:path></S:log-report>`
	readme := doReportAt(t, base+"/svn/demo/!svn/bln/2/trunk/README.txt", body)
	if strings.Contains(readme, "<D:version-name>2</D:version-name>") {
		t.Fatalf("README log includes the unrelated r2:\n%s", readme)
	}
	if !strings.Contains(readme, "<D:version-name>1</D:version-name>") {
		t.Fatalf("README log missing r1:\n%s", readme)
	}
	guide := doReportAt(t, base+"/svn/demo/!svn/bln/2/trunk/doc/guide.txt", body)
	if !strings.Contains(guide, "<D:version-name>2</D:version-name>") {
		t.Fatalf("guide log missing r2:\n%s", guide)
	}
}

// TestReportLogAscending checks that an ascending revision range returns
// items instead of nothing.
func TestReportLogAscending(t *testing.T) {
	ts, repo, base := testServer(t, "", "")
	defer ts.Close()
	if _, err := repo.CommitTxn(1, []fs.Change{
		{Path: "/trunk/README.txt", Action: fs.ActionModify, Kind: fs.KindFile, Data: []byte("v2\n")},
	}, map[string]string{"svn:log": "edit"}); err != nil {
		t.Fatal(err)
	}
	body := `<?xml version="1.0"?><S:log-report xmlns:S="svn:">` +
		`<S:start-revision>1</S:start-revision><S:end-revision>2</S:end-revision>` +
		`<S:path></S:path></S:log-report>`
	out := doReportAt(t, base+"/svn/demo/!svn/bln/2", body)
	if !strings.Contains(out, "<D:version-name>1</D:version-name>") ||
		!strings.Contains(out, "<D:version-name>2</D:version-name>") {
		t.Fatalf("ascending log missing revisions:\n%s", out)
	}
}

// TestReportGetMergeinfo checks the mergeinfo-report item shape.
func TestReportGetMergeinfo(t *testing.T) {
	ts, repo, base := testServer(t, "", "")
	defer ts.Close()
	if _, err := repo.CommitTxn(1, []fs.Change{
		{Path: "/trunk", Action: fs.ActionModify, Kind: fs.KindDir,
			Props: map[string]string{"svn:mergeinfo": "/branches/b:1-5"}},
	}, map[string]string{"svn:log": "mergeinfo"}); err != nil {
		t.Fatal(err)
	}
	body := `<?xml version="1.0"?><S:mergeinfo-report xmlns:S="svn:">` +
		`<S:revision>2</S:revision><S:inherit>inherited</S:inherit><S:path></S:path></S:mergeinfo-report>`
	out := doReportAt(t, base+"/svn/demo/!svn/bln/2/trunk", body)
	for _, want := range []string{
		`<S:mergeinfo-report`,
		`<S:mergeinfo-item>`,
		`<S:mergeinfo-info>/branches/b:1-5</S:mergeinfo-info>`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("mergeinfo-report missing %q:\n%s", want, out)
		}
	}
}

// TestReportUpdateMixedRevision: the WC root entry says r1 while the server is
// at r2. The changed file must be sent as open-file with a base-checksum of the
// r1 text (not r2), and unchanged files must be skipped.
func TestReportUpdateMixedRevision(t *testing.T) {
	ts, repo, base := testServer(t, "", "")
	defer ts.Close()
	if _, err := repo.CommitTxn(1, []fs.Change{
		{Path: "/trunk/README.txt", Action: fs.ActionModify, Kind: fs.KindFile, Data: []byte("v2\n")},
	}, map[string]string{"svn:log": "r2"}); err != nil {
		t.Fatal(err)
	}
	body := `<?xml version="1.0"?><S:update-report xmlns:S="svn:">` +
		`<S:src-path>` + base + `/svn/demo/</S:src-path>` +
		`<S:target-revision>2</S:target-revision><S:depth>unknown</S:depth>` +
		`<S:entry rev="1" depth="infinity"></S:entry></S:update-report>`
	out := doReport(t, base, body)
	if strings.Contains(out, `add-file name="README.txt"`) {
		t.Fatalf("changed file sent as add-file:\n%s", out)
	}
	if !strings.Contains(out, `open-file name="README.txt"`) {
		t.Fatalf("missing open-file for README:\n%s", out)
	}
	sum := md5.Sum([]byte("hello svn\n"))
	want := `base-checksum="` + hex.EncodeToString(sum[:]) + `"`
	if !strings.Contains(out, want) {
		t.Fatalf("base-checksum is not the r1 text (%s):\n%s", want, out)
	}
	if strings.Contains(out, `name="guide.txt"`) {
		t.Fatalf("unchanged file was sent:\n%s", out)
	}
}

// TestReportUpdateDeleteEntry: a path present at the WC baseline but gone from
// the target must be reported as delete-entry.
func TestReportUpdateDeleteEntry(t *testing.T) {
	ts, repo, base := testServer(t, "", "")
	defer ts.Close()
	if _, err := repo.CommitTxn(1, []fs.Change{
		{Path: "/trunk/doc/guide.txt", Action: fs.ActionDelete},
	}, map[string]string{"svn:log": "delete guide"}); err != nil {
		t.Fatal(err)
	}
	body := `<?xml version="1.0"?><S:update-report xmlns:S="svn:">` +
		`<S:src-path>` + base + `/svn/demo/</S:src-path>` +
		`<S:target-revision>2</S:target-revision><S:depth>unknown</S:depth>` +
		`<S:entry rev="1" depth="infinity"></S:entry></S:update-report>`
	out := doReport(t, base, body)
	if !strings.Contains(out, `<S:delete-entry name="guide.txt"/>`) {
		t.Fatalf("missing delete-entry:\n%s", out)
	}
}

// TestReportUpdateAnchoredSwitch: for svn switch the top-level open-directory
// is only the anchor and must not carry properties (the client asserts on it);
// the target element carries the content.
func TestReportUpdateAnchoredSwitch(t *testing.T) {
	ts, _, base := testServer(t, "", "")
	defer ts.Close()
	body := `<?xml version="1.0"?><S:update-report xmlns:S="svn:">` +
		`<S:src-path>` + base + `/svn/demo/</S:src-path>` +
		`<S:target-revision>1</S:target-revision><S:depth>unknown</S:depth>` +
		`<S:update-target>trunk</S:update-target>` +
		`<S:entry rev="1" depth="infinity"></S:entry></S:update-report>`
	out := doReport(t, base, body)
	anchorIdx := strings.Index(out, `<S:open-directory rev="`)
	targetIdx := strings.Index(out, `<S:open-directory name="trunk"`)
	if anchorIdx < 0 || targetIdx < 0 || targetIdx < anchorIdx {
		t.Fatalf("expected anchor then target directory:\n%s", out)
	}
	if between := out[anchorIdx:targetIdx]; strings.Contains(between, "set-prop") {
		t.Fatalf("anchor open-directory carries properties:\n%s", between)
	}
}

// TestReportTwoTreeDiff: svn merge/diff between two URLs sends src-path and
// dst-path. The report must diff those trees (branch@2 vs trunk@1), not the
// target path against itself; a branch-only file has to arrive as add-file.
func TestReportTwoTreeDiff(t *testing.T) {
	ts, repo, base := testServer(t, "", "")
	defer ts.Close()
	if _, err := repo.CommitTxn(1, []fs.Change{
		{Path: "/branch", Action: fs.ActionCopy, Kind: fs.KindDir, CopyFromPath: "/trunk", CopyFromRev: 1},
		{Path: "/branch/f.txt", Action: fs.ActionAdd, Kind: fs.KindFile, Data: []byte("f\n")},
	}, map[string]string{"svn:log": "r2"}); err != nil {
		t.Fatal(err)
	}
	body := `<?xml version="1.0"?><S:update-report xmlns:S="svn:">` +
		`<S:src-path>` + base + `/svn/demo/trunk</S:src-path>` +
		`<S:target-revision>2</S:target-revision>` +
		`<S:dst-path>` + base + `/svn/demo/branch</S:dst-path>` +
		`<S:depth>infinity</S:depth><S:entry depth="infinity" rev="1"></S:entry></S:update-report>`
	out := doReport(t, base, body)
	if !strings.Contains(out, `<S:add-file name="f.txt"`) {
		t.Fatalf("branch-only file not added:\n%s", out)
	}
	if strings.Contains(out, `name="README.txt"`) {
		t.Fatalf("unchanged file emitted:\n%s", out)
	}
	if strings.Contains(out, `<S:add-file name="guide.txt"`) {
		t.Fatalf("unchanged subtree emitted:\n%s", out)
	}
}
