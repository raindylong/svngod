package server

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"io"
	"net/http"
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

// TestReportLogQuietFilter: `svn log -q URL/path` sends no
// discover-changed-paths element. The server must still filter by path, but
// must not run a whole-tree diff per revision (the 1M-file log slowdown); the
// node-id comparison decides whether the path moved.
func TestReportLogQuietFilter(t *testing.T) {
	ts, repo, base := testServer(t, "", "")
	defer ts.Close()
	if _, err := repo.CommitTxn(1, []fs.Change{
		{Path: "/trunk/doc/guide.txt", Action: fs.ActionModify, Kind: fs.KindFile, Data: []byte("changed\n")},
	}, map[string]string{"svn:log": "doc edit"}); err != nil {
		t.Fatal(err)
	}
	body := `<?xml version="1.0"?><S:log-report xmlns:S="svn:">` +
		`<S:start-revision>2</S:start-revision><S:end-revision>0</S:end-revision>` +
		`<S:no-revprops/><S:path></S:path></S:log-report>`

	readme := doReportAt(t, base+"/svn/demo/!svn/bln/2/trunk/README.txt", body)
	if strings.Contains(readme, "<D:version-name>2</D:version-name>") {
		t.Fatalf("quiet README log includes the unrelated r2:\n%s", readme)
	}
	if !strings.Contains(readme, "<D:version-name>1</D:version-name>") {
		t.Fatalf("quiet README log missing r1:\n%s", readme)
	}
	if strings.Contains(readme, "S:added-path") || strings.Contains(readme, "S:modified-path") {
		t.Fatalf("quiet log emitted changed paths:\n%s", readme)
	}
	guide := doReportAt(t, base+"/svn/demo/!svn/bln/2/trunk/doc/guide.txt", body)
	if !strings.Contains(guide, "<D:version-name>2</D:version-name>") {
		t.Fatalf("quiet guide log missing r2:\n%s", guide)
	}
}

// TestReportLogQuietFilterDirectoryCopy: a path inside a directory copied in
// this revision counts as touched even though its own node id is new; the
// ancestor copy check must keep the revision.
func TestReportLogQuietFilterDirectoryCopy(t *testing.T) {
	ts, repo, base := testServer(t, "", "")
	defer ts.Close()
	if _, err := repo.CommitTxn(1, []fs.Change{
		{Path: "/branch", Action: fs.ActionCopy, Kind: fs.KindDir,
			CopyFromPath: "/trunk", CopyFromRev: 1},
	}, map[string]string{"svn:log": "branch"}); err != nil {
		t.Fatal(err)
	}
	body := `<?xml version="1.0"?><S:log-report xmlns:S="svn:">` +
		`<S:start-revision>2</S:start-revision><S:end-revision>0</S:end-revision>` +
		`<S:no-revprops/><S:path></S:path></S:log-report>`
	out := doReportAt(t, base+"/svn/demo/!svn/bln/2/branch/README.txt", body)
	if !strings.Contains(out, "<D:version-name>2</D:version-name>") {
		t.Fatalf("quiet log for a copied subtree missed the copy revision:\n%s", out)
	}
}

// TestReportLogVerbosePrunesUnchanged: treeDiff must not emit unchanged
// siblings (the node-id prune keeps a 1M-file revision cheap and the output
// accurate).
func TestReportLogVerbosePrunesUnchanged(t *testing.T) {
	ts, repo, base := testServer(t, "", "")
	defer ts.Close()
	if _, err := repo.CommitTxn(1, []fs.Change{
		{Path: "/trunk/doc/guide.txt", Action: fs.ActionModify, Kind: fs.KindFile, Data: []byte("changed\n")},
	}, map[string]string{"svn:log": "doc edit"}); err != nil {
		t.Fatal(err)
	}
	body := `<?xml version="1.0"?><S:log-report xmlns:S="svn:">` +
		`<S:start-revision>2</S:start-revision><S:end-revision>2</S:end-revision>` +
		`<S:discover-changed-paths/><S:no-revprops/><S:path></S:path></S:log-report>`
	out := doReportAt(t, base+"/svn/demo/!svn/bln/2", body)
	if !strings.Contains(out, "<S:modified-path") || !strings.Contains(out, "/trunk/doc/guide.txt") {
		t.Fatalf("verbose log missing the modified path:\n%s", out)
	}
	if strings.Contains(out, "/trunk/README.txt") {
		t.Fatalf("verbose log emitted an unchanged sibling:\n%s", out)
	}
}

// TestReportLogVerbosePropOnly: a property-only change creates a new node
// revision, so the node-id prune must still report it (with prop-mods and no
// text-mods).
func TestReportLogVerbosePropOnly(t *testing.T) {
	ts, repo, base := testServer(t, "", "")
	defer ts.Close()
	if _, err := repo.CommitTxn(1, []fs.Change{
		{Path: "/trunk/README.txt", Action: fs.ActionModify, Kind: fs.KindFile,
			Props: map[string]string{"svn:eol-style": "native"}},
	}, map[string]string{"svn:log": "prop edit"}); err != nil {
		t.Fatal(err)
	}
	body := `<?xml version="1.0"?><S:log-report xmlns:S="svn:">` +
		`<S:start-revision>2</S:start-revision><S:end-revision>2</S:end-revision>` +
		`<S:discover-changed-paths/><S:no-revprops/><S:path></S:path></S:log-report>`
	out := doReportAt(t, base+"/svn/demo/!svn/bln/2", body)
	if !strings.Contains(out, "/trunk/README.txt") || !strings.Contains(out, `prop-mods="true"`) {
		t.Fatalf("prop-only change missing from verbose log:\n%s", out)
	}
	if strings.Contains(out, `text-mods="true"`) {
		t.Fatalf("prop-only change reported text-mods:\n%s", out)
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

// TestLineageGapBetweenSourceAndCopy: a copy made at r3 from a source last
// changed at r1 does not exist at r2; svn reports a pathless gap there so
// merge ranges start at the right revision (r2, not r3).
func TestLineageGapBetweenSourceAndCopy(t *testing.T) {
	st := store.NewMemoryStore()
	repo, err := fs.CreateRepo(st, "r", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	r1, _ := repo.CommitTxn(0, []fs.Change{
		{Path: "/trunk", Action: fs.ActionAdd, Kind: fs.KindDir, Props: map[string]string{}},
		{Path: "/trunk/a.txt", Action: fs.ActionAdd, Kind: fs.KindFile, Data: []byte("a\n")},
	}, nil)
	r2, _ := repo.CommitTxn(r1, []fs.Change{
		{Path: "/trunk/a.txt", Action: fs.ActionModify, Kind: fs.KindFile, Data: []byte("a2\n")},
	}, nil)
	r3, err := repo.CommitTxn(r2, []fs.Change{
		{Path: "/branch", Action: fs.ActionCopy, Kind: fs.KindDir, CopyFromPath: "/trunk", CopyFromRev: r1},
		{Path: "/branch/a.txt", Action: fs.ActionCopy, Kind: fs.KindFile, CopyFromPath: "/trunk/a.txt", CopyFromRev: r2},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := lineagePaths(repo, "/branch", r3, r1)
	want := map[uint64]string{r3: "/branch", r2: "", r1: "/trunk"}
	for rev, p := range want {
		if got[rev] != p {
			t.Errorf("rev %d = %q, want %q (all: %v)", rev, got[rev], p, got)
		}
	}
}

// TestReportUpdateSubdirTargetBaseline: an `svn up` run inside a subdirectory
// sends update-target and an entry tree rooted at that target. The per-file
// revision must be taken from that tree; rooting it at src-path used to send
// an already-current file as add-file/open-file with the wrong base
// (E155017 checksum mismatch).
func TestReportUpdateSubdirTargetBaseline(t *testing.T) {
	ts, repo, base := testServer(t, "", "")
	defer ts.Close()
	if _, err := repo.CommitTxn(1, []fs.Change{
		{Path: "/trunk/README.txt", Action: fs.ActionModify, Kind: fs.KindFile, Data: []byte("v2\n")},
	}, map[string]string{"svn:log": "r2"}); err != nil {
		t.Fatal(err)
	}
	body := `<?xml version="1.0"?><S:update-report xmlns:S="svn:">` +
		`<S:src-path>` + base + `/svn/demo/</S:src-path>` +
		`<S:target-revision>2</S:target-revision><S:update-target>trunk</S:update-target>` +
		`<S:depth>unknown</S:depth>` +
		`<S:entry rev="2" depth="infinity" start-empty="true"></S:entry>` +
		`<S:entry rev="2" depth="infinity">README.txt</S:entry>` +
		`</S:update-report>`
	out := doReport(t, base, body)
	if strings.Contains(out, `name="README.txt"`) {
		t.Fatalf("file already at the target revision was sent again:\n%s", out)
	}
}

// TestReportFetchURLUsesModificationRev covers a copy that was edited later:
// the update report must point the client at a revision that holds the current
// text. It used to point at the copy revision, so a fresh export received the
// copy source's bytes and the client reported a checksum mismatch.
func TestReportFetchURLUsesModificationRev(t *testing.T) {
	ts, repo, base := testServer(t, "", "")
	defer ts.Close()
	props := func(msg string) map[string]string {
		return map[string]string{"svn:log": msg, "svn:author": "u", "svn:date": "2026-01-01T00:00:00Z"}
	}
	if _, err := repo.CommitTxn(1, []fs.Change{{
		Path: "/trunk/copied.txt", Action: fs.ActionCopy, Kind: fs.KindFile,
		CopyFromPath: "/trunk/README.txt", CopyFromRev: 1,
	}}, props("copy")); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CommitTxn(2, []fs.Change{{
		Path: "/trunk/copied.txt", Action: fs.ActionModify, Kind: fs.KindFile,
		Data: []byte("changed after copy\n"),
	}}, props("edit")); err != nil {
		t.Fatal(err)
	}
	body := `<?xml version="1.0" encoding="utf-8"?>` +
		`<S:update-report xmlns:S="http://subversion.tigris.org/xmlns/dav/" xmlns:V="DAV:">` +
		`<S:src-path>` + base + `/svn/demo/</S:src-path>` +
		`<S:recurse>yes</S:recurse>` +
		`<S:target-revision>3</S:target-revision>` +
		`<S:update-target rev="0"/>` +
		`<S:ignore-ancestry>no</S:ignore-ancestry>` +
		`<S:text-deltas>yes</S:text-deltas>` +
		`</S:update-report>`
	out := doReport(t, base, body)

	i := strings.Index(out, `add-file name="copied.txt"`)
	if i < 0 {
		t.Fatalf("copied.txt missing from update report:\n%s", out)
	}
	rest := out[i:]
	j := strings.Index(rest, `!svn/rvr/`)
	if j < 0 {
		t.Fatalf("no rvr URL for copied.txt:\n%s", rest[:400])
	}
	href := rest[j:]
	revEnd := strings.IndexByte(href[len("!svn/rvr/"):], '/')
	gotRev := href[len("!svn/rvr/") : len("!svn/rvr/")+revEnd]
	if gotRev != "3" {
		t.Fatalf("copied.txt fetch rev=%s, want 3 (copy was edited at r3)", gotRev)
	}
	// The advertised URL must actually serve the edited text.
	k := strings.Index(rest, "http")
	urlEnd := strings.Index(rest[k:], "</D:href>")
	url := rest[k : k+urlEnd]
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if string(b) != "changed after copy\n" {
		t.Fatalf("fetch URL content=%q", b)
	}
}
