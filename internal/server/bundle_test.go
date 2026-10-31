package server

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"svngod/internal/fs"
	"svngod/internal/fs/store"
)

type testBundleChange struct {
	Path    string            `json:"path"`
	Action  string            `json:"action"`
	Kind    string            `json:"kind"`
	BaseRev uint64            `json:"baseRev,omitempty"`
	Props   map[string]string `json:"props,omitempty"`
	TextLen int64             `json:"textLen,omitempty"`
}

type testBundle struct {
	Token   string             `json:"token"`
	BaseRev uint64             `json:"baseRev"`
	Message string             `json:"message"`
	Changes []testBundleChange `json:"changes"`
}

func (b *testBundle) body(texts ...string) []byte {
	b.Changes = b.Changes[:0]
	for i, txt := range texts {
		b.Changes = append(b.Changes, testBundleChange{
			Path: fmt.Sprintf("/f%d.txt", i), Action: "put", Kind: "file",
			BaseRev: b.BaseRev, TextLen: int64(len(txt)),
		})
	}
	hdr, _ := json.Marshal(b)
	var buf bytes.Buffer
	buf.WriteString(bundleMagic)
	var sz [8]byte
	binary.BigEndian.PutUint64(sz[:], uint64(len(hdr)))
	buf.Write(sz[:])
	buf.Write(hdr)
	for _, txt := range texts {
		buf.WriteString(txt)
	}
	return buf.Bytes()
}

func newBundleTestServer(t *testing.T) (*httptest.Server, *fs.Repo) {
	t.Helper()
	st := store.NewMemoryStore()
	repo, err := fs.CreateRepo(st, "bun", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	srv := New(Config{
		BasePath: "/svn",
		OpenRepo: func(name string) (*fs.Repo, error) {
			if name != "bun" {
				return nil, fs.ErrRepoNotFound
			}
			return repo, nil
		},
	})
	return httptest.NewServer(srv.Handler()), repo
}

func postBundle(t *testing.T, ts *httptest.Server, body []byte) (int, map[string]interface{}) {
	t.Helper()
	req, err := http.NewRequest("POST", ts.URL+"/svn/bun/!svngod/bundle", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-svngod-bundle")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out := map[string]interface{}{}
	json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestBundleCommitAndReplay(t *testing.T) {
	ts, repo := newBundleTestServer(t)
	defer ts.Close()

	b := &testBundle{Token: strings.Repeat("a1", 16), Message: "bulk"}
	code, out := postBundle(t, ts, b.body("hello", "world"))
	if code != 200 || out["revision"].(float64) != 1 {
		t.Fatalf("bundle commit status=%d out=%v", code, out)
	}
	nr, err := repo.Resolve(1, "/f0.txt")
	if err != nil {
		t.Fatal(err)
	}
	if txt, _ := repo.GetText(nr.Node); string(txt) != "hello" {
		t.Fatalf("content=%q", txt)
	}

	// Same token -> same revision, no second commit.
	b2 := &testBundle{Token: b.Token, Message: "bulk"}
	code, out = postBundle(t, ts, b2.body("hello", "world"))
	if code != 200 || out["revision"].(float64) != 1 || out["replayed"] != true {
		t.Fatalf("replay status=%d out=%v", code, out)
	}
	if head, _ := repo.Head(); head != 1 {
		t.Fatalf("replay created a revision: head=%d", head)
	}

	// A new token with a modify and a delete lands as one revision.
	mod := &testBundle{Token: strings.Repeat("b2", 16), BaseRev: 1, Message: "edit"}
	mod.Changes = []testBundleChange{
		{Path: "/f0.txt", Action: "put", Kind: "file", BaseRev: 1, TextLen: 5},
		{Path: "/f1.txt", Action: "delete", Kind: "file", BaseRev: 1},
	}
	hdr, _ := json.Marshal(mod)
	var buf bytes.Buffer
	buf.WriteString(bundleMagic)
	var sz [8]byte
	binary.BigEndian.PutUint64(sz[:], uint64(len(hdr)))
	buf.Write(sz[:])
	buf.Write(hdr)
	buf.WriteString("HELLO")
	code, out = postBundle(t, ts, buf.Bytes())
	if code != 200 || out["revision"].(float64) != 2 {
		t.Fatalf("modify bundle status=%d out=%v", code, out)
	}
	nr, _ = repo.Resolve(2, "/f0.txt")
	if txt, _ := repo.GetText(nr.Node); string(txt) != "HELLO" {
		t.Fatalf("modified content=%q", txt)
	}
	if _, err := repo.Resolve(2, "/f1.txt"); err == nil {
		t.Fatal("deleted file still present")
	}
}

func TestBundleDeleteAndAuthz(t *testing.T) {
	ts, repo := newBundleTestServer(t)
	defer ts.Close()
	if _, err := repo.CommitTxn(0, []fs.Change{{Path: "/gone.txt", Action: fs.ActionAdd, Kind: fs.KindFile, Data: []byte("x")}}, nil); err != nil {
		t.Fatal(err)
	}
	b := &testBundle{Token: strings.Repeat("c3", 16), BaseRev: 1, Message: "rm"}
	b.Changes = []testBundleChange{{Path: "/gone.txt", Action: "delete", Kind: "file", BaseRev: 1}}
	hdr, _ := json.Marshal(b)
	var buf bytes.Buffer
	buf.WriteString(bundleMagic)
	var sz [8]byte
	binary.BigEndian.PutUint64(sz[:], uint64(len(hdr)))
	buf.Write(sz[:])
	buf.Write(hdr)
	code, out := postBundle(t, ts, buf.Bytes())
	if code != 200 || out["revision"].(float64) != 2 {
		t.Fatalf("delete bundle status=%d out=%v", code, out)
	}
	if _, err := repo.Resolve(2, "/gone.txt"); err == nil {
		t.Fatal("file still present after bundle delete")
	}

	// A malformed token is rejected.
	bad := &testBundle{Token: "short", Message: "x"}
	code, _ = postBundle(t, ts, bad.body("x"))
	if code != 400 {
		t.Fatalf("short token status=%d, want 400", code)
	}
}

var _ = io.Discard

// TestBundleLargeText ensures the bulk channel carries files above the old
// 16 MiB client limit: the body is spooled to disk on both sides, so a large
// text must commit and read back byte-identically.
func TestBundleLargeText(t *testing.T) {
	ts, repo := newBundleTestServer(t)
	defer ts.Close()
	big := strings.Repeat("0123456789abcdef", (17<<20)/16) // 17 MiB
	b := &testBundle{Token: strings.Repeat("a1", 16), Message: "large"}
	code, out := postBundle(t, ts, b.body(big))
	if code != http.StatusOK {
		t.Fatalf("status=%d out=%v", code, out)
	}
	head, _ := repo.HeadFresh()
	if head != 1 {
		t.Fatalf("head=%d", head)
	}
	nr, err := repo.Resolve(1, "/f0.txt")
	if err != nil {
		t.Fatal(err)
	}
	text, err := repo.GetText(nr.Node)
	if err != nil {
		t.Fatal(err)
	}
	if len(text) != len(big) || string(text) != big {
		t.Fatalf("text mismatch: got %d bytes", len(text))
	}
}

func TestBundleTokenInRevPropsAndRecovery(t *testing.T) {
	ts, repo := newBundleTestServer(t)
	defer ts.Close()
	tok := strings.Repeat("c3", 16)
	b := &testBundle{Token: tok, Message: "recover"}
	body := b.body("payload")
	code, out := postBundle(t, ts, body)
	if code != http.StatusOK {
		t.Fatalf("status=%d out=%v", code, out)
	}
	rev1 := uint64(out["revision"].(float64))
	rp, err := repo.GetRevProps(rev1)
	if err != nil || rp == nil {
		t.Fatalf("revprops: %v", err)
	}
	if rp.Props["svngod:bundle-token"] != tok {
		t.Fatalf("token revprop=%q", rp.Props["svngod:bundle-token"])
	}

	// Simulate a crash after CommitTxn but before the token record was stored.
	ctx := context.Background()
	if err := repo.Store().Delete(ctx, []string{bundleKey(repo, tok)}); err != nil {
		t.Fatal(err)
	}
	in, _ := json.Marshal(bundleIntent{Token: tok, BaseRev: b.BaseRev, Message: b.Message, User: "alice", Created: time.Now().Unix()})
	if err := repo.Store().Put(ctx, bundleIntentKey(repo, tok), in); err != nil {
		t.Fatal(err)
	}

	code, out = postBundle(t, ts, body)
	if code != http.StatusOK {
		t.Fatalf("replay status=%d out=%v", code, out)
	}
	rev2 := uint64(out["revision"].(float64))
	if rev2 != rev1 {
		t.Fatalf("replay revision=%d, want %d", rev2, rev1)
	}
	if replayed, _ := out["replayed"].(bool); !replayed {
		t.Fatalf("expected replayed=true, out=%v", out)
	}
	head, _ := repo.HeadFresh()
	if head != 1 {
		t.Fatalf("head=%d, duplicate commit", head)
	}
	if _, err := repo.Store().Get(ctx, bundleIntentKey(repo, tok), nil); err == nil {
		t.Fatal("intent not cleaned up")
	}
}

func TestBundleStatReportsHeadHashes(t *testing.T) {
	ts, repo := newBundleTestServer(t)
	defer ts.Close()
	data := bytes.Repeat([]byte("z"), 5<<20)
	if _, err := repo.CommitTxn(0, []fs.Change{{
		Path: "/big.bin", Action: fs.ActionAdd, Kind: fs.KindFile, Data: data,
	}}, map[string]string{"svn:log": "x", "svn:author": "u", "svn:date": "2026-01-01T00:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	body := `{"files":["/big.bin","/missing.bin"]}`
	req, _ := http.NewRequest("POST", ts.URL+"/svn/bun/!svngod/stat", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var out struct {
		Files map[string]struct {
			SHA1 string `json:"sha1"`
			Size int64  `json:"size"`
		} `json:"files"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	e, ok := out.Files["/big.bin"]
	if !ok {
		t.Fatalf("big.bin missing from stat: %v", out.Files)
	}
	want := sha1.Sum(data)
	if e.SHA1 != hex.EncodeToString(want[:]) || e.Size != int64(len(data)) {
		t.Fatalf("stat=%+v want sha1=%x size=%d", e, want, len(data))
	}
	if _, ok := out.Files["/missing.bin"]; ok {
		t.Fatal("missing path reported")
	}
}

func TestBundleCheckChangeRejectsStaleBase(t *testing.T) {
	ts, repo := newBundleTestServer(t)
	defer ts.Close()
	if _, err := repo.CommitTxn(0, []fs.Change{
		{Path: "/a.txt", Action: fs.ActionAdd, Kind: fs.KindFile, Data: []byte("a1")},
		{Path: "/b.txt", Action: fs.ActionAdd, Kind: fs.KindFile, Data: []byte("b1")},
	}, map[string]string{"svn:log": "r1", "svn:author": "u", "svn:date": "2026-01-01T00:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CommitTxn(0, []fs.Change{
		{Path: "/a.txt", Action: fs.ActionModify, Kind: fs.KindFile, Data: []byte("a2")},
	}, map[string]string{"svn:log": "r2", "svn:author": "u", "svn:date": "2026-01-01T00:00:01Z"}); err != nil {
		t.Fatal(err)
	}
	build := func(checkBase uint64) []byte {
		hdr := &testBundle{
			Token:   strings.Repeat("d4", 16),
			BaseRev: 2,
			Message: "check",
			Changes: []testBundleChange{
				{Path: "/b.txt", Action: "put", Kind: "file", BaseRev: 2, TextLen: 2},
				{Path: "/a.txt", Action: "check", Kind: "file", BaseRev: checkBase},
			},
		}
		h, _ := json.Marshal(hdr)
		var buf bytes.Buffer
		buf.WriteString(bundleMagic)
		var sz [8]byte
		binary.BigEndian.PutUint64(sz[:], uint64(len(h)))
		buf.Write(sz[:])
		buf.Write(h)
		buf.WriteString("b2")
		return buf.Bytes()
	}
	code, out := postBundle(t, ts, build(1))
	if code != http.StatusConflict {
		t.Fatalf("stale check status=%d out=%v", code, out)
	}
	head, _ := repo.HeadFresh()
	if head != 2 {
		t.Fatalf("failed check moved head to %d", head)
	}
	// A fresh token is required because the failed attempt consumed the first.
	b := &testBundle{
		Token:   strings.Repeat("e5", 16),
		BaseRev: 2,
		Message: "check ok",
		Changes: []testBundleChange{
			{Path: "/b.txt", Action: "put", Kind: "file", BaseRev: 2, TextLen: 2},
			{Path: "/a.txt", Action: "check", Kind: "file", BaseRev: 2},
		},
	}
	h, _ := json.Marshal(b)
	var buf bytes.Buffer
	buf.WriteString(bundleMagic)
	var sz [8]byte
	binary.BigEndian.PutUint64(sz[:], uint64(len(h)))
	buf.Write(sz[:])
	buf.Write(h)
	buf.WriteString("b2")
	code, out = postBundle(t, ts, buf.Bytes())
	if code != http.StatusOK {
		t.Fatalf("fresh check status=%d out=%v", code, out)
	}
	head, _ = repo.HeadFresh()
	if head != 3 {
		t.Fatalf("head=%d, want 3", head)
	}
	nr, _ := repo.Resolve(3, "/a.txt")
	got, _ := repo.GetText(nr.Node)
	if string(got) != "a2" {
		t.Fatalf("/a.txt changed to %q", got)
	}
}
