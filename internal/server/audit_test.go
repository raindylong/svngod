package server

import (
	"bytes"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"svngod/internal/fs"
	"svngod/internal/fs/store"
)

// syncBuffer makes the audit stream safe to read from the test goroutine while
// the server still finishes its own request logging.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func auditTestServer(t *testing.T, repo *fs.Repo, hooksDir string) (*httptest.Server, *syncBuffer) {
	t.Helper()
	var buf syncBuffer
	srv := New(Config{
		BasePath: "/svn",
		HooksDir: hooksDir,
		OpenRepo: func(name string) (*fs.Repo, error) {
			if name == "demo" {
				return repo, nil
			}
			return nil, fs.ErrRepoNotFound
		},
		Logger: log.New(&buf, "", 0),
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, &buf
}

func TestAuditCommitLine(t *testing.T) {
	repo := buildRepo(t)
	ts, buf := auditTestServer(t, repo, "")
	commitRequest(t, ts.URL, "demo", "audit-commit")
	head, _ := repo.Head()

	out := buf.String()
	want := `audit commit mode="dav" repo="demo" rev=` + strconv.FormatUint(head, 10) +
		` user="anonymous" paths=3 add=1 modify=1 delete=1`
	if !strings.Contains(out, want) {
		t.Fatalf("audit stream missing %q:\n%s", want, out)
	}
}

func TestAuditRevPropLine(t *testing.T) {
	repo := buildRepo(t)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "pre-revprop-change"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	ts, buf := auditTestServer(t, repo, dir)
	resp := doRequest(t, "PROPPATCH", ts.URL+"/svn/demo/!svn/rev/1", revPropBody, "", "", nil)
	if resp.StatusCode != http.StatusMultiStatus {
		t.Fatalf("status=%d body=%s", resp.StatusCode, bodyOf(t, resp))
	}
	resp.Body.Close()

	want := `audit revprop repo="demo" rev=1 user="anonymous" set=1 remove=0`
	if out := buf.String(); !strings.Contains(out, want) {
		t.Fatalf("audit stream missing %q:\n%s", want, out)
	}
}

func TestAuditBundleCommitLine(t *testing.T) {
	st := store.NewMemoryStore()
	repo, err := fs.CreateRepo(st, "bun", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	var buf syncBuffer
	srv := New(Config{
		BasePath: "/svn",
		OpenRepo: func(name string) (*fs.Repo, error) {
			if name == "bun" {
				return repo, nil
			}
			return nil, fs.ErrRepoNotFound
		},
		Logger: log.New(&buf, "", 0),
	})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	b := &testBundle{Token: strings.Repeat("a1", 16), Message: "bulk"}
	if code, out := postBundle(t, ts, b.body("hello", "world")); code != 200 {
		t.Fatalf("bundle status=%d out=%v", code, out)
	}
	want := `audit commit mode="bundle" repo="bun" rev=1 user="anonymous" paths=2 add=2 modify=0 delete=0`
	if out := buf.String(); !strings.Contains(out, want) {
		t.Fatalf("audit stream missing %q:\n%s", want, out)
	}
}

func TestAuditQuotesUserControlledFields(t *testing.T) {
	var buf syncBuffer
	srv := New(Config{Logger: log.New(&buf, "", 0)})
	srv.audit("login", "user", "evil\nadmin", "repo", "demo\r\nx")
	out := buf.String()
	if strings.Count(out, "\n") != 1 || strings.Contains(out, "\r") {
		t.Fatalf("audit line was not kept on one record: %q", out)
	}
	if !strings.Contains(out, `user="evil\nadmin"`) {
		t.Fatalf("audit did not quote the username: %q", out)
	}
}
