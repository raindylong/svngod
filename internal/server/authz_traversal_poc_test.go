package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"svngod/internal/authz"
	"svngod/internal/fs"
	"svngod/internal/fs/store"
)

// These tests exist to prove or disprove three authz findings before any fix is
// written. A finding that cannot be reproduced must not be "fixed" - the change
// would be unverifiable and the real bug would stay.
//
// The common thread: the path authz checks and the path the handler resolves
// must be the same string. Three layers disagree today:
//
//	parseResource   keeps ".."        (url.go)
//	authz           keeps ".."        (authz.normalizePath)
//	fs              DROPS ".."        (fs.segments)
//
// So authz evaluates "/../private" (no rule matches, falls back to root) while
// the fs resolves the same request to "/private".

// authzServer builds a repo plus a server whose authz grants read on "/" and
// denies everything under /private.
func authzServer(t *testing.T, rule string) *Server {
	t.Helper()
	az, err := authz.Parse([]byte(rule))
	if err != nil {
		t.Fatalf("parse authz: %v", err)
	}
	return authzServerWith(t, az)
}

func authzServerWith(t *testing.T, az *authz.Config) *Server {
	t.Helper()
	secret := "TOP SECRET\n"
	st := store.NewMemoryStore()
	repo, err := fs.CreateRepo(st, "demo", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CommitTxn(0, []fs.Change{
		{Path: "/public", Action: fs.ActionAdd, Kind: fs.KindDir},
		{Path: "/public/readme.txt", Action: fs.ActionAdd, Kind: fs.KindFile, Data: []byte("public\n")},
		{Path: "/private", Action: fs.ActionAdd, Kind: fs.KindDir},
		{Path: "/private/secret.txt", Action: fs.ActionAdd, Kind: fs.KindFile, Data: []byte(secret)},
		{Path: "/private/keep.txt", Action: fs.ActionAdd, Kind: fs.KindFile, Data: []byte("orig\n")},
	}, map[string]string{"svn:log": "seed", "svn:author": "alice", "svn:date": "2026-10-28T10:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	return New(Config{
		BasePath:  "/svn",
		Authorize: az,
		OpenRepo: func(name string) (*fs.Repo, error) {
			if name == "demo" {
				return repo, nil
			}
			return nil, fs.ErrRepoNotFound
		},
	})
}

const denyPrivate = "[demo:/]\n* = r\n[demo:/private]\n* =\n"

// TestAuthzDotDotCannotReadDeniedPath is the read half of the traversal finding.
// A leading ".." must not let a client read /private through an allowed URL.
func TestAuthzDotDotCannotReadDeniedPath(t *testing.T) {
	s := authzServer(t, denyPrivate)

	// Control: the direct URL is denied, proving the rule itself works.
	direct := httptest.NewRequest("GET", "/svn/demo/private/secret.txt", nil)
	direct.RemoteAddr = "192.0.2.1:1234"
	directRec := httptest.NewRecorder()
	s.Handler().ServeHTTP(directRec, direct)
	if directRec.Code != http.StatusForbidden {
		t.Fatalf("control: direct read returned %d, want 403", directRec.Code)
	}

	for _, path := range []string{
		"/svn/demo/../private/secret.txt",
		"/svn/demo/%2e%2e/private/secret.txt",
		"/svn/demo/../../../../private/secret.txt",
	} {
		req := httptest.NewRequest("GET", path, nil)
		req.RemoteAddr = "192.0.2.1:1234"
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)

		if rec.Code == http.StatusOK && strings.Contains(rec.Body.String(), "TOP SECRET") {
			t.Fatalf("%s leaked the denied body (status %d)", path, rec.Code)
		}
		if rec.Code == http.StatusOK {
			t.Fatalf("%s returned 200 for a denied path", path)
		}
	}
}

// TestAuthzDotDotCannotWriteDeniedPath is the write half.
func TestAuthzDotDotCannotWriteDeniedPath(t *testing.T) {
	s := authzServer(t, "[demo:/]\n* = rw\n[demo:/private]\n* =\n")

	req := httptest.NewRequest("PUT", "/svn/demo/!svn/txn/txn-x/../private/keep.txt", strings.NewReader("HACKED\n"))
	req.RemoteAddr = "192.0.2.1:1234"
	req.ContentLength = int64(len("HACKED\n"))
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code == http.StatusOK || rec.Code == http.StatusCreated || rec.Code == http.StatusNoContent {
		t.Fatalf("write through '..' accepted with %d", rec.Code)
	}
}

// TestAuthzReportMeCannotReadDeniedPath is the /!svn/me half: the URL carries no
// path, so authz sees "/" and the report body names the real target.
func TestAuthzReportMeCannotReadDeniedPath(t *testing.T) {
	s := authzServer(t, denyPrivate)

	body := `<?xml version="1.0"?>
<S:get-locks-report xmlns:S="svn:" xmlns:D="DAV:">
<D:path>/private</D:path>
</S:get-locks-report>`
	req := httptest.NewRequest("REPORT", "/svn/demo/!svn/me", strings.NewReader(body))
	req.RemoteAddr = "192.0.2.1:1234"
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	// The exact status matters less than whether it exposes the denied subtree.
	if rec.Code == http.StatusOK && strings.Contains(rec.Body.String(), "secret") {
		t.Fatalf("report on /!svn/me exposed the denied subtree: %s", rec.Body.String())
	}
}
