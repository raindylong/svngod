package server

import (
	"net/http/httptest"
	"strings"
	"testing"

	"svngod/internal/fs"
	"svngod/internal/fs/store"
)

// TestReportSeesCommitsFromAnotherProcess pins the cross-process read
// contract: a REPORT (svn up / svn log) served by process B must include a
// revision process A just committed, even when B has a warm HEAD cache. The
// 100ms HEAD cache used to make the newest revision invisible right after a
// load-balanced commit.
func TestReportSeesCommitsFromAnotherProcess(t *testing.T) {
	st := store.NewMemoryStore()
	procA, err := fs.CreateRepo(st, "demo", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	r1, err := procA.CommitTxn(0, []fs.Change{
		{Path: "/trunk", Action: fs.ActionAdd, Kind: fs.KindDir},
		{Path: "/trunk/a.txt", Action: fs.ActionAdd, Kind: fs.KindFile, Data: []byte("a\n")},
	}, map[string]string{"svn:log": "first", "svn:author": "alice", "svn:date": "2026-10-28T10:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	procB, err := fs.OpenRepo(st, "demo")
	if err != nil {
		t.Fatal(err)
	}
	// Prime B's cache; the next assertion only means anything while it is warm.
	if head, herr := procB.Head(); herr != nil || head != r1 {
		t.Fatalf("prime head=%d err=%v", head, herr)
	}
	if _, err := procA.CommitTxn(r1, []fs.Change{
		{Path: "/trunk/late.txt", Action: fs.ActionAdd, Kind: fs.KindFile, Data: []byte("late\n")},
	}, map[string]string{"svn:log": "late commit", "svn:author": "bob", "svn:date": "2026-10-28T10:01:00Z"}); err != nil {
		t.Fatal(err)
	}

	srv := New(Config{
		BasePath: "/svn",
		OpenRepo: func(name string) (*fs.Repo, error) {
			if name == "demo" {
				return procB, nil
			}
			return nil, fs.ErrRepoNotFound
		},
	})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	logBody := `<?xml version="1.0" encoding="utf-8"?>` +
		`<S:log-report xmlns:S="http://subversion.tigris.org/xmlns/dav/">` +
		`<S:path>/</S:path>` +
		`<S:revprop name="svn:author"/>` +
		`<S:revprop name="svn:log"/>` +
		`</S:log-report>`
	out := doReportAt(t, ts.URL+"/svn/demo/!svn/me", logBody)
	for _, want := range []string{"late commit", "<D:version-name>2</D:version-name>"} {
		if !strings.Contains(out, want) {
			t.Fatalf("log-report served by another process is missing %q:\n%s", want, out)
		}
	}

	updBody := `<?xml version="1.0" encoding="utf-8"?>` +
		`<S:update-report xmlns:S="http://subversion.tigris.org/xmlns/dav/" xmlns:V="DAV:">` +
		`<S:src-path>` + ts.URL + `/svn/demo/</S:src-path>` +
		`<S:recurse>yes</S:recurse>` +
		`<S:update-target rev="1"/>` +
		`</S:update-report>`
	out = doReportAt(t, ts.URL+"/svn/demo/!svn/me", updBody)
	for _, want := range []string{`target-revision rev="2"`, "late.txt"} {
		if !strings.Contains(out, want) {
			t.Fatalf("update-report served by another process is missing %q:\n%s", want, out)
		}
	}
}
