package server

import (
	"bytes"
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"svngod/internal/fs"
	"svngod/internal/fs/store"
)

// scrubFixture builds a Server whose repoCache holds one "demo" repository
// with a committed revision, plus a handle on its backing store.
func scrubFixture(t *testing.T) (*Server, *fs.Repo, store.Store) {
	t.Helper()
	st := store.NewMemoryStore()
	repo, err := fs.CreateRepo(st, "demo", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CommitTxn(0, []fs.Change{
		{Path: "/trunk", Action: fs.ActionAdd, Kind: fs.KindDir},
		{Path: "/trunk/README.txt", Action: fs.ActionAdd, Kind: fs.KindFile,
			Data: bytes.Repeat([]byte("hello svn\n"), 40000)}, // external text
	}, map[string]string{"svn:log": "init"}); err != nil {
		t.Fatal(err)
	}
	srv := New(Config{BasePath: "/svn"})
	srv.repoMapMu.Lock()
	srv.repoCache["demo"] = repo
	srv.repoMapMu.Unlock()
	return srv, repo, st
}

// TestScrubOnceAdvancesWatermark: a clean repository is scrubbed once; the
// watermark must reach head and no issue must be counted.
func TestScrubOnceAdvancesWatermark(t *testing.T) {
	srv, repo, _ := scrubFixture(t)
	srv.scrubOnce(context.Background())
	srv.scrubMu.Lock()
	last := srv.scrub.verified["demo"]
	srv.scrubMu.Unlock()
	head, _ := repo.Head()
	if last != head {
		t.Fatalf("watermark=%d, want head=%d", last, head)
	}
	srv.m.scrubIssuesMu.Lock()
	n := srv.m.scrubIssues["demo"]
	srv.m.scrubIssuesMu.Unlock()
	if n != 0 {
		t.Fatalf("clean scrub counted %d issues", n)
	}
}

// TestScrubCountsCorruption: overwrite the external text object with garbage;
// the scrub must count the corruption in the metric and expose it via
// /_metrics, while still advancing the watermark (no infinite re-walk).
func TestScrubCountsCorruption(t *testing.T) {
	srv, repo, st := scrubFixture(t)
	nr, err := repo.Resolve(1, "/trunk/README.txt")
	if err != nil {
		t.Fatal(err)
	}
	if nr.TextRep != fs.TextRepExternal {
		t.Fatalf("text rep=%d, want external", nr.TextRep)
	}
	key := repo.Keys().Text(nr.TextRef)
	if err := st.Put(context.Background(), key, bytes.Repeat([]byte("X"), int(nr.TextSize))); err != nil {
		t.Fatal(err)
	}
	srv.scrubOnce(context.Background())
	srv.m.scrubIssuesMu.Lock()
	n := srv.m.scrubIssues["demo"]
	srv.m.scrubIssuesMu.Unlock()
	if n == 0 {
		t.Fatal("scrub counted no issues for a corrupted external text")
	}
	// Watermark still advances: corruption is reported, not re-walked.
	srv.scrubMu.Lock()
	last := srv.scrub.verified["demo"]
	srv.scrubMu.Unlock()
	head, _ := repo.Head()
	if last != head {
		t.Fatalf("watermark=%d after corrupted scrub, want %d", last, head)
	}
	// And the metric line shows up in the Prometheus text.
	rec := httptest.NewRecorder()
	srv.serveMetrics(rec)
	if !strings.Contains(rec.Body.String(), `svngod_scrub_corruptions_total{repo="demo"} `) {
		t.Fatalf("scrub metric missing from /_metrics:\n%s", rec.Body.String())
	}
}

// TestScrubResumesAfterChunk: with two revisions and a tiny chunk the first
// tick verifies only the first chunk; a second tick continues.
func TestScrubResumesAfterChunk(t *testing.T) {
	srv, repo, _ := scrubFixture(t)
	if _, err := repo.CommitTxn(1, []fs.Change{
		{Path: "/trunk/next.txt", Action: fs.ActionAdd, Kind: fs.KindFile, Data: []byte("second\n")},
	}, nil); err != nil {
		t.Fatal(err)
	}
	savedChunk := scrubChunk
	defer func() { scrubChunk = savedChunk }()
	scrubChunk = 1
	srv.scrubOnce(context.Background())
	srv.scrubMu.Lock()
	after1 := srv.scrub.verified["demo"]
	srv.scrubMu.Unlock()
	srv.scrubOnce(context.Background())
	srv.scrubMu.Lock()
	after2 := srv.scrub.verified["demo"]
	srv.scrubMu.Unlock()
	head, _ := repo.Head()
	if after1 != 1 {
		t.Fatalf("watermark after tick 1 = %d, want 1", after1)
	}
	if after2 != head {
		t.Fatalf("watermark after tick 2 = %d, want %d", after2, head)
	}
}

// guard against accidental flag/JSON drift
var _ = fmt.Sprintf
var _ = time.Second
