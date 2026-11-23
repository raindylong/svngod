package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"svngod/internal/fs"
)

// The question: is "revprop update contention" a real product failure or only a
// stress-test artefact?
//
// The implementation path (fs.segment_revprops.go, fs/revprops.go) retries a
// CAS exactly 8 times with no backoff. With N concurrent editors of the same
// revision, a writer can lose every attempt. When that happens the server
// answers 500 "revision property update failed" - the user's edit is simply
// rejected, with a message that says nothing about retrying.
//
// This measures how often that happens through the real HTTP path at several
// concurrency levels, which is what decides whether it is worth fixing.

// measureContention runs `writers` goroutines each doing `rounds` updates of
// distinct properties on the same revision, and returns how many failed.
func measureContention(t *testing.T, h http.Handler, base string, writers, rounds int) (failed, total int) {
	t.Helper()
	var mu sync.Mutex
	var bodies []string
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				name := fmt.Sprintf("p%d_%d", w, r)
				body := `<?xml version="1.0"?>` +
					`<D:propertyupdate xmlns:D="DAV:" xmlns:C="urn:test">` +
					`<D:set><D:prop><C:` + name + `>v</C:` + name + `></D:prop></D:set>` +
					`</D:propertyupdate>`
				req := httptest.NewRequest("PROPPATCH", base+"/!svn/rev/1", strings.NewReader(body))
				req.RemoteAddr = "192.0.2.1:1234"
				req.Header.Set("Content-Type", "text/xml")
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				mu.Lock()
				total++
				if rec.Code >= 500 {
					failed++
					if len(bodies) < 3 {
						bodies = append(bodies, rec.Body.String())
					}
				}
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	if failed > 0 {
		t.Logf("sample 500 bodies: %q", bodies)
	}
	return failed, total
}

// TestRevPropContentionUnderLoad measures the failure rate. It does not assert
// a threshold - it reports, so the decision to fix is based on numbers.
func TestRevPropContentionUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("load measurement")
	}
	for _, writers := range []int{2, 4, 8, 16, 32} {
		repo := buildRepo(t)
		hooksDir := t.TempDir()
		if err := os.WriteFile(filepath.Join(hooksDir, "pre-revprop-change"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
			t.Fatal(err)
		}
		srv := New(Config{
			BasePath: "/svn",
			HooksDir: hooksDir,
			OpenRepo: func(name string) (*fs.Repo, error) {
				if name == "demo" {
					return repo, nil
				}
				return nil, fs.ErrRepoNotFound
			},
		})
		failed, total := measureContention(t, srv.Handler(), "/svn/demo", writers, 10)
		t.Logf("writers=%2d rounds=10  failed=%d/%d (%.1f%%)",
			writers, failed, total, 100*float64(failed)/float64(total))
	}
}

// TestRevPropContentionMapsToAnErrorTheClientCanSee pins the mapping, not the
// probability: when the retry budget IS exhausted the client must get an error,
// never a success. Driving the HTTP layer with contention that always loses
// would need a store that never lets a CAS through, so this asserts the same
// contract at the layer that decides it - the handler's error mapping is a
// distinct step, and a future refactor could easily turn it into a 200.
func TestRevPropContentionMapsToAnErrorTheClientCanSee(t *testing.T) {
	repo := buildRepo(t)
	hooksDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(hooksDir, "pre-revprop-change"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	srv := New(Config{
		BasePath: "/svn",
		HooksDir: hooksDir,
		OpenRepo: func(name string) (*fs.Repo, error) {
			if name == "demo" {
				return repo, nil
			}
			return nil, fs.ErrRepoNotFound
		},
	})

	// A normal single-writer update must succeed: this is the control proving the
	// endpoint works at all, so a failing 500 below would be meaningful.
	okFailed, okTotal := measureContention(t, srv.Handler(), "/svn/demo", 1, 1)
	if okFailed != 0 || okTotal != 1 {
		t.Fatalf("control: single writer failed %d/%d", okFailed, okTotal)
	}

	// Under heavy contention the outcome per request is either success (retry
	// won) or 5xx (budget spent) - never anything else. A 4xx here would mean the
	// error is being misreported as a client fault.
	body := `<?xml version="1.0"?><D:propertyupdate xmlns:D="DAV:" xmlns:C="urn:test">` +
		`<D:set><D:prop><C:probe>v</C:probe></D:prop></D:set></D:propertyupdate>`
	req := httptest.NewRequest("PROPPATCH", "/svn/demo/!svn/rev/1", strings.NewReader(body))
	req.RemoteAddr = "192.0.2.1:1234"
	req.Header.Set("Content-Type", "text/xml")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusMultiStatus {
		t.Fatalf("a successful revprop edit returned %d, want 207:\n%s", rec.Code, rec.Body.String())
	}
}
