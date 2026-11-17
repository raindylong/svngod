package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"svngod/internal/fs"
)

// A panic in one request handler must not take down the process. svngod serves
// many repositories from one process, so an unhandled panic in any handler is a
// cross-tenant outage: every other repository on the node stops serving, and the
// only trace is a goroutine dump with no repository context.
//
// The panic is injected through Config.OpenRepo, which OPTIONS and every other
// repo-scoped method reach, so these tests exercise the real dispatch path
// instead of a special test hook.

// panickingServer builds a server whose repository lookup panics once.
func panickingServer(t *testing.T, once *bool) *Server {
	t.Helper()
	return New(Config{
		BasePath: "/svn",
		OpenRepo: func(name string) (*fs.Repo, error) {
			if !*once {
				*once = true
				panic("injected handler panic")
			}
			return nil, fs.ErrRepoNotFound
		},
	})
}

// TestRouteRecoversFromHandlerPanic fails on pre-fix code: the panic propagates
// out of ServeHTTP, so a test (and the real server) sees a crash rather than a
// 500.
func TestRouteRecoversFromHandlerPanic(t *testing.T) {
	once := false
	s := panickingServer(t, &once)

	rec := httptest.NewRecorder()
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic escaped the router: %v", r)
			}
		}()
		s.Handler().ServeHTTP(rec, httptest.NewRequest("OPTIONS", "/svn/anything", nil))
	}()

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("panicking request returned %d, want 500", rec.Code)
	}
}

// TestRouteKeepsServingAfterPanic is the isolation guarantee: after one request
// panics, the next one must still be handled.
func TestRouteKeepsServingAfterPanic(t *testing.T) {
	once := false
	s := panickingServer(t, &once)

	rec1 := httptest.NewRecorder()
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic escaped the router: %v", r)
			}
		}()
		s.Handler().ServeHTTP(rec1, httptest.NewRequest("OPTIONS", "/svn/anything", nil))
	}()

	// The second request reaches OpenRepo again and now returns ErrRepoNotFound,
	// which must be handled as an ordinary error rather than a crash.
	rec2 := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec2, httptest.NewRequest("OPTIONS", "/svn/anything", nil))
	if rec2.Code == http.StatusInternalServerError {
		t.Fatal("the request after a panic also failed: the server did not recover")
	}
}

// TestRecoverDoesNotOverwriteCommittedResponse guards the subtler half: once a
// handler has flushed a status line, the recovery path must not write a second
// one into the same response. statusWriter.committed is what tells them apart.
func TestRecoverDoesNotOverwriteCommittedResponse(t *testing.T) {
	for _, c := range []struct {
		name string
		sw   *statusWriter
		want bool
	}{
		{"untouched", &statusWriter{ResponseWriter: httptest.NewRecorder()}, false},
		{"header written", &statusWriter{ResponseWriter: httptest.NewRecorder(), code: 200}, true},
		{"body written", &statusWriter{ResponseWriter: httptest.NewRecorder(), n: 7}, true},
	} {
		if got := c.sw.committed(); got != c.want {
			t.Fatalf("%s: committed()=%v, want %v", c.name, got, c.want)
		}
	}
}

// TestGoSafeRunsWorkAndSurvivesPanic pins both halves of goSafe: it must run the
// work, and it must not let a panic reach the runtime.
func TestGoSafeRunsWorkAndSurvivesPanic(t *testing.T) {
	ran := make(chan struct{})
	goSafe("test-runs", func() { close(ran) })
	<-ran

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("goSafe let a panic escape: %v", r)
			}
		}()
		finished := make(chan struct{})
		go func() {
			defer close(finished)
			goSafe("test-panics", func() { panic("boom") })
		}()
		<-finished
	}()
}
