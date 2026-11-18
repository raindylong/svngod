package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"svngod/internal/fs"
)

// Every handler that reads a body sets its own limit, but nothing bounded the
// request as a whole. That leaves two gaps:
//
//   - A handler added later with no limit of its own has no backstop.
//   - A request whose Content-Length is a lie (chunked, or a header far larger
//     than the body actually read) is never compared against anything.
//
// The bound is applied where the bytes are counted - the same wrapper the
// metrics already use - so it applies to every path by construction rather than
// by each handler remembering.

// oversizedServer builds a server with a small request-body cap and a real
// repository.
//
// The repository matters: with a missing one, handlers answer 404 before
// touching the body, so the test would pass or fail for the wrong reason. What
// is under test is the cap engaging at the read, not a routing error.
func oversizedServer(t *testing.T, limit int64) *Server {
	t.Helper()
	repo := buildRepo(t)
	return New(Config{
		BasePath:     "/svn",
		MaxBodyBytes: limit,
		OpenRepo: func(name string) (*fs.Repo, error) {
			if name == "demo" {
				return repo, nil
			}
			return nil, fs.ErrRepoNotFound
		},
	})
}

// TestRequestBodyTooLargeIsRejected fails on pre-fix code: POST /!svn/me reads a
// body with its own 1 MiB limit, so a 2 MiB request is answered by that handler
// (400 or similar) rather than being refused at the door with 413. With a global
// cap smaller than the body, the refusal must happen regardless of which handler
// would have run.
func TestRequestBodyTooLargeIsRejected(t *testing.T) {
	s := oversizedServer(t, 1024) // 1 KiB cap

	body := strings.Repeat("x", 8192)
	req := httptest.NewRequest("POST", "/svn/demo/!svn/me", strings.NewReader(body))
	req.RemoteAddr = "192.0.2.1:1234"
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body returned %d, want 413", rec.Code)
	}
}

// TestRequestBodyWithinLimitPasses is the false-positive guard: a request under
// the cap must reach the handler normally.
func TestRequestBodyWithinLimitPasses(t *testing.T) {
	s := oversizedServer(t, 1<<20) // 1 MiB cap

	req := httptest.NewRequest("POST", "/svn/demo/!svn/me", strings.NewReader("(create-txn)"))
	req.RemoteAddr = "192.0.2.1:1234"
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code == http.StatusRequestEntityTooLarge {
		t.Fatal("a small body was rejected as too large")
	}
}

// TestLyingContentLengthIsCaught: a chunked request or a Content-Length that
// understates the body must not slip past. The cap counts bytes as they are
// read, so it cannot be fooled by the header.
func TestLyingContentLengthIsCaught(t *testing.T) {
	s := oversizedServer(t, 1024)

	body := strings.Repeat("y", 8192)
	req := httptest.NewRequest("POST", "/svn/demo/!svn/me", strings.NewReader(body))
	req.ContentLength = 10 // lies: claims 10 bytes, sends 8192
	req.RemoteAddr = "192.0.2.1:1234"
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("understated Content-Length returned %d, want 413", rec.Code)
	}
}

// TestBodyCapZeroMeansUnlimited keeps the opt-out: 0 must not mean "reject
// everything".
func TestBodyCapZeroMeansUnlimited(t *testing.T) {
	s := oversizedServer(t, 0)

	body := strings.Repeat("z", 1<<16)
	req := httptest.NewRequest("POST", "/svn/demo/!svn/me", strings.NewReader(body))
	req.RemoteAddr = "192.0.2.1:1234"
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code == http.StatusRequestEntityTooLarge {
		t.Fatal("cap 0 rejected a body; 0 must mean unlimited")
	}
}

// TestBodyCapDoesNotBlockLargeUploads: the global cap must sit above the PUT
// limit, or legitimate multi-gigabyte uploads break. This runs PUT-threshold
// bookkeeping without a real repository by checking the wrapper directly.
func TestBodyCapDoesNotBlockLargeUploads(t *testing.T) {
	// The PUT path allows 64 GiB; the default global cap must be at least that.
	if defaultMaxBodyBytes < 64<<30 {
		t.Fatalf("default body cap %d is below the 64 GiB PUT limit", int64(defaultMaxBodyBytes))
	}
	if defaultMaxBodyBytes <= 0 {
		t.Fatal("default body cap must be positive")
	}
}

// TestBodyCapAppliesAtTheWrapper pins the mechanism rather than trying to drive
// every method through it.
//
// A first version walked POST/PUT/PROPFIND/REPORT/... and expected 413 from
// each. That was wrong: handlers reject those requests (404, 405, 400) before
// reading the body, so the assertion failed for routing reasons and would
// "pass" only if the test served a repository state those paths accept. The cap
// lives on the body wrapper, which every handler that reads shares - so testing
// the wrapper is both more direct and not dependent on routing.
func TestBodyCapAppliesAtTheWrapper(t *testing.T) {
	body := strings.Repeat("q", 4096)
	inner := io.NopCloser(strings.NewReader(body))
	cr := &countingReadCloser{r: inner, limit: 512}

	over := false
	cr.onOver = func() { over = true }

	buf := make([]byte, 1024)
	var total int
	for {
		n, err := cr.Read(buf)
		total += n
		if err != nil {
			break
		}
	}
	if total != 512 {
		t.Fatalf("wrapper allowed %d bytes, want exactly the limit 512", total)
	}
	if !over {
		t.Fatal("onOver never fired; the router could not answer 413")
	}
	if !cr.overLimit() {
		t.Fatal("overLimit() false after the cap was hit")
	}
}

// TestBodyCapDoesNotBreakBodylessRequests: GET/OPTIONS with no body must be
// unaffected, or every read-only client breaks.
func TestBodyCapDoesNotBreakBodylessRequests(t *testing.T) {
	s := oversizedServer(t, 512)

	for _, m := range []string{"GET", "OPTIONS"} {
		req := httptest.NewRequest(m, "/svn/demo", nil)
		req.RemoteAddr = "192.0.2.1:1234"
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		if rec.Code == http.StatusRequestEntityTooLarge {
			t.Fatalf("%s with no body was rejected as too large", m)
		}
	}
}

// TestBodyCapCountsChunkedBodies covers the case where ContentLength is -1
// (chunked): the cap must still count what arrives.
func TestBodyCapCountsChunkedBodies(t *testing.T) {
	s := oversizedServer(t, 1024)

	pr, pw := io.Pipe()
	go func() {
		// Write more than the cap; the handler should stop reading and the
		// server should answer 413 without draining everything.
		buf := make([]byte, 4096)
		for i := 0; i < 4; i++ {
			if _, err := pw.Write(buf); err != nil {
				break
			}
		}
		pw.Close()
	}()

	req := httptest.NewRequest("POST", "/svn/demo/!svn/me", pr)
	req.ContentLength = -1 // chunked
	req.RemoteAddr = "192.0.2.1:1234"
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("chunked oversized body returned %d, want 413", rec.Code)
	}
}
