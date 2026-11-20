package server

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"svngod/internal/fs"
)

// A request body has a size cap (bodylimit_test.go) but no rate floor. That
// leaves the slow-client case open: a client that trickles bytes keeps its
// connection, its goroutine and - for upload paths - an upload slot, for as long
// as it likes. ReadHeaderTimeout does not help; it stops before the body.
//
// Two shapes need different treatment, because they fail differently:
//
//   - trickle: bytes keep arriving, just very slowly. Every Read returns, so a
//     read deadline alone never fires. Only an average-rate check catches this.
//   - silent: the client stops sending entirely. Read blocks in the kernel until
//     the connection breaks, which can be many minutes. Only a read deadline
//     interrupts this; no amount of arithmetic on bytes already read helps.
//
// So the fix is both: an average floor measured on the wrapper that already
// counts body bytes, and a read deadline refreshed on each successful read.

// fakeClock drives the rate check without wall-clock waits, so the tests measure
// the rule rather than the timer.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

// TestBodyRateFloorRejectsTrickle is the core case: bytes do arrive, but the
// average falls under the floor once the grace window has passed.
func TestBodyRateFloorRejectsTrickle(t *testing.T) {
	fc := &fakeClock{t: time.Unix(1700000000, 0)}
	body := strings.NewReader(strings.Repeat("a", 4096))
	cr := &countingReadCloser{
		r:       io.NopCloser(body),
		minRate: 1024, // bytes/sec
		grace:   time.Second,
		now:     fc.now,
	}

	buf := make([]byte, 16)
	// First read inside the grace window: allowed even though 16 B/0 is under
	// any floor, because there is not enough evidence yet.
	if _, err := cr.Read(buf); err != nil {
		t.Fatalf("read inside grace window failed: %v", err)
	}
	// 16 bytes in 10s is 1.6 B/s, far under 1024 B/s.
	fc.advance(10 * time.Second)
	if _, err := cr.Read(buf); err != errSlowBody {
		t.Fatalf("trickled read returned %v, want errSlowBody", err)
	}
	if !cr.tooSlow() {
		t.Fatal("tooSlow() false after the floor was violated")
	}
}

// TestBodyRateFloorAllowsBurstyButFastTraffic is the false-positive guard: a
// request that is momentarily slow but keeps a healthy average must pass. Real
// clients stall on disk, on GC, behind a proxy.
func TestBodyRateFloorAllowsBurstyButFastTraffic(t *testing.T) {
	fc := &fakeClock{t: time.Unix(1700000000, 0)}
	body := strings.NewReader(strings.Repeat("b", 1<<20))
	cr := &countingReadCloser{
		r:       io.NopCloser(body),
		minRate: 1024,
		grace:   time.Second,
		now:     fc.now,
	}

	buf := make([]byte, 64<<10)
	var total int
	for {
		n, err := cr.Read(buf)
		total += n
		if err != nil {
			break
		}
		// 64 KiB per 10s is ~6.5 KiB/s: above the floor.
		fc.advance(10 * time.Second)
	}
	if total != 1<<20 {
		t.Fatalf("read %d bytes, want all %d", total, 1<<20)
	}
	if cr.tooSlow() {
		t.Fatal("a body averaging above the floor was flagged as too slow")
	}
}

// TestBodyRateFloorDisabledIsNoOp keeps the opt-out: minRate 0 must not inspect
// the clock at all.
func TestBodyRateFloorDisabledIsNoOp(t *testing.T) {
	fc := &fakeClock{t: time.Unix(1700000000, 0)}
	cr := &countingReadCloser{
		r:     io.NopCloser(strings.NewReader(strings.Repeat("c", 4096))),
		grace: time.Second,
		now:   fc.now,
	}
	buf := make([]byte, 32)
	if _, err := cr.Read(buf); err != nil {
		t.Fatalf("read failed: %v", err)
	}
	fc.advance(time.Hour)
	if _, err := cr.Read(buf); err != nil {
		t.Fatalf("disabled floor rejected a read: %v", err)
	}
}

// slowBodyServer builds a server with a rate floor short enough to exercise
// through the real router.
func slowBodyServer(t *testing.T, minRate int64) *Server {
	t.Helper()
	repo := buildRepo(t)
	return New(Config{
		BasePath:         "/svn",
		MinBodyRate:      minRate,
		BodyRateGrace:    time.Millisecond,
		BodyStallTimeout: time.Hour, // this test is about the average, not the stall
		OpenRepo: func(name string) (*fs.Repo, error) {
			if name == "demo" {
				return repo, nil
			}
			return nil, fs.ErrRepoNotFound
		},
	})
}

// trickleReader delivers one byte per Read, with a real pause so elapsed time
// (not an injected clock) drives the check end-to-end.
type trickleReader struct {
	left  int
	pause time.Duration
}

func (r *trickleReader) Read(p []byte) (int, error) {
	if r.left <= 0 {
		return 0, io.EOF
	}
	time.Sleep(r.pause)
	if len(p) == 0 {
		return 0, nil
	}
	p[0] = 'x'
	r.left--
	return 1, nil
}

// TestSlowBodyIsAnsweredWith408 checks the router's mapping: a body under the
// rate floor must not be reported as "too large" (413) - the client sent a legal
// amount of data, just too slowly. 408 is the honest answer.
func TestSlowBodyIsAnsweredWith408(t *testing.T) {
	s := slowBodyServer(t, 1<<20) // 1 MiB/s; the trickle does ~1 KiB/s

	req := httptest.NewRequest("POST", "/svn/demo/!svn/me", &trickleReader{left: 4096, pause: time.Millisecond})
	req.ContentLength = -1
	req.RemoteAddr = "192.0.2.1:1234"
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestTimeout {
		t.Fatalf("slow body returned %d, want 408", rec.Code)
	}
}

// TestSlowBodyIsNotReportedAsTooLarge pins the distinction in the other
// direction: the rate floor must never produce a 413 once it has fired, or
// operators will chase the wrong cause.
func TestSlowBodyIsNotReportedAsTooLarge(t *testing.T) {
	s := slowBodyServer(t, 1<<20)

	req := httptest.NewRequest("POST", "/svn/demo/!svn/me", &trickleReader{left: 4096, pause: time.Millisecond})
	req.ContentLength = -1
	req.RemoteAddr = "192.0.2.1:1234"
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code == http.StatusRequestEntityTooLarge {
		t.Fatal("a slow body was reported as too large")
	}
}

// TestFastBodyIsNotAffectedByRateFloor: turning the floor on must not touch a
// normal request.
func TestFastBodyIsNotAffectedByRateFloor(t *testing.T) {
	s := slowBodyServer(t, 1024)

	req := httptest.NewRequest("POST", "/svn/demo/!svn/me", strings.NewReader("(create-txn)"))
	req.RemoteAddr = "192.0.2.1:1234"
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code == http.StatusRequestTimeout {
		t.Fatalf("a fast body was rejected with %d", rec.Code)
	}
}

// TestBodyStallDeadlineIsRefreshed covers the silent-client half. The wrapper
// must push a deadline out on every successful read, which is what interrupts a
// Read that would otherwise block indefinitely.
func TestBodyStallDeadlineIsRefreshed(t *testing.T) {
	fc := &fakeClock{t: time.Unix(1700000000, 0)}
	var deadlines []time.Time
	cr := &countingReadCloser{
		r:     io.NopCloser(strings.NewReader(strings.Repeat("d", 4096))),
		grace: time.Second,
		stall: 30 * time.Second,
		now:   fc.now,
		setReadDeadline: func(d time.Time) error {
			deadlines = append(deadlines, d)
			return nil
		},
	}

	buf := make([]byte, 64)
	for i := 0; i < 3; i++ {
		if _, err := cr.Read(buf); err != nil {
			t.Fatalf("read %d failed: %v", i, err)
		}
		fc.advance(5 * time.Second)
	}
	if len(deadlines) < 3 {
		t.Fatalf("read deadline set %d times for 3 reads, want >= 3", len(deadlines))
	}
	// Each deadline must be ahead of the moment it was set: a deadline in the
	// past would abort the next read even though the client is sending.
	for i, d := range deadlines {
		if !d.After(fc.now().Add(-25 * time.Second)) {
			t.Fatalf("deadline %d (%v) is not a fresh window", i, d)
		}
	}
}

// TestBodyStallDeadlineDisabledWhenZero: 0 must mean "no deadline", so an
// operator can opt out on a network where long pauses are normal.
func TestBodyStallDeadlineDisabledWhenZero(t *testing.T) {
	cr := &countingReadCloser{
		r:     io.NopCloser(strings.NewReader("abc")),
		grace: time.Second,
		setReadDeadline: func(time.Time) error {
			t.Fatal("deadline set although the stall timeout is disabled")
			return nil
		},
	}
	buf := make([]byte, 8)
	if _, err := cr.Read(buf); err != nil && err != io.EOF {
		t.Fatalf("read failed: %v", err)
	}
}

// TestStalledBodyIsCutOffOnARealConnection is the end-to-end half of the stall
// case, and the only test that proves the deadline reaches the socket: the
// in-memory ResponseRecorder has no connection, so a fake setter can only show
// that armStall was called, not that a blocked Read is interrupted.
//
// The client opens a request, sends headers and part of the body, then goes
// quiet. Without a read deadline the handler blocks until the test's own
// timeout; with one the server answers and closes.
func TestStalledBodyIsCutOffOnARealConnection(t *testing.T) {
	repo := buildRepo(t)
	s := New(Config{
		BasePath:         "/svn",
		BodyStallTimeout: 150 * time.Millisecond,
		BodyRateGrace:    time.Hour, // isolate the stall path from the rate floor
		OpenRepo: func(name string) (*fs.Repo, error) {
			if name == "demo" {
				return repo, nil
			}
			return nil, fs.ErrRepoNotFound
		},
	})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	conn, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Announce more body than we send, so the handler waits for the rest.
	if _, err := fmt.Fprintf(conn, "POST /svn/demo/!svn/me HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\npartial"); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Never send the rest. The server must give up on its own.
	//
	// The assertion is the error type, not just that the read returned: waiting
	// out this side's own deadline also "returns", so a timeout here would pass
	// a test that proves nothing. A server-side cutoff shows up as EOF because
	// the connection is closed, and it must happen before the client's deadline.
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	start := time.Now()
	_, err = io.ReadAll(conn)
	elapsed := time.Since(start)
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Fatalf("connection still open after %v: the server never cut the stalled body off", elapsed)
	}
	if elapsed > time.Second {
		t.Fatalf("stalled body survived %v, want a cut-off near the 150ms stall window", elapsed)
	}
}

// TestBodyDeadlineIsClearedOnClose pins the ownership rule: the deadline lives on
// the connection, so the wrapper that sets it clears it. net/http happens to
// reset the deadline when the next request starts, and an earlier version of this
// file claimed that made the clearing unnecessary - but relying on it would mean
// the wrapper's correctness depends on a detail of net/http's keep-alive path.
func TestBodyDeadlineIsClearedOnClose(t *testing.T) {
	var cleared bool
	cr := &countingReadCloser{
		r:     io.NopCloser(strings.NewReader("e")),
		grace: time.Second,
		stall: 30 * time.Second,
		setReadDeadline: func(d time.Time) error {
			if d.IsZero() {
				cleared = true
			}
			return nil
		},
	}
	buf := make([]byte, 8)
	if _, err := cr.Read(buf); err != nil && err != io.EOF {
		t.Fatalf("read: %v", err)
	}
	if err := cr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !cleared {
		t.Fatal("Close did not clear the connection read deadline")
	}
}
