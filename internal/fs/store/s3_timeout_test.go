package store

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Every S3 call takes the caller's ctx, and internal/fs passes
// context.Background() in many places (reads, head, uuid). With no deadline on
// that ctx, one failure mode is unbounded: a gateway that sends response headers
// and then stalls the body. http.Transport.ResponseHeaderTimeout does not cover
// it (the timer stops when headers arrive), and minio's retry layer only retries
// transport errors, not a read that never returns. The request then hangs
// forever, holding a goroutine and, in the commit path, the commit lease.
//
// These tests use a stub gateway so the stall is deterministic.

// stallingGateway answers every request with headers and a partial body, then
// stops writing without closing. A real gateway does this when its backend is
// wedged.
func stallingGateway(t *testing.T, bodyBytes int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A valid-looking XML error is what minio inspects for 4xx; here only the
		// body stall matters, so keep it minimal.
		w.Header().Set("Content-Type", "application/xml")
		w.Header().Set("Content-Length", "100000") // promise more than we send
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(make([]byte, bodyBytes))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// Stall: wait for the client to give up. The test server closes this
		// only when the handler returns, which is what we are testing against.
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	return srv
}

// newStubS3 builds an S3Store pointed at a given HTTP server.
func newStubS3(t *testing.T, rawURL, bucket string) *S3Store {
	t.Helper()
	host := strings.TrimPrefix(rawURL, "http://")
	host = strings.TrimPrefix(host, "https://")
	st, err := NewS3Store(S3Config{
		Endpoint: host,
		Bucket:   bucket,
		// A bucket probe runs at construction; the stub answers it with a stall,
		// which NewS3Store tolerates (see the redirect/transport fallback).
		AccessKey: "ak", SecretKey: "sk", UseSSL: false,
	})
	if err != nil {
		t.Fatalf("NewS3Store: %v", err)
	}
	return st
}

// TestS3GetDoesNotHangOnStalledBody fails on the current code: Get reads the
// body with whatever ctx the caller supplied. With Background() there is no
// deadline, so this call never returns.
func TestS3GetDoesNotHangOnStalledBody(t *testing.T) {
	srv := stallingGateway(t, 10)
	st := newStubS3(t, srv.URL, "bkt")

	done := make(chan struct{})
	go func() {
		defer close(done)
		// context.Background() mirrors internal/fs callers.
		_, _ = st.Get(context.Background(), "some/key", nil)
	}()

	select {
	case <-done:
	case <-time.After(readIdleTimeout + 15*time.Second):
		t.Fatalf("Get never returned on a stalled body within %v; the read deadline is not applied", readIdleTimeout)
	}
}

// TestS3HeadHasDeadline covers Head: it also uses the caller's ctx, and a
// stalled StatObject response has the same problem.
func TestS3HeadHasDeadline(t *testing.T) {
	srv := stallingGateway(t, 10)
	st := newStubS3(t, srv.URL, "bkt")

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = st.Head(context.Background(), "some/key")
	}()

	select {
	case <-done:
	case <-time.After(readIdleTimeout + 15*time.Second):
		t.Fatalf("Head never returned on a stalled response within %v", readIdleTimeout)
	}
}

// TestS3GetHonoursCallerDeadlineForStalledBody: the fix must not ignore a caller
// deadline that is shorter than the store's own.
func TestS3GetHonoursCallerDeadlineForStalledBody(t *testing.T) {
	srv := stallingGateway(t, 10)
	st := newStubS3(t, srv.URL, "bkt")

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = st.Get(ctx, "some/key", nil)
	}()
	select {
	case <-done:
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Fatalf("Get took %v; it did not honour the 300ms caller deadline", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Get ignored the caller's deadline entirely")
	}
}

// TestS3GetReturnsForHealthyResponse is the false-positive guard: a normal
// response must complete quickly rather than tripping the idle timer.
//
// This asserts completion, not payload contents: minio validates ETag, size and
// checksum against the request it signed, which a hand-written stub cannot
// reproduce faithfully. Payload correctness over a real gateway is covered by
// TestRealS3RoundTrip (build-tagged, needs the lab), and by the existing
// integration tests.
func TestS3GetReturnsForHealthyResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/bkt/") || strings.HasSuffix(r.URL.Path, "/bkt") {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Content-Length", "9")
		w.Header().Set("ETag", `"5d41402abc4b2a76b9719d911017c592"`)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "hello svn")
	}))
	t.Cleanup(srv.Close)

	st := newStubS3(t, srv.URL, "bkt")
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = st.Get(context.Background(), "k", nil)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a healthy response did not complete; the idle timer is misfiring")
	}
}

// TestS3DialDeadlineIsShort keeps the existing dial bound intact: a blackholed
// address must fail fast rather than waiting for the OS default.
func TestS3DialDeadlineIsShort(t *testing.T) {
	// 192.0.2.0/24 is TEST-NET-1: routable nowhere, so connect hangs until a
	// timeout fires.
	st, err := NewS3Store(S3Config{
		Endpoint: "192.0.2.1:9000", Bucket: "bkt", AccessKey: "ak", SecretKey: "sk",
	})
	if err != nil {
		t.Skipf("constructor refused unreachable endpoint: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = st.Get(context.Background(), "k", nil)
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("a blackholed endpoint hung for over 15s; the dial deadline is not applied")
	}
	_ = net.Dial // keep the import meaningful if assertions change
}
