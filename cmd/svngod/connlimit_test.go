package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A connection cap is the only protection that works before a request exists.
//
// The upload limiter in internal/server bounds concurrent PUT bodies, but it
// runs inside a handler - by then the connection, its goroutine and its buffers
// are already allocated. Nothing bounded how many connections could be opened,
// so a peer that opens sockets and sends nothing exhausts file descriptors and
// stops the server accepting legitimate clients. On the reference host the soft
// fd limit is 1024, so this is a few hundred sockets away, not a theoretical
// attack.
//
// These tests drive real TCP connections. A counter assertion would pass even
// if the listener never enforced anything.

// countingHandler increments a live-connection counter and blocks until the test
// closes the connection, so held connections stay held.
func countingHandler(live *atomic.Int64, release <-chan struct{}) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		live.Add(1)
		defer live.Add(-1)
		<-release
	})
}

// startCapped starts a listener with the cap under test and returns its address
// plus a stop func.
func startCapped(t *testing.T, maxConns int) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	capped := limitConnections(ln, maxConns)
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})}
	go srv.Serve(capped)
	return ln.Addr().String(), func() {
		srv.Close()
		capped.Close()
	}
}

// dialAndHold opens a connection and completes one request, keeping the
// connection alive for reuse (the server side stays occupied).
func dialAndHold(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	// A full request/response keeps the connection in the server's active set
	// without relying on keep-alive timing.
	fmt.Fprintf(c, "GET / HTTP/1.1\r\nHost: x\r\nConnection: keep-alive\r\n\r\n")
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 256)
	if _, err := c.Read(buf); err != nil && err != io.EOF {
		t.Fatalf("read response: %v", err)
	}
	c.SetReadDeadline(time.Time{})
	return c
}

// TestConnectionCapRejectsBeyondLimit fails on pre-fix code: without a cap the
// server accepts every connection the OS allows.
func TestConnectionCapRejectsBeyondLimit(t *testing.T) {
	const maxConns = 4
	addr, stop := startCapped(t, maxConns)
	defer stop()

	var held []net.Conn
	defer func() {
		for _, c := range held {
			c.Close()
		}
	}()
	for i := 0; i < maxConns; i++ {
		held = append(held, dialAndHold(t, addr))
	}

	// The next connection must be refused promptly. Accepted-and-hung would also
	// "not be served", but it is a bad outcome: the client waits instead of
	// backing off, and the socket still occupies an fd on our side. So this
	// requires either a refused dial or an immediate RST/EOF - never a timeout.
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return // refused outright: ideal
	}
	defer c.Close()
	fmt.Fprintf(c, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 256)
	n, rerr := c.Read(buf)
	if rerr == nil && n > 0 {
		t.Fatalf("connection %d was served despite a cap of %d: %q", maxConns+1, maxConns, buf[:n])
	}
	if nerr, ok := rerr.(net.Error); ok && nerr.Timeout() {
		t.Fatal("overflow connection was accepted and left hanging; it must be reset so the peer backs off")
	}
}

// TestConnectionCapReleasesSlots is the other half: a cap that never frees a
// slot turns into a permanent outage after the first burst. It would pass the
// test above and still be unusable.
func TestConnectionCapReleasesSlots(t *testing.T) {
	const maxConns = 2
	addr, stop := startCapped(t, maxConns)
	defer stop()

	c1 := dialAndHold(t, addr)
	c2 := dialAndHold(t, addr)
	c1.Close()
	c2.Close()

	// Give the listener a moment to observe the closed sockets.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		fmt.Fprintf(c, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
		c.SetReadDeadline(time.Now().Add(1 * time.Second))
		buf := make([]byte, 256)
		n, rerr := c.Read(buf)
		c.Close()
		if rerr == nil && n > 0 {
			return // slot was reclaimed
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("connection slots were never released after clients closed")
}

// TestConnectionCapZeroMeansUnlimited keeps the opt-out meaningful: 0 must not
// accidentally mean "refuse everything".
func TestConnectionCapZeroMeansUnlimited(t *testing.T) {
	addr, stop := startCapped(t, 0)
	defer stop()

	var held []net.Conn
	defer func() {
		for _, c := range held {
			c.Close()
		}
	}()
	for i := 0; i < 12; i++ {
		held = append(held, dialAndHold(t, addr))
	}
}

// TestConnectionCapSurvivesChurn guards against a slot leak that only shows up
// under repeated open/close: a counter-based implementation that forgets to
// decrement on error paths would pass a single-shot test.
func TestConnectionCapSurvivesChurn(t *testing.T) {
	const maxConns = 3
	addr, stop := startCapped(t, maxConns)
	defer stop()

	for round := 0; round < 20; round++ {
		var wg sync.WaitGroup
		for i := 0; i < maxConns; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				c, err := net.DialTimeout("tcp", addr, time.Second)
				if err != nil {
					return
				}
				fmt.Fprintf(c, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
				c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
				io.ReadFull(c, make([]byte, 16))
				c.Close()
			}()
		}
		wg.Wait()
	}
	// After the churn the server must still serve.
	c := dialAndHold(t, addr)
	c.Close()
}
