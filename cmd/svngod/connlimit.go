package main

import (
	"net"
	"sync"
)

// connLimiter caps simultaneous TCP connections.
//
// This is the only limit that applies before a request exists. The upload
// limiter inside internal/server bounds concurrent PUT bodies, but it runs in a
// handler - the socket, goroutine and read buffers are already allocated by
// then. Nothing bounded how many sockets could be opened, so a peer that opens
// connections and sends nothing could exhaust file descriptors and stop the
// server accepting real clients. The reference host has a soft fd limit of 1024,
// so that is a few hundred sockets away.
//
// Unlike x/net/netutil.LimitListener, which blocks inside Accept when full (so a
// client hangs until a slot frees), this accepts and immediately closes the
// excess connection. A closed connection tells the peer to back off now, and it
// keeps Accept non-blocking so shutdown stays prompt.
type connLimiter struct {
	net.Listener
	sem chan struct{}

	mu     sync.Mutex
	closed bool
}

// limitConnections wraps ln. maxConns <= 0 returns ln unchanged (no limit).
func limitConnections(ln net.Listener, maxConns int) net.Listener {
	if maxConns <= 0 {
		return ln
	}
	return &connLimiter{Listener: ln, sem: make(chan struct{}, maxConns)}
}

func (l *connLimiter) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		select {
		case l.sem <- struct{}{}:
			return &limitedConn{Conn: c, release: l.release}, nil
		default:
			// At capacity: refuse this one rather than queue it. Closing sends
			// FIN/RST so the client learns immediately instead of waiting.
			c.Close()
			// Keep accepting: the next peer may be a legitimate client whose
			// connection arrives after one closes.
			continue
		}
	}
}

func (l *connLimiter) release() {
	<-l.sem
}

// Close closes the listener. Connections already accepted are owned by the
// HTTP server and closed by it.
func (l *connLimiter) Close() error {
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()
	return l.Listener.Close()
}

// limitedConn returns its slot exactly once, however the connection ends. A
// double release would silently inflate the effective limit, which is the kind
// of bug that only shows up after a long run.
type limitedConn struct {
	net.Conn
	release func()
	once    sync.Once
}

func (c *limitedConn) Close() error {
	c.once.Do(c.release)
	return c.Conn.Close()
}
