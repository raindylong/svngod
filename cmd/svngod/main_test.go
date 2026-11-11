package main

import (
	"context"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

// TestServeWithShutdownDrains: cancelling the context stops the listener but
// lets an in-flight request finish (a commit must not be killed mid-publish).
func TestServeWithShutdownDrains(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	var completed atomic.Bool
	hs := &http.Server{
		Addr: addr,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(300 * time.Millisecond)
			completed.Store(true)
			w.Write([]byte("ok"))
		}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- serveWithShutdown(ctx, hs, "", "") }()

	// Wait for the listener, then start a request and cancel while it runs.
	for i := 0; i < 100; i++ {
		c, derr := net.Dial("tcp", addr)
		if derr == nil {
			c.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	go func() {
		time.Sleep(80 * time.Millisecond)
		cancel()
	}()
	resp, err := http.Get("http://" + addr + "/")
	if err != nil {
		t.Fatalf("in-flight request failed: %v", err)
	}
	resp.Body.Close()
	if !completed.Load() {
		t.Fatal("handler was cut off by shutdown")
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("serveWithShutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not stop after shutdown")
	}
}
