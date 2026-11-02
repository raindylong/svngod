package store

import (
	"context"
	"fmt"
	"testing"
)

// TestFastRecoveryCopiesPendingDelta: a mirror that was whole when it went
// down only misses the keys written while it was out of service. Recovery must
// copy exactly that delta (no full listing) and put the mirror back in service
// only after it holds every key and the newest HEAD.
func TestFastRecoveryCopiesPendingDelta(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	for _, s := range []Store{a, b} {
		if err := s.Put(ctx, "repos/x/current", []byte("head=1")); err != nil {
			t.Fatal(err)
		}
	}
	m := NewMirror([]Store{a, b}, WithRepoPrefix("repos/x"), WithMutableOrder(compareCurrentBytes))
	defer m.Close()
	m.SetControlKey("repos/x/current")

	// Simulate the mirror going down without starting the async recovery loop,
	// so the test controls when recovery runs.
	m.mu.Lock()
	m.healthy[1] = false
	m.wholeAtDown[1] = true
	m.mu.Unlock()

	const n = 50
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("repos/x/texts/t%03d", i)
		if err := m.Put(ctx, key, []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok, _ := b.Head(ctx, "repos/x/texts/t000"); ok {
		t.Fatal("standby received writes while out of service")
	}
	if err := m.Put(ctx, "repos/x/current", []byte("head=2")); err != nil {
		t.Fatal(err)
	}

	if !m.tryFastRecover() {
		t.Fatal("fast recovery reported failure for a plain short outage")
	}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("repos/x/texts/t%03d", i)
		if _, ok, _ := b.Head(ctx, key); !ok {
			t.Fatalf("fast recovery missed %s", key)
		}
	}
	if cur, err := b.Get(ctx, "repos/x/current", nil); err != nil || string(cur) != "head=2" {
		t.Fatalf("standby HEAD = %q, %v; want head=2", cur, err)
	}
	if !m.isHealthy(1) {
		t.Fatal("mirror not returned to service after fast recovery")
	}
}

// TestFastRecoveryDropsDeletedPending: a key recorded while the mirror was
// down can be deleted before recovery runs (an abandoned upload session). The
// mirror must be healed anyway; retrying the missing key forever used to keep
// it out of service permanently.
func TestFastRecoveryDropsDeletedPending(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	for _, s := range []Store{a, b} {
		if err := s.Put(ctx, "repos/x/current", []byte("head=1")); err != nil {
			t.Fatal(err)
		}
	}
	m := NewMirror([]Store{a, b}, WithRepoPrefix("repos/x"), WithMutableOrder(compareCurrentBytes))
	defer m.Close()
	m.SetControlKey("repos/x/current")
	m.mu.Lock()
	m.healthy[1] = false
	m.wholeAtDown[1] = true
	m.mu.Unlock()

	if err := m.Put(ctx, "repos/x/txn/abc/data/1", []byte("gone")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, "repos/x/texts/keep", []byte("keep")); err != nil {
		t.Fatal(err)
	}
	// The session is abandoned and its body is deleted from the primary.
	if err := a.Delete(ctx, []string{"repos/x/txn/abc/data/1"}); err != nil {
		t.Fatal(err)
	}

	if !m.tryFastRecover() {
		t.Fatal("fast recovery failed because of a key that no longer exists")
	}
	if _, ok, _ := b.Head(ctx, "repos/x/texts/keep"); !ok {
		t.Fatal("fast recovery did not copy the surviving key")
	}
	if !m.isHealthy(1) {
		t.Fatal("mirror not returned to service")
	}
}

// TestFastRecoveryRefusesOverflow: once the recorded delta is unbounded the
// mirror must fall back to a full repair, never guess that the delta is whole.
func TestFastRecoveryRefusesOverflow(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	for _, s := range []Store{a, b} {
		if err := s.Put(ctx, "repos/x/current", []byte("head=1")); err != nil {
			t.Fatal(err)
		}
	}
	m := NewMirror([]Store{a, b}, WithRepoPrefix("repos/x"))
	defer m.Close()
	m.SetControlKey("repos/x/current")
	m.mu.Lock()
	m.healthy[1] = false
	m.wholeAtDown[1] = true
	m.pendingOverflow[1] = true
	m.mu.Unlock()
	if m.tryFastRecover() {
		t.Fatal("fast recovery accepted an overflowed delta")
	}
}
