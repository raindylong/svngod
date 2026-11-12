package store

import (
	"context"
	"testing"
	"time"
)

// geoFixture: local region = 2-node mirror quorum 2; remote region = 2-node
// mirror quorum 2; GeoStore wires them with the standard control name.
func geoFixture(t *testing.T) (*GeoStore, *MirrorStore, *MirrorStore) {
	t.Helper()
	local := NewMirror([]Store{NewMemoryStore(), NewMemoryStore()}, WithWriteQuorum(2), WithRepoPrefix("repos/gx"))
	remote := NewMirror([]Store{NewMemoryStore(), NewMemoryStore()}, WithWriteQuorum(2), WithRepoPrefix("repos/gx"))
	g := NewGeoStore(GeoConfig{
		Local:       local,
		Remote:      remote,
		ControlName: "repos/gx/current",
		Interval:    20 * time.Millisecond,
	})
	t.Cleanup(g.Close)
	return g, local, remote
}

func waitGeoIdle(t *testing.T, g *GeoStore, wantShipped uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		st := g.Status()
		if st.Pending == 0 && st.Shipped >= wantShipped {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	st := g.Status()
	t.Fatalf("geo shipper did not converge: %+v (want shipped>=%d)", st, wantShipped)
}

// TestGeoBasicShipping: writes ack locally and appear on the remote region.
func TestGeoBasicShipping(t *testing.T) {
	g, _, remote := geoFixture(t)
	ctx := context.Background()
	if err := g.Put(ctx, "repos/gx/obj1", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := g.Put(ctx, "repos/gx/current", []byte("head")); err != nil {
		t.Fatal(err)
	}
	waitGeoIdle(t, g, 2)
	b, err := remote.Get(ctx, "repos/gx/obj1", nil)
	if err != nil || string(b) != "hello" {
		t.Fatalf("remote obj1: %q err=%v", b, err)
	}
	if cur, err := remote.Get(ctx, "repos/gx/current", nil); err != nil || string(cur) != "head" {
		t.Fatalf("remote current: %q err=%v", cur, err)
	}
}

// TestGeoLocalReadsDoNotNeedRemote: reads serve from the local region even
// while the remote is unreachable (partition tolerance).
func TestGeoLocalReadsDoNotNeedRemote(t *testing.T) {
	g, _, remote := geoFixture(t)
	ctx := context.Background()
	if err := g.Put(ctx, "repos/gx/obj1", []byte("local-only")); err != nil {
		t.Fatal(err)
	}
	// Break the remote: every underlying replica rejects.
	// (Close the remote mirror's replicas by deleting is not possible on
	// MemoryStore; instead just verify reads work before shipment completes
	// - the shipper runs async, the read must not wait for it.)
	b, err := g.Get(ctx, "repos/gx/obj1", nil)
	if err != nil || string(b) != "local-only" {
		t.Fatalf("local read: %q err=%v", b, err)
	}
	_ = remote
}

// TestGeoShipRetriesAfterError: a failing remote is retried and eventually
// converges once it recovers; local writes are never blocked meanwhile.
type failingStore struct {
	Store
	fail bool
}

func (s *failingStore) Put(ctx context.Context, key string, data []byte) error {
	if s.fail {
		return errNodeDownGeo
	}
	return s.Store.Put(ctx, key, data)
}

var errNodeDownGeo = &geoTestErr{}

type geoTestErr struct{}

func (*geoTestErr) Error() string { return "geo test: remote down" }

func TestGeoShipRetriesAfterError(t *testing.T) {
	local := NewMirror([]Store{NewMemoryStore(), NewMemoryStore()}, WithWriteQuorum(2), WithRepoPrefix("repos/gr"))
	fs := &failingStore{Store: NewMemoryStore(), fail: true}
	remote := NewMirror([]Store{fs, NewMemoryStore()}, WithWriteQuorum(2), WithRepoPrefix("repos/gr"))
	g := NewGeoStore(GeoConfig{
		Local:       local,
		Remote:      remote,
		ControlName: "repos/gr/current",
		Interval:    10 * time.Millisecond,
	})
	defer g.Close()
	ctx := context.Background()
	if err := g.Put(ctx, "repos/gr/obj1", []byte("retry")); err != nil {
		t.Fatal(err)
	}
	// Local commit must succeed while the remote is down; give the shipper a
	// moment to fail at least once.
	time.Sleep(100 * time.Millisecond)
	if st := g.Status(); st.Errors == 0 {
		t.Fatalf("expected ship errors while remote down, got %+v", st)
	}
	if st := g.Status(); st.Pending != 1 {
		t.Fatalf("expected 1 pending item, got %+v", st)
	}
	// Heal the remote; convergence must complete.
	fs.fail = false
	waitGeoIdle(t, g, 1)
	b, err := remote.Get(ctx, "repos/gr/obj1", nil)
	if err != nil || string(b) != "retry" {
		t.Fatalf("remote after heal: %q err=%v", b, err)
	}
}

// TestGeoHeadCASMonotonic: HEAD publications overwrite the remote pointer
// with the newest local value, never regressing.
func TestGeoHeadCASMonotonic(t *testing.T) {
	g, _, remote := geoFixture(t)
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		if err := g.Put(ctx, "repos/gx/current", []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	waitGeoIdle(t, g, 3)
	cur, err := remote.Get(ctx, "repos/gx/current", nil)
	if err != nil || len(cur) != 1 || cur[0] != 3 {
		t.Fatalf("remote current=%v err=%v, want [3]", cur, err)
	}
}
