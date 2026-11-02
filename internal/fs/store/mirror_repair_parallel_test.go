package store

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
)

// TestRepairCopiesManyObjectsInParallel guards the recovery path that used to
// copy objects one at a time: after a 27-revision outage at 800k objects a
// serial pass needed 48s. Both small objects and a large (streamed) object
// must reach the lagging mirror in one pass.
func TestRepairCopiesManyObjectsInParallel(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	m := NewMirror([]Store{a, b}, WithRepoPrefix("repos/x"))
	defer m.Close()

	big := make([]byte, repairStreamThreshold+1024)
	for i := range big {
		big[i] = byte(i)
	}
	if err := a.Put(ctx, "repos/x/texts/big", big); err != nil {
		t.Fatal(err)
	}
	const n = 400
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("repos/x/texts/t%04d", i)
		if err := a.Put(ctx, key, []byte(key)); err != nil {
			t.Fatal(err)
		}
	}

	m.RepairNow()

	if _, ok, _ := b.Head(ctx, "repos/x/texts/big"); !ok {
		t.Fatal("repair did not copy the large object")
	}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("repos/x/texts/t%04d", i)
		if _, ok, _ := b.Head(ctx, key); !ok {
			t.Fatalf("repair did not copy %s", key)
		}
	}
	for _, s := range m.Status() {
		if !s.Healthy {
			t.Fatalf("mirror[%d] still unhealthy after a complete repair", s.Index)
		}
	}
}

type listCounter struct {
	Store
	lists int32
}

func (c *listCounter) ListPrefix(ctx context.Context, prefix, cont string, max int) ([]ListedObj, string, error) {
	atomic.AddInt32(&c.lists, 1)
	return c.Store.ListPrefix(ctx, prefix, cont, max)
}

// TestPeriodicRepairSkipsWhenUpToDate checks the no-op guard: with every
// mirror healthy and holding the acknowledged HEAD, the 60s pass must not
// enumerate a million objects. As soon as a mirror is out of service the full
// listing has to run again.
func TestPeriodicRepairSkipsWhenUpToDate(t *testing.T) {
	ctx := context.Background()
	ca, cb := &listCounter{Store: NewMemoryStore()}, &listCounter{Store: NewMemoryStore()}
	cur := []byte("head=1")
	for _, s := range []Store{ca, cb} {
		if err := s.Put(ctx, "repos/x/current", cur); err != nil {
			t.Fatal(err)
		}
	}
	m := NewMirror([]Store{ca, cb}, WithRepoPrefix("repos/x"), WithMutableOrder(compareCurrentBytes))
	defer m.Close()
	m.SetControlKey("repos/x/current")

	atomic.StoreInt32(&ca.lists, 0)
	atomic.StoreInt32(&cb.lists, 0)
	m.repair()
	if got := atomic.LoadInt32(&ca.lists) + atomic.LoadInt32(&cb.lists); got != 0 {
		t.Fatalf("up-to-date repair listed %d pages, want 0", got)
	}

	m.markDown(1)
	m.repair()
	if got := atomic.LoadInt32(&ca.lists) + atomic.LoadInt32(&cb.lists); got == 0 {
		t.Fatal("repair with a down mirror did not list the mirrors")
	}
}

// compareCurrentBytes orders the test's "head=N" values numerically.
func compareCurrentBytes(key string, a, b []byte) int {
	var x, y int
	fmt.Sscanf(string(a), "head=%d", &x)
	fmt.Sscanf(string(b), "head=%d", &y)
	switch {
	case x < y:
		return -1
	case x > y:
		return 1
	}
	return 0
}
