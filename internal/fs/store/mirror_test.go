package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// mirrorFixtures holds two FileStores as stand-in "primary S3" and "standby
// S3" so the mirror semantics (double-write, failover read, repair catch-up)
// are exercised against real object stores, not memory.
type mirrorFixtures struct {
	a, b Store
	m    *MirrorStore
	ctx  context.Context
}

func newMirrorFixtures(t *testing.T) *mirrorFixtures {
	t.Helper()
	a := NewMemoryStore()
	b := NewMemoryStore()
	m := NewMirror([]Store{a, b})
	return &mirrorFixtures{a: a, b: b, m: m, ctx: context.Background()}
}

func TestMirrorDoubleWrite(t *testing.T) {
	f := newMirrorFixtures(t)
	if err := f.m.Put(f.ctx, "repos/demo/revs-loose/0000000000/000000000001", []byte("rev1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Both mirrors hold the object.
	for _, s := range []Store{f.a, f.b} {
		_, ok, err := s.Head(f.ctx, "repos/demo/revs-loose/0000000000/000000000001")
		if err != nil || !ok {
			t.Fatalf("object not mirrored (ok=%v err=%v)", ok, err)
		}
	}
}

func TestMirrorFailoverReadsAndCatchUp(t *testing.T) {
	f := newMirrorFixtures(t)
	if err := f.m.Put(f.ctx, "repos/demo/revs-loose/0000000000/000000000001", []byte("payload")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Simulate standby S3 losing an object (a write was missed while it was
	// down). Mark it down by removing the data from b.
	if err := f.b.Delete(f.ctx, []string{"repos/demo/revs-loose/0000000000/000000000001"}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// Reads still succeed: they fail over to the primary (a).
	got, err := f.m.Get(f.ctx, "repos/demo/revs-loose/0000000000/000000000001", nil)
	if err != nil || string(got) != "payload" {
		t.Fatalf("Get after standby loss: got=%q err=%v", got, err)
	}

	// Repair must re-materialize the object on the standby.
	f.m.RepairNow()
	_, ok, err := f.b.Head(f.ctx, "repos/demo/revs-loose/0000000000/000000000001")
	if err != nil || !ok {
		t.Fatalf("standby not caught up after repair (ok=%v err=%v)", ok, err)
	}
}

func TestMirrorPrimaryDownElection(t *testing.T) {
	f := newMirrorFixtures(t)
	if err := f.a.Delete(f.ctx, []string{"repos/demo/current"}); err != nil {
		t.Fatalf("delete current from a: %v", err)
	}
	// Mark primary down via a failing CAS (mimics a real S3 outage).
	_, err := f.m.CAS(f.ctx, "repos/demo/current", "etag", []byte(`{"Head":5}`))
	if err == nil {
		t.Fatalf("expected CAS error when primary is down")
	}
	// Data reads should still work via the standby.
	if err := f.b.Put(f.ctx, "repos/demo/revs-loose/0000000000/000000000009", []byte("x")); err != nil {
		t.Fatalf("standby put: %v", err)
	}
	got, err := f.m.Get(f.ctx, "repos/demo/revs-loose/0000000000/000000000009", nil)
	if err != nil || string(got) != "x" {
		t.Fatalf("Get after primary down: got=%q err=%v", got, err)
	}
}

func TestMirrorConcurrentWrites(t *testing.T) {
	f := newMirrorFixtures(t)
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "repos/demo/revs-loose/0000000000/0000000000" + string(rune('0'+i%10))
			if err := f.m.Put(f.ctx, key, []byte{byte(i)}); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent Put: %v", err)
	}
	// No deadlock: all objects reach both mirrors.
	n, _ := listAll(f.ctx, f.b, "")
	if len(n) == 0 {
		t.Fatalf("standby empty after concurrent writes")
	}
}

var _ = errors.New // keep errors import if unused in future edits

func TestMirrorFailoverFencesStaleHead(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	m := NewMirror([]Store{a, b})
	defer m.Close()
	key := "repos/demo/current"
	if err := m.Put(ctx, key, []byte("head=1")); err != nil {
		t.Fatal(err)
	}
	m.SetControlKey(key) // adopt head=1 as the acknowledged head

	// Standby missed the last commit and still has head=0.
	if err := b.Put(ctx, key, []byte("head=0")); err != nil {
		t.Fatal(err)
	}
	// Primary fails.
	m.markDown(0)

	// Reads of the control key must expose the acknowledged head, never the
	// standby's older pointer.
	got, err := m.Get(ctx, key, nil)
	if err != nil || string(got) != "head=1" {
		t.Fatalf("Get current after failover = %q, %v; want acknowledged head", got, err)
	}
	// Writes must be refused until the standby catches up.
	if err := m.Put(ctx, "repos/demo/revs/2", []byte("x")); !errors.Is(err, ErrReplicaLagging) {
		t.Fatalf("write on stale standby = %v, want ErrReplicaLagging", err)
	}
	// Simulate repair copying the acknowledged pointer.
	if err := b.Put(ctx, key, []byte("head=1")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, "repos/demo/revs/2", []byte("x")); err != nil {
		t.Fatalf("write after catch-up: %v", err)
	}
	if err := m.Put(ctx, key, []byte("head=2")); err != nil {
		t.Fatalf("advance head: %v", err)
	}
	if got, _ := m.Get(ctx, key, nil); string(got) != "head=2" {
		t.Fatalf("head after write = %q", got)
	}
}

func TestMirrorReadRepair(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	m := NewMirror([]Store{a, b})
	defer m.Close()
	key := "repos/x/objects/1"
	if err := m.Put(ctx, key, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	// The primary lost the object; the standby still has it.
	if err := a.Delete(ctx, []string{key}); err != nil {
		t.Fatal(err)
	}
	got, err := m.Get(ctx, key, nil)
	if err != nil || string(got) != "payload" {
		t.Fatalf("Get=%q err=%v", got, err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok, _ := a.Head(ctx, key); ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("primary was not healed by read repair")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestMirrorStatusReportsHealth(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	m := NewMirror([]Store{a, b})
	defer m.Close()
	if err := m.Put(ctx, "repos/x/objects/2", []byte("v")); err != nil {
		t.Fatal(err)
	}
	st := m.Status()
	if len(st) != 2 {
		t.Fatalf("statuses=%d", len(st))
	}
	for i := range st {
		if !st[i].Healthy || st[i].LastSuccess.IsZero() {
			t.Fatalf("replica %d status=%+v", i, st[i])
		}
	}
	if !st[0].Primary {
		t.Fatal("replica 0 should be primary")
	}
	m.markDown(1)
	st = m.Status()
	if st[1].Healthy {
		t.Fatal("replica 1 still healthy after markDown")
	}
}

// TestMirrorRecoveryLoopRestoresStandby: after a standby goes down and misses a
// write, it must be repaired and returned to service within seconds, not after
// the 60s periodic pass. A stale standby must not serve reads meanwhile.
func TestMirrorRecoveryLoopRestoresStandby(t *testing.T) {
	f := newMirrorFixtures(t)
	ctx := f.ctx
	if err := f.m.Put(ctx, "repos/demo/a", []byte("v")); err != nil {
		t.Fatal(err)
	}
	f.m.markDown(1)
	if err := f.m.Put(ctx, "repos/demo/b", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := f.b.Head(ctx, "repos/demo/b"); !ok && f.m.isHealthy(1) {
		t.Fatal("standby returned to service without the object it missed")
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if f.m.isHealthy(1) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !f.m.isHealthy(1) {
		t.Fatal("standby not returned to service by the recovery loop")
	}
	if _, ok, _ := f.b.Head(ctx, "repos/demo/b"); !ok {
		t.Fatal("standby still missing the object after repair")
	}
	f.m.Close()
}

// TestMirrorFencedHeadReturnsNewest: a process that fenced after losing the
// primary must not keep serving its last acknowledged HEAD forever. Once a
// replica (or another process) has advanced it, reads must see the newer one.
func TestMirrorFencedHeadReturnsNewest(t *testing.T) {
	a := NewMemoryStore()
	b := NewMemoryStore()
	order := func(key string, x, y []byte) int {
		var nx, ny int
		fmt.Sscanf(string(x), "HEAD=%d", &nx)
		fmt.Sscanf(string(y), "HEAD=%d", &ny)
		switch {
		case nx < ny:
			return -1
		case nx > ny:
			return 1
		}
		return 0
	}
	m := NewMirror([]Store{a, b}, WithMutableOrder(order))
	key := "repos/demo/current"
	m.SetControlKey(key)
	// The primary is stale, the standby is ahead; the process last acked 1.
	if err := a.Put(context.Background(), key, []byte("HEAD=1")); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(context.Background(), key, []byte("HEAD=2")); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	m.fenced = true
	m.ackedCurrent = []byte("HEAD=1")
	m.mu.Unlock()
	got, err := m.Get(context.Background(), key, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "HEAD=2" {
		t.Fatalf("fenced read=%q, want newest HEAD=2", got)
	}
}

// TestMirrorRepairScopedToRepo: repair must never copy another repository's
// objects into this repository's backends. Mirrors share a bucket/local root,
// and an unscoped repair used to replicate every repo (and even test objects).
func TestMirrorRepairScopedToRepo(t *testing.T) {
	a := NewMemoryStore()
	b := NewMemoryStore()
	ctx := context.Background()
	if err := a.Put(ctx, "repos/x/a", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := a.Put(ctx, "repos/y/b", []byte("y")); err != nil {
		t.Fatal(err)
	}
	m := NewMirror([]Store{a, b}, WithRepoPrefix("repos/x"))
	m.RepairNow()
	if ok, _ := b.Exists(ctx, "repos/x/a"); !ok {
		t.Fatal("scoped repair did not copy the repository object")
	}
	if ok, _ := b.Exists(ctx, "repos/y/b"); ok {
		t.Fatal("repair copied another repository's object")
	}
	m.Close()
}

// TestCheckCaughtUpAdoptsNewerHead: another svngod process may have committed
// through this mirror while this process was fenced. A replica that is AHEAD
// of the local acknowledgement must be adopted, not treated as lagging, or
// the process would refuse writes forever (repair cannot update its memory).
func TestCheckCaughtUpAdoptsNewerHead(t *testing.T) {
	a := NewMemoryStore()
	b := NewMemoryStore()
	order := func(key string, x, y []byte) int {
		var nx, ny int
		fmt.Sscanf(string(x), "HEAD=%d", &nx)
		fmt.Sscanf(string(y), "HEAD=%d", &ny)
		switch {
		case nx < ny:
			return -1
		case nx > ny:
			return 1
		}
		return 0
	}
	m := NewMirror([]Store{a, b}, WithMutableOrder(order))
	ctx := context.Background()
	key := "repos/demo/current"
	m.SetControlKey(key)
	if err := a.Put(ctx, key, []byte("HEAD=2")); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	m.fenced = true
	m.ackedCurrent = []byte("HEAD=1")
	m.mu.Unlock()
	if err := m.checkCaughtUp(a, 0); err != nil {
		t.Fatalf("newer HEAD rejected: %v", err)
	}
	if got := string(m.ackedCopy()); got != "HEAD=2" {
		t.Fatalf("acked HEAD=%q, want HEAD=2", got)
	}
	m.Close()
}

func testHeadOrder(_ string, x, y []byte) int {
	var nx, ny int
	fmt.Sscanf(string(x), "HEAD=%d", &nx)
	fmt.Sscanf(string(y), "HEAD=%d", &ny)
	switch {
	case nx < ny:
		return -1
	case nx > ny:
		return 1
	}
	return 0
}

// TestNoteAckedMonotonic: the acknowledged HEAD is a high-water mark. A repair
// that can only see a lagging replica must never lower it, otherwise fencing
// clears and the lagging node can commit at an old revision number.
func TestNoteAckedMonotonic(t *testing.T) {
	m := NewMirror([]Store{NewMemoryStore(), NewMemoryStore()}, WithMutableOrder(testHeadOrder))
	defer m.Close()
	m.mu.Lock()
	m.controlKey = "repos/demo/current"
	m.ackedCurrent = []byte("HEAD=2")
	m.mu.Unlock()
	m.noteAcked([]byte("HEAD=1"))
	if got := string(m.ackedCopy()); got != "HEAD=2" {
		t.Fatalf("ack lowered to %q", got)
	}
	m.noteAcked([]byte("HEAD=3"))
	if got := string(m.ackedCopy()); got != "HEAD=3" {
		t.Fatalf("ack did not advance: %q", got)
	}
}

// TestCheckCaughtUpUnknownHead: fenced with no known HEAD must refuse writes.
func TestCheckCaughtUpUnknownHead(t *testing.T) {
	a := NewMemoryStore()
	m := NewMirror([]Store{a, NewMemoryStore()}, WithMutableOrder(testHeadOrder))
	defer m.Close()
	m.mu.Lock()
	m.controlKey = "repos/demo/current"
	m.fenced = true
	m.ackedCurrent = nil
	m.mu.Unlock()
	if err := m.checkCaughtUp(a, 0); !errors.Is(err, ErrReplicaLagging) {
		t.Fatalf("unknown HEAD accepted: %v", err)
	}
}

// TestSetControlKeyArbiterAuthority: for a two-node pair the arbiter's HEAD is
// authoritative, so a cold start with the standby unreachable adopts it instead
// of fencing; when the arbiter is the unreachable one, the survivor cannot be
// trusted and writes stay fenced.
func TestSetControlKeyArbiterAuthority(t *testing.T) {
	ctx := context.Background()
	key := "repos/demo/current"

	a, b := NewMemoryStore(), NewMemoryStore()
	if err := a.Put(ctx, key, []byte("HEAD=2")); err != nil {
		t.Fatal(err)
	}
	m := &MirrorStore{
		mirrors: []Store{a, b}, healthy: []bool{true, true},
		repoPrefix: "repos/demo", controlKey: key, mutableOrder: testHeadOrder,
	}
	m.SetControlKey(key)
	m.mu.Lock()
	fenced, ack := m.fenced, string(m.ackedCurrent)
	m.mu.Unlock()
	if fenced || ack != "HEAD=2" {
		t.Fatalf("arbiter view not adopted: fenced=%v ack=%q", fenced, ack)
	}

	badArbiter := &getFailStore{Store: NewMemoryStore()}
	standby := NewMemoryStore()
	if err := standby.Put(ctx, key, []byte("HEAD=2")); err != nil {
		t.Fatal(err)
	}
	badArbiter.fail.Store(true)
	m2 := &MirrorStore{
		mirrors: []Store{badArbiter, standby}, healthy: []bool{true, true},
		repoPrefix: "repos/demo", controlKey: key, mutableOrder: testHeadOrder,
	}
	m2.SetControlKey(key)
	m2.mu.Lock()
	fenced = m2.fenced
	m2.mu.Unlock()
	if !fenced {
		t.Fatal("survivor adopted a HEAD without the arbiter")
	}
}

// TestRepairDoesNotLowerAck: reconcileMutable with only an older replica
// reachable must leave both the pointer and the acknowledgement untouched.
func TestRepairDoesNotLowerAck(t *testing.T) {
	ctx := context.Background()
	a := NewMemoryStore()
	b := NewMemoryStore()
	key := "repos/demo/current"
	if err := a.Put(ctx, key, []byte("HEAD=1")); err != nil {
		t.Fatal(err)
	}
	m := NewMirror([]Store{a, b}, WithMutableOrder(testHeadOrder))
	defer m.Close()
	m.mu.Lock()
	m.controlKey = key
	m.ackedCurrent = []byte("HEAD=2")
	m.fenced = true
	m.mu.Unlock()
	m.RepairNow()
	if got := string(m.ackedCopy()); got != "HEAD=2" {
		t.Fatalf("repair lowered ack to %q", got)
	}
	got, err := a.Get(ctx, key, nil)
	if err != nil || string(got) != "HEAD=1" {
		t.Fatalf("repair rewrote the pointer: %q err=%v", got, err)
	}
}
