package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// putFailStore fails Put while leaving reads and CAS intact, simulating a
// replica that accepts connections but refuses writes.
type putFailStore struct {
	Store
	fail bool
}

var errPutDown = errors.New("replica read-only")

func (s *putFailStore) Put(ctx context.Context, key string, data []byte) error {
	if s.fail {
		return errPutDown
	}
	return s.Store.Put(ctx, key, data)
}

// A revision object must never become visible on a standby when the arbiter
// refused it: a retry could then publish different bytes under the same
// revision number on each replica.
func TestRevisionPutRequiresArbiter(t *testing.T) {
	ctx := context.Background()
	a := &putFailStore{Store: NewMemoryStore()}
	b := NewMemoryStore()
	m := NewMirror([]Store{a, b})
	defer m.Close()
	a.fail = true

	key := "repos/x/revs-loose/0000000000/0000000001"
	if err := m.Put(ctx, key, []byte("rb")); !errors.Is(err, ErrQuorumUnmet) {
		t.Fatalf("Put revision with a down arbiter = %v, want ErrQuorumUnmet", err)
	}
	if _, err := b.Get(ctx, key, nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("revision leaked to the standby: %v", err)
	}
}

// Even when this process marked the arbiter down, a revision write still
// targets it first; otherwise the standby could serve a revision the arbiter
// (the HEAD authority) never saw.
func TestRevisionPutAlwaysTargetsArbiter(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	m := NewMirror([]Store{a, b})
	defer m.Close()
	m.mu.Lock()
	m.healthy[0] = false
	m.mu.Unlock()

	key := "repos/x/revs-loose/0000000000/0000000002"
	if err := m.Put(ctx, key, []byte("rb2")); err != nil {
		t.Fatal(err)
	}
	got, err := a.Get(ctx, key, nil)
	if err != nil || string(got) != "rb2" {
		t.Fatalf("arbiter copy = %q, %v", got, err)
	}
	if got, err := b.Get(ctx, key, nil); err != nil || string(got) != "rb2" {
		t.Fatalf("standby copy = %q, %v", got, err)
	}
}

// A standby that holds an orphan block from a failed attempt must be healed
// from the arbiter, not left divergent.
func TestCopyKeyToOverwritesDivergentRevision(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	m := NewMirror([]Store{a, b})
	defer m.Close()

	key := "repos/x/revs-loose/0000000000/0000000003"
	if err := a.Put(ctx, key, []byte("arbiter-bytes")); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(ctx, key, []byte("orphan-bytes")); err != nil {
		t.Fatal(err)
	}
	if err := m.copyKeyTo(1, key); err != nil {
		t.Fatal(err)
	}
	if got, _ := b.Get(ctx, key, nil); string(got) != "arbiter-bytes" {
		t.Fatalf("standby not healed: %q", got)
	}
	if got, _ := a.Get(ctx, key, nil); string(got) != "arbiter-bytes" {
		t.Fatalf("arbiter changed: %q", got)
	}
}

// Repair must never copy a standby orphan onto the arbiter.
func TestCopyKeyToNeverOverwritesArbiter(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	m := NewMirror([]Store{a, b})
	defer m.Close()

	key := "repos/x/revs-loose/0000000000/0000000004"
	if err := a.Put(ctx, key, []byte("arbiter-bytes")); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(ctx, key, []byte("orphan-bytes")); err != nil {
		t.Fatal(err)
	}
	if err := m.copyKeyTo(0, key); err != nil {
		t.Fatal(err)
	}
	if got, _ := a.Get(ctx, key, nil); string(got) != "arbiter-bytes" {
		t.Fatalf("arbiter overwritten by a standalone orphan: %q", got)
	}
}

// A CAS based on a stale standby value must be refused by the arbiter, and
// the stale writer must not overwrite the standby either.
func TestControlCASIsCheckedOnArbiter(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	m := NewMirror([]Store{a, b})
	defer m.Close()

	key := "repos/x/current"
	head0 := []byte("head-0")
	head1 := []byte("head-1")
	if err := a.Put(ctx, key, head1); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(ctx, key, head0); err != nil {
		t.Fatal(err)
	}
	if _, err := m.CAS(ctx, key, ContentETag(head0), []byte("head-2")); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("CAS with a stale standby value = %v, want ErrPreconditionFailed", err)
	}
	if got, _ := b.Get(ctx, key, nil); string(got) != "head-0" {
		t.Fatalf("failed CAS updated the standby: %q", got)
	}
	if got, _ := a.Get(ctx, key, nil); string(got) != "head-1" {
		t.Fatalf("arbiter changed by a failed CAS: %q", got)
	}
	// The correct base succeeds and is mirrored to the standby.
	if _, err := m.CAS(ctx, key, ContentETag(head1), []byte("head-2")); err != nil {
		t.Fatal(err)
	}
	if got, _ := a.Get(ctx, key, nil); string(got) != "head-2" {
		t.Fatalf("arbiter lost the successful CAS: %q", got)
	}
	if got, _ := b.Get(ctx, key, nil); string(got) != "head-2" {
		t.Fatalf("standby not mirrored: %q", got)
	}
}

// Repair must overwrite a standby's orphan revision block with the arbiter's
// bytes, including when their sizes match (node-id reordering keeps the total
// length but changes content).
func TestRepairReconcilesDivergentRevisionTail(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	m := NewMirror([]Store{a, b})
	defer m.Close()

	key := "repos/x/revs-loose/0000000000/0000000005"
	if err := a.Put(ctx, key, []byte("arbiter-A")); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(ctx, key, []byte("orphan-AB")); err != nil {
		t.Fatal(err)
	}
	// Equal lengths: only the content differs, which a size check cannot see.
	sets := []map[string]int64{{key: 9}, {key: 9}}
	m.reconcileRevisions(map[string]bool{key: true}, sets, []bool{true, true})
	if got, _ := b.Get(ctx, key, nil); string(got) != "arbiter-A" {
		t.Fatalf("standby revision not reconciled: %q", got)
	}
	if got, _ := a.Get(ctx, key, nil); string(got) != "arbiter-A" {
		t.Fatalf("arbiter changed during reconcile: %q", got)
	}
}

// Only one process may hold the repair lease, so four svngod instances do not
// run the same full bucket scan after a failover. A crashed holder's lease is
// taken over once its deadline passes.
func TestRepairLeaseSerialisesProcesses(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	mk := func(owner string) *MirrorStore {
		return &MirrorStore{
			mirrors: []Store{a, b}, healthy: []bool{true, true},
			repoPrefix: "repos/x", repairOwner: owner, lazy: true,
		}
	}
	m1, m2 := mk("p1"), mk("p2")
	if !m1.acquireRepairLease(ctx) {
		t.Fatal("first process did not acquire the lease")
	}
	if m2.acquireRepairLease(ctx) {
		t.Fatal("second process acquired a held lease")
	}
	m1.releaseRepairLease(ctx)
	if !m2.acquireRepairLease(ctx) {
		t.Fatal("lease not acquired after release")
	}
	m2.releaseRepairLease(ctx)

	// A crashed holder leaves the payload behind; the deadline allows takeover.
	if err := a.Put(ctx, m2.repairLeaseKey(), []byte("dead 1")); err != nil {
		t.Fatal(err)
	}
	if !m2.acquireRepairLease(ctx) {
		t.Fatal("expired lease was not taken over")
	}
	m2.releaseRepairLease(ctx)
	if _, ok, err := a.Head(ctx, m2.repairLeaseKey()); err != nil || ok {
		t.Fatalf("lease still present after release: ok=%v err=%v", ok, err)
	}
}

// A long repair pass renews its lease; a process that does not own it cannot.
func TestRepairLeaseRenewedByOwnerOnly(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	mk := func(owner string) *MirrorStore {
		return &MirrorStore{
			mirrors: []Store{a, b}, healthy: []bool{true, true},
			repoPrefix: "repos/x", repairOwner: owner, lazy: true,
		}
	}
	m1, m2 := mk("p1"), mk("p2")
	if !m1.acquireRepairLease(ctx) {
		t.Fatal("acquire")
	}
	before, err := a.Get(ctx, m1.repairLeaseKey(), nil)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if !m1.renewRepairLeaseOnce(ctx) {
		t.Fatal("owner could not renew")
	}
	after, err := a.Get(ctx, m1.repairLeaseKey(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) == string(after) || !strings.HasPrefix(string(after), "p1 ") {
		t.Fatalf("lease not renewed: before=%q after=%q", before, after)
	}
	if m2.renewRepairLeaseOnce(ctx) {
		t.Fatal("non-owner renewed the lease")
	}
	m1.releaseRepairLease(ctx)
}

// Write leadership must return to the arbiter whenever it is healthy; keeping
// a standby elected after a transient timeout made a standby outage fence all
// commits even though mirror 0 was serving.
func TestPrimaryPrefersArbiter(t *testing.T) {
	a, b := NewMemoryStore(), NewMemoryStore()
	m := NewMirror([]Store{a, b})
	defer m.Close()
	m.mu.Lock()
	m.prim = 1
	m.mu.Unlock()
	if _, pi := m.primary(); pi != 0 {
		t.Fatalf("primary = %d, want the healthy arbiter 0", pi)
	}
}

// A cold start with the standby unreachable must not fence commits: the
// arbiter's HEAD is authoritative because every commit publishes there first.
func TestControlKeyAdoptsArbiterWhenStandbyDown(t *testing.T) {
	ctx := context.Background()
	a := NewMemoryStore()
	b := &getFailStore{Store: NewMemoryStore()}
	key := "repos/x/current"
	if err := a.Put(ctx, key, []byte("head=5")); err != nil {
		t.Fatal(err)
	}
	m := NewMirror([]Store{a, b}, WithRepoPrefix("repos/x"), WithMutableOrder(compareCurrentBytes))
	defer m.Close()
	b.fail.Store(true)
	m.SetControlKey(key)
	if got := string(m.ackedCopy()); got != "head=5" {
		t.Fatalf("acked head = %q, want the arbiter's value", got)
	}
	if err := m.checkCaughtUp(a, 0); err != nil {
		t.Fatalf("commit fenced with a healthy arbiter: %v", err)
	}
}

// A repair pass that finds a mirror unreachable must start the recovery loop;
// the health loop skips unhealthy mirrors, so nothing else would re-admit it.
func TestSetHealthyStartsRecovery(t *testing.T) {
	a, b := NewMemoryStore(), NewMemoryStore()
	m := &MirrorStore{
		mirrors: []Store{a, b}, healthy: []bool{true, true},
		repoPrefix: "repos/x", repairOwner: "p1", lazy: true,
		stop: make(chan struct{}),
	}
	m.setHealthy(1, false)
	m.mu.Lock()
	rec := m.recovering
	m.mu.Unlock()
	if !rec {
		t.Fatal("marking a mirror down did not start recovery")
	}
	close(m.stop)
}
