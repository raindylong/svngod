package store

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// regFailStore makes the arbiter unreachable for register operations while
// leaving the other methods intact.
type regFailStore struct {
	Store
	fail atomic.Bool
}

var errArbiterDown = errors.New("arbiter down")

func (s *regFailStore) CreateIfAbsent(ctx context.Context, key string, data []byte) error {
	if s.fail.Load() {
		return errArbiterDown
	}
	return s.Store.CreateIfAbsent(ctx, key, data)
}

func (s *regFailStore) CAS(ctx context.Context, key, expect string, data []byte) (string, error) {
	if s.fail.Load() {
		return "", errArbiterDown
	}
	return s.Store.CAS(ctx, key, expect, data)
}

// getFailStore models a replica that accepts connections but cannot serve a
// read (for example a paused process).
type getFailStore struct {
	Store
	fail atomic.Bool
}

func (s *getFailStore) Get(ctx context.Context, key string, rng *Range) ([]byte, error) {
	if s.fail.Load() {
		return nil, errors.New("store: no answer")
	}
	return s.Store.Get(ctx, key, rng)
}

// TestCopyKeyToMissingIsGoneDespiteDownPeer: a pending key that no reachable
// replica holds must be dropped even if another replica is unreachable;
// otherwise one down node blocks the recovery of a different replica forever.
func TestCopyKeyToMissingIsGoneDespiteDownPeer(t *testing.T) {
	a := NewMemoryStore()
	b := NewMemoryStore()
	c := &getFailStore{Store: NewMemoryStore()}
	m := NewMirror([]Store{a, b, c})
	defer m.Close()
	c.fail.Store(true)
	err := m.copyKeyTo(1, "repos/x/texts/absent")
	if !errors.Is(err, errRepairKeyGone) {
		t.Fatalf("copyKeyTo = %v, want errRepairKeyGone", err)
	}
}

// TestCopyKeyToUnknownWhenAllSourcesFail: with no reachable source at all the
// key is not declared gone (we cannot know whether it still exists).
func TestCopyKeyToUnknownWhenAllSourcesFail(t *testing.T) {
	a := &getFailStore{Store: NewMemoryStore()}
	b := NewMemoryStore()
	c := &getFailStore{Store: NewMemoryStore()}
	m := NewMirror([]Store{a, b, c})
	defer m.Close()
	a.fail.Store(true)
	c.fail.Store(true)
	err := m.copyKeyTo(1, "repos/x/texts/unknown")
	if err == nil || errors.Is(err, errRepairKeyGone) {
		t.Fatalf("copyKeyTo = %v, want a transport error", err)
	}
}

// idStore attaches a location identity to a MemoryStore for ordering tests.
type idStore struct {
	Store
	id string
}

func (s *idStore) Identity() string { return s.id }

// TestMirrorOrderCanonicalByIdentity: two processes that configure the same
// replica set in opposite orders must still pick the same arbiter; otherwise
// they create two leases and publish the same revision with different trees.
func TestMirrorOrderCanonicalByIdentity(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	ia, ib := &idStore{Store: a, id: "s3://a"}, &idStore{Store: b, id: "s3://b"}
	m1 := NewMirror([]Store{ia, ib})
	defer m1.Close()
	m2 := NewMirror([]Store{ib, ia})
	defer m2.Close()
	firstID := func(m *MirrorStore) string {
		return m.mirrors[0].(interface{ Identity() string }).Identity()
	}
	if firstID(m1) != "s3://a" || firstID(m2) != "s3://a" {
		t.Fatalf("reversed configs chose different first mirrors: %q vs %q", firstID(m1), firstID(m2))
	}
	key := "repos/x/locks/commit"
	if err := m1.CreateIfAbsentQuorum(ctx, key, []byte("owner1 1"), 1); err != nil {
		t.Fatalf("first lease: %v", err)
	}
	if err := m2.CreateIfAbsentQuorum(ctx, key, []byte("owner2 2"), 1); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("second lease with reversed config = %v, want ErrPreconditionFailed", err)
	}
}

// TestMirrorOrderKeepsConfigWithoutIdentity: stores that cannot name
// themselves keep the configured order (MemoryStore-based unit tests).
func TestMirrorOrderKeepsConfigWithoutIdentity(t *testing.T) {
	a, b := NewMemoryStore(), NewMemoryStore()
	m := NewMirror([]Store{a, b})
	defer m.Close()
	if m.mirrors[0] != a || m.mirrors[1] != b {
		t.Fatal("configured order changed for identity-less stores")
	}
}

// TestMajorityQuorumIntersects: with three nodes a strict majority is enough,
// so one node outage keeps commits possible, while two writers with different
// health views still cannot both hold the lease.
func TestMajorityQuorumIntersects(t *testing.T) {
	ctx := context.Background()
	a, b, c := NewMemoryStore(), NewMemoryStore(), NewMemoryStore()
	m1 := NewMirror([]Store{a, b, c})
	defer m1.Close()
	m2 := NewMirror([]Store{a, b, c})
	defer m2.Close()
	if got := m1.RegisterQuorum(); got != 2 {
		t.Fatalf("RegisterQuorum(3) = %d, want 2", got)
	}
	// One node down: a majority still exists.
	m1.mu.Lock()
	m1.healthy[2] = false
	m1.mu.Unlock()
	key := "repos/x/locks/commit"
	if err := m1.CreateIfAbsentQuorum(ctx, key, []byte("owner1 1"), 2); err != nil {
		t.Fatalf("create with 2/3 nodes: %v", err)
	}
	// The second writer sees a different node down. Its majority must overlap
	// the first writer's, so it cannot acquire a second lease.
	m2.mu.Lock()
	m2.healthy[0] = false
	m2.mu.Unlock()
	if err := m2.CreateIfAbsentQuorum(ctx, key, []byte("owner2 2"), 2); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("second lease = %v, want ErrPreconditionFailed", err)
	}
	// Two nodes down leaves no majority.
	m2.mu.Lock()
	m2.healthy[1] = false
	m2.mu.Unlock()
	if err := m2.CreateIfAbsentQuorum(ctx, "repos/x/locks/other", []byte("x"), 2); !errors.Is(err, ErrQuorumUnmet) {
		t.Fatalf("create with 1/3 nodes = %v, want ErrQuorumUnmet", err)
	}
}

// TestLeaseRegisterUsesArbiter pins the split-lease fix: two processes with
// different health views must still contend on the configured first mirror.
// Skipping a "down" arbiter let each process create a lease on a different
// replica and publish the same revision with different content.
func TestLeaseRegisterUsesArbiter(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	m1 := NewMirror([]Store{a, b})
	defer m1.Close()
	m2 := NewMirror([]Store{a, b})
	defer m2.Close()

	key := "repos/x/locks/commit"
	if err := m1.CreateIfAbsentQuorum(ctx, key, []byte("owner1 1"), 1); err != nil {
		t.Fatalf("first lease: %v", err)
	}
	// Process 2 believes the arbiter is down (its own health view differs).
	m2.mu.Lock()
	m2.healthy[0] = false
	m2.mu.Unlock()
	if err := m2.CreateIfAbsentQuorum(ctx, key, []byte("owner2 2"), 1); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("second lease on a non-arbiter = %v, want ErrPreconditionFailed", err)
	}

	// A CAS must still go to the arbiter: it succeeds there and never reaches
	// the standby, so the standby cannot hold a competing lease.
	cur, err := a.Get(ctx, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	expect := ContentETag(cur)
	if err := m2.CASQuorum(ctx, key, expect, []byte("owner2 3"), 1); err != nil {
		t.Fatalf("CAS on a reachable arbiter: %v", err)
	}
	if got, err := a.Get(ctx, key, nil); err != nil || string(got) != "owner2 3" {
		t.Fatalf("arbiter lease = %q, %v", got, err)
	}
	if _, err := b.Get(ctx, key, nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("standby received a lease copy: %v", err)
	}
}

// TestLeaseRegisterRefusesWhenArbiterUnreachable: if the arbiter really is
// down, the register must fail instead of moving to the standby (which would
// let another process keep using the arbiter and both commit).
func TestLeaseRegisterRefusesWhenArbiterUnreachable(t *testing.T) {
	ctx := context.Background()
	bad := &regFailStore{Store: NewMemoryStore()}
	b := NewMemoryStore()
	m := NewMirror([]Store{bad, b})
	defer m.Close()
	bad.fail.Store(true)

	key := "repos/x/locks/commit"
	if err := m.CreateIfAbsentQuorum(ctx, key, []byte("owner 1"), 1); !errors.Is(err, ErrQuorumUnmet) {
		t.Fatalf("create with a down arbiter = %v, want ErrQuorumUnmet", err)
	}
	if _, err := b.Get(ctx, key, nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("lease leaked to the standby: %v", err)
	}
	if err := m.CASQuorum(ctx, key, "etag", []byte("owner 2"), 1); !errors.Is(err, ErrQuorumUnmet) {
		t.Fatalf("CAS with a down arbiter = %v, want ErrQuorumUnmet", err)
	}
	if err := m.DeleteIfMatchQuorum(ctx, key, "etag", 1); !errors.Is(err, ErrQuorumUnmet) {
		t.Fatalf("delete with a down arbiter = %v, want ErrQuorumUnmet", err)
	}
}

// TestArbiterGetIgnoresHealth: lease verification reads the arbiter even when
// this process marked it down; otherwise a stale copy on another replica could
// confirm a lease that is no longer held.
func TestArbiterGetIgnoresHealth(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	m := NewMirror([]Store{a, b})
	defer m.Close()
	if err := a.Put(ctx, "repos/x/locks/commit", []byte("owner 1")); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	m.healthy[0] = false
	m.mu.Unlock()
	got, err := m.ArbiterGet(ctx, "repos/x/locks/commit")
	if err != nil || string(got) != "owner 1" {
		t.Fatalf("ArbiterGet = %q, %v", got, err)
	}
}
