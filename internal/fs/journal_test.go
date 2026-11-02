package fs

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"svngod/internal/fs/store"
)

func TestCommitRecordRoundTripAndCRC(t *testing.T) {
	rec := &CommitRecord{
		Version: journalVersion, Rev: 7, Parent: 6, Lease: "tok",
		Created: 1700000000, RevBlockKey: "repos/r/revs-loose/0/7",
		RevBlockSHA: "abc", PropsKey: "repos/r/revprops-loose/0/7",
		Author: "alice", Log: "hello", Quorum: 2,
	}
	b := EncodeCommitRecord(rec)
	got, err := DecodeCommitRecord(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.Rev != 7 || got.Parent != 6 || got.Log != "hello" || got.Quorum != 2 {
		t.Fatalf("roundtrip=%+v", got)
	}
	// A single flipped byte in the body must be rejected.
	bad := append([]byte(nil), b...)
	bad[len(bad)-2] ^= 0xff
	if _, err := DecodeCommitRecord(bad); err == nil {
		t.Fatal("corrupted record accepted")
	}
	if _, err := DecodeCommitRecord(b[:len(b)-1]); err == nil {
		t.Fatal("truncated record accepted")
	}
	if _, err := DecodeCommitRecord([]byte("NOTWAL!!")); err == nil {
		t.Fatal("bad magic accepted")
	}
}

// switchStore can be taken down and brought back, like an S3 node going away.
type switchStore struct {
	store.Store
	down atomic.Bool
}

var errNodeDown = errors.New("node down")

func (s *switchStore) fail() error {
	if s.down.Load() {
		return errNodeDown
	}
	return nil
}

func (s *switchStore) Put(ctx context.Context, key string, data []byte) error {
	if err := s.fail(); err != nil {
		return err
	}
	return s.Store.Put(ctx, key, data)
}

func (s *switchStore) PutFrom(ctx context.Context, key string, r io.Reader, size int64) error {
	if err := s.fail(); err != nil {
		return err
	}
	return s.Store.PutFrom(ctx, key, r, size)
}

func (s *switchStore) Get(ctx context.Context, key string, rng *store.Range) ([]byte, error) {
	if err := s.fail(); err != nil {
		return nil, err
	}
	return s.Store.Get(ctx, key, rng)
}

func (s *switchStore) GetTo(ctx context.Context, key string, w io.Writer) (int64, error) {
	if err := s.fail(); err != nil {
		return 0, err
	}
	return s.Store.GetTo(ctx, key, w)
}

func (s *switchStore) Exists(ctx context.Context, key string) (bool, error) {
	if err := s.fail(); err != nil {
		return false, err
	}
	return s.Store.Exists(ctx, key)
}

func (s *switchStore) Head(ctx context.Context, key string) (string, bool, error) {
	if err := s.fail(); err != nil {
		return "", false, err
	}
	return s.Store.Head(ctx, key)
}

func (s *switchStore) CreateIfAbsent(ctx context.Context, key string, data []byte) error {
	if err := s.fail(); err != nil {
		return err
	}
	return s.Store.CreateIfAbsent(ctx, key, data)
}

func (s *switchStore) CAS(ctx context.Context, key, expect string, data []byte) (string, error) {
	if err := s.fail(); err != nil {
		return "", err
	}
	return s.Store.CAS(ctx, key, expect, data)
}

func (s *switchStore) ListPrefix(ctx context.Context, prefix, cont string, max int) ([]store.ListedObj, string, error) {
	if err := s.fail(); err != nil {
		return nil, "", err
	}
	return s.Store.ListPrefix(ctx, prefix, cont, max)
}

func (s *switchStore) Delete(ctx context.Context, keys []string) error {
	if err := s.fail(); err != nil {
		return err
	}
	return s.Store.Delete(ctx, keys)
}

// quorumFixture builds a 3-node mirror with quorum 2: a, b (switchable), c.
func quorumFixture(t *testing.T) (*Repo, *switchStore, *switchStore, *store.MirrorStore) {
	t.Helper()
	ma := store.NewMemoryStore()
	mb := &switchStore{Store: store.NewMemoryStore()}
	mc := &switchStore{Store: store.NewMemoryStore()}
	ms := store.NewMirror([]store.Store{ma, mb, mc}, store.WithWriteQuorum(2))
	r, err := CreateRepo(ms, "q", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	return r, mb, mc, ms
}

// TestQuorumCommitRefusesWhenTooFewCopies: with quorum=2 a second node going
// away must block publication (503), leaving HEAD untouched.
func TestQuorumCommitRefusesWhenTooFewCopies(t *testing.T) {
	r, mb, mc, ms := quorumFixture(t)
	if _, err := r.CommitTxn(0, []Change{{Path: "/a", Action: ActionAdd, Kind: KindFile, Data: []byte("1")}}, nil); err != nil {
		t.Fatalf("healthy commit: %v", err)
	}
	// Two of three nodes down leaves a single copy: quorum=2 cannot be met.
	mb.down.Store(true)
	mc.down.Store(true)
	_, err := r.CommitTxn(1, []Change{{Path: "/b", Action: ActionAdd, Kind: KindFile, Data: []byte("2")}}, nil)
	if !errors.Is(err, store.ErrQuorumUnmet) {
		t.Fatalf("commit with one copy: err=%v, want ErrQuorumUnmet", err)
	}
	if h, _ := r.HeadFresh(); h != 1 {
		t.Fatalf("head=%d after refused commit, want 1", h)
	}
	mb.down.Store(false)
	mc.down.Store(false)
	ms.RepairNow() // the fast recovery loop would do this within seconds
	if _, err := r.CommitTxn(1, []Change{{Path: "/b", Action: ActionAdd, Kind: KindFile, Data: []byte("2")}}, nil); err != nil {
		t.Fatalf("commit after recovery: %v", err)
	}
	if h, _ := r.HeadFresh(); h != 2 {
		t.Fatalf("head=%d after recovery, want 2", h)
	}
}

// TestJournalReplayRollsForward: a quorum-acked record whose HEAD CAS never
// ran (rewound here) is published when the repository is reopened.
func TestJournalReplayRollsForward(t *testing.T) {
	r, _, _, ms := quorumFixture(t)
	ctx := context.Background()
	if _, err := r.CommitTxn(0, []Change{{Path: "/a", Action: ActionAdd, Kind: KindFile, Data: []byte("1")}}, nil); err != nil {
		t.Fatal(err)
	}
	if rec, err := r.JournalRecord(1); err != nil || rec == nil {
		t.Fatalf("journal record missing: %v", err)
	}
	// Simulate the crash window: record and data are durable, HEAD was not
	// advanced. Rewind `current` on every replica (direct store access).
	cur, etag, err := r.readCurrent()
	if err != nil {
		t.Fatal(err)
	}
	rewound := (&Current{Head: 0, Created: cur.Created}).Marshal()
	for _, s := range []store.Store{ms} {
		if _, err := s.CAS(ctx, r.keys.Current(), etag, rewound); err != nil {
			t.Fatal(err)
		}
	}
	// A new process opens the repo: the WAL must roll the revision forward.
	r2, err := OpenRepo(ms, "q")
	if err != nil {
		t.Fatal(err)
	}
	if h, _ := r2.HeadFresh(); h != 1 {
		t.Fatalf("head=%d after replay, want 1", h)
	}
	if nr, err := r2.Resolve(1, "/a"); err != nil || nr == nil {
		t.Fatalf("rolled-forward revision unreadable: %v", err)
	}
}

// TestJournalReplayAvailabilityMode: without an explicit quorum the mirror is
// best-effort (primary/standby availability), but the WAL is still written and
// replayed so a rejoining node cannot resurrect an older HEAD as the current
// one and cause duplicate revision numbers.
func TestJournalReplayAvailabilityMode(t *testing.T) {
	a := store.NewMemoryStore()
	b := store.NewMemoryStore()
	ms := store.NewMirror([]store.Store{a, b})
	r, err := CreateRepo(ms, "av", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.CommitTxn(0, []Change{{Path: "/a", Action: ActionAdd, Kind: KindFile, Data: []byte("1")}}, nil); err != nil {
		t.Fatal(err)
	}
	if rec, err := r.JournalRecord(1); err != nil || rec == nil {
		t.Fatalf("availability mode did not write a WAL record: %v", err)
	}
	// Rewind HEAD to simulate a crash after the WAL append but before the CAS.
	cur, etag, err := r.readCurrent()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ms.CAS(context.Background(), r.keys.Current(), etag, (&Current{Head: 0, Created: cur.Created}).Marshal()); err != nil {
		t.Fatal(err)
	}
	r2, err := OpenRepo(ms, "av")
	if err != nil {
		t.Fatal(err)
	}
	if h, _ := r2.HeadFresh(); h != 1 {
		t.Fatalf("head=%d after replay, want 1", h)
	}
}
