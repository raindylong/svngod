package fs

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"svngod/internal/fs/store"
)

// TestConcurrentSamePathCommit verifies that, under the repo.mu serial lock and
// path-level optimistic concurrency control, N concurrent commits to the SAME
// path yield exactly one winner, the losers get ErrOutOfDate (not a silent
// lost update), and the committed content is the winner's — no block pollution.
func TestConcurrentSamePathCommit(t *testing.T) {
	st, err := store.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	r, err := CreateRepo(st, "repos/test", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	base, err := r.CommitTxn(0, []Change{
		{Path: "/f.txt", Action: ActionAdd, Kind: KindFile, Data: []byte("base content for f.txt seed revision")},
	}, nil)
	if err != nil || base != 1 {
		t.Fatalf("seed commit: rev=%d err=%v", base, err)
	}

	const n = 10
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = r.CommitTxn(base, []Change{
				{Path: "/f.txt", Action: ActionModify, Kind: KindFile, Data: []byte(fmt.Sprintf("v%d", i))},
			}, nil)
		}(i)
	}
	wg.Wait()

	succ, ood := 0, 0
	var winnerErr error
	for i, e := range errs {
		switch {
		case e == nil:
			succ++
		case errors.Is(e, ErrOutOfDate):
			ood++
		default:
			winnerErr = fmt.Errorf("unexpected error from goroutine %d: %v", i, e)
		}
	}
	if winnerErr != nil {
		t.Fatal(winnerErr)
	}
	if succ != 1 {
		t.Fatalf("expected exactly 1 winner, got %d (out-of-date=%d)", succ, ood)
	}
	if ood != n-1 {
		t.Fatalf("expected %d out-of-date losers, got %d", n-1, ood)
	}

	head, err := r.Head()
	if err != nil {
		t.Fatal(err)
	}
	if head != base+1 {
		t.Fatalf("head=%d, want %d (only one new revision, no block pollution)", head, base+1)
	}

	// Read back the winner's committed content — must not be polluted by a loser.
	nr, err := r.Resolve(head, "/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	if nr.Kind != KindFile {
		t.Fatalf("f.txt kind=%v", nr.Kind)
	}
	got, err := r.GetText(nr.Node)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(got), "v") {
		t.Fatalf("committed f.txt content is polluted/foreign: %q", got)
	}
}

// TestConcurrentSamePathCommitS3 runs the identical same-path atomicity contract
// against a real S3 backend (local RustFS at 127.0.0.1:9000, AK/SK k/s). The
// store's real content-hash CAS must yield exactly one winner and ErrOutOfDate
// for the rest. Skipped when the local RustFS instance is not reachable, so the
// suite stays green in environments without it.
func TestConcurrentSamePathCommitS3(t *testing.T) {
	// NewS3Store tolerates an unreachable endpoint (a mirror must start when
	// its primary is down), so probe the port to decide whether to skip.
	if c, derr := net.DialTimeout("tcp", "127.0.0.1:9000", 300*time.Millisecond); derr != nil {
		t.Skipf("local RustFS not reachable, skipping S3 CAS concurrency test: %v", derr)
	} else {
		c.Close()
	}
	st, err := store.NewS3Store(store.S3Config{
		Endpoint:   "127.0.0.1:9000",
		AccessKey:  "k",
		SecretKey:  "s",
		Bucket:     "svn",
		Region:     "us-east-1",
		RepoPrefix: "it/" + strconv.FormatInt(time.Now().UnixNano(), 36),
		BaseRepo:   "r",
	})
	if err != nil {
		t.Skipf("local RustFS not reachable, skipping S3 CAS concurrency test: %v", err)
	}
	r, err := CreateRepo(st, "", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	base, err := r.CommitTxn(0, []Change{
		{Path: "/f.txt", Action: ActionAdd, Kind: KindFile, Data: []byte("s3 cas base content seed")},
	}, nil)
	if err != nil || base != 1 {
		t.Fatalf("seed commit: rev=%d err=%v", base, err)
	}

	const n = 10
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = r.CommitTxn(base, []Change{
				{Path: "/f.txt", Action: ActionModify, Kind: KindFile, Data: []byte(fmt.Sprintf("s3v%d", i))},
			}, nil)
		}(i)
	}
	wg.Wait()

	succ, ood := 0, 0
	var winnerErr error
	for i, e := range errs {
		switch {
		case e == nil:
			succ++
		case errors.Is(e, ErrOutOfDate):
			ood++
		default:
			winnerErr = fmt.Errorf("unexpected error from goroutine %d: %v", i, e)
		}
	}
	if winnerErr != nil {
		t.Fatal(winnerErr)
	}
	if succ != 1 || ood != n-1 {
		t.Fatalf("expected 1 winner + %d out-of-date on S3, got succ=%d ood=%d", n-1, succ, ood)
	}
	head, err := r.Head()
	if err != nil {
		t.Fatal(err)
	}
	if head != base+1 {
		t.Fatalf("head=%d, want %d (no block pollution)", head, base+1)
	}
	nr, err := r.Resolve(head, "/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.GetText(nr.Node)
	if err != nil {
		t.Fatal(err)
	}
	if s := string(got); !strings.HasPrefix(s, "s3v") {
		t.Fatalf("S3 committed f.txt polluted/foreign: %q", s)
	}
	if os.Getenv("RUSTFS_TEST") == "" {
		t.Log("passed against local RustFS at 127.0.0.1:9000")
	}
}
