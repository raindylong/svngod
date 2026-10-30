// Command perf is a scale test for the fs layer: it drives millions of
// sequential revisions (one file modified per revision) and reports per-revision
// latency trends, shard packing, and final correctness. Run:
//
//	go run ./cmd/perf [revisions]
package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"svngod/internal/fs"
	"svngod/internal/fs/store"
)

func main() {
	n := 20000
	if len(os.Args) > 1 {
		if v, err := strconv.Atoi(os.Args[1]); err == nil {
			n = v
		}
	}
	root := "/tmp/svnperf"
	os.RemoveAll(root)
	if err := os.MkdirAll(root, 0o755); err != nil {
		panic(err)
	}
	st, err := store.NewFileStore(root)
	if err != nil {
		panic(err)
	}
	repo, err := fs.CreateRepo(st, "repos/demo", 4096, 64)
	if err != nil {
		panic(err)
	}
	rp := map[string]string{"svn:author": "perf", "svn:date": time.Now().UTC().Format("2006-01-02T15:04:05.000000Z")}

	var last uint64
	if rev0, err := repo.CommitTxn(0, []fs.Change{
		{Path: "/hello.txt", Action: fs.ActionAdd, Kind: fs.KindFile, Data: []byte("v1\n")},
	}, rp); err != nil {
		panic(err)
	} else {
		last = rev0
	}

	start := time.Now()
	var p99 time.Duration
	var batch time.Duration
	for i := 2; i <= n; i++ {
		t0 := time.Now()
		text := []byte(fmt.Sprintf("v%d\n", i))
		rev, err := repo.CommitTxn(last, []fs.Change{
			{Path: "/hello.txt", Action: fs.ActionModify, Kind: fs.KindFile, Data: text},
		}, rp)
		if err != nil {
			panic(err)
		}
		last = rev
		d := time.Since(t0)
		batch += d
		if i%1000 == 0 {
			fmt.Printf("rev=%6d avg(prev-1k)=%.3fms cum=%.3fms/rev elapsed=%s\n",
				i, float64(batch.Milliseconds())/1000.0, float64(time.Since(start).Milliseconds())/float64(i), time.Since(start).Round(time.Millisecond))
			batch = 0
		}
	}
	head, err := repo.Head()
	if err != nil {
		panic(err)
	}
	total := time.Since(start)
	fmt.Printf("\nHEAD=%d shards=%d total=%.2fs avg=%.3fms/rev p99>=%.1fms\n",
		head, head/4096+1, total.Seconds(), float64(total.Milliseconds())/float64(n), float64(p99.Milliseconds()))

	// correctness: resolved content matches the last revision.
	nr, err := repo.Resolve(head, "/hello.txt")
	if err != nil {
		panic(err)
	}
	t, err := repo.GetText(nr.Node)
	if err != nil {
		panic(err)
	}
	want := fmt.Sprintf("v%d\n", n)
	if string(t) != want {
		panic(fmt.Sprintf("content mismatch: got %q want %q", string(t), want))
	}
	fmt.Printf("correctness: text at HEAD matches rev %d\n", n)

	// spot-check a packed shard far from HEAD resolves correctly.
	if head > 8192 {
		rev := uint64(8191)
		nr2, err := repo.Resolve(rev, "/hello.txt")
		if err != nil {
			panic(err)
		}
		t2, err := repo.GetText(nr2.Node)
		if err != nil {
			panic(err)
		}
		if string(t2) != fmt.Sprintf("v%d\n", rev) {
			panic(fmt.Sprintf("packed rev %d content mismatch: %q", rev, string(t2)))
		}
		fmt.Printf("correctness: packed shard rev %d resolves OK\n", rev)
	}
}
