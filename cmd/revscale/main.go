// Command revscale drives a large number of revisions into one repository.
//
// It is a scale harness, not a server: commits go straight through the fs
// layer (the same CommitTxn a DAV MERGE calls), so a ten-million-revision
// campaign is not limited by one client process per commit. Multiple revscale
// processes can share one repository; the commit lease still serialises
// finalisation exactly as it does for concurrent svngod instances.
//
// Paths are sharded by decimal digits (six levels, ten children each, ten
// files per leaf) so every commit rewrites only a handful of tiny directory
// nodes instead of a directory with millions of children.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"svngod/internal/fs"
	"svngod/internal/fs/store"
)

var (
	storeMode = flag.String("store", "mirror", "mirror|s3|file")
	root      = flag.String("root", "/tmp/revscale", "file store root")
	endpoints = flag.String("endpoints", "192.168.23.228:9000,192.168.23.245:9000", "comma-separated S3 endpoints (first is the arbiter)")
	bucket    = flag.String("bucket", "svn-ext", "S3 bucket")
	ak        = flag.String("ak", "", "S3 access key")
	sk        = flag.String("sk", "", "S3 secret key")
	prefix    = flag.String("prefix", "repos", "bucket-level repository prefix")
	repoName  = flag.String("repo", "scale10m", "repository name inside prefix")
	target    = flag.Int64("target", 1000, "total revisions to commit")
	workers   = flag.Int("workers", 8, "concurrent commit workers in this process")
	start     = flag.Int64("start", -1, "first revision index (default: current HEAD)")
	mixed     = flag.Bool("mixed", false, "mix in edits, deletes, copies and property changes")
	bench     = flag.Bool("bench", false, "print every commit latency (benchmark mode)")
	progress  = flag.Duration("progress", time.Second, "progress interval")
	rawBench  = flag.Int("raw", 0, "measure raw 1KiB Put/Get round trips and exit")
)

var authors = []string{"alice", "bob", "carol", "admin", "dave", "erin", "frank", "grace"}

func s3Store(ep string) store.Store {
	s, err := store.NewS3Store(store.S3Config{
		Endpoint: ep, AccessKey: *ak, SecretKey: *sk, Bucket: *bucket, Region: "us-east-1",
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "store:", err)
		os.Exit(1)
	}
	return s
}

func main() {
	flag.Parse()
	var st store.Store
	switch *storeMode {
	case "mirror":
		eps := strings.Split(*endpoints, ",")
		if len(eps) < 2 {
			fmt.Fprintln(os.Stderr, "mirror needs at least two endpoints")
			os.Exit(2)
		}
		var ms []store.Store
		for _, ep := range eps {
			ms = append(ms, s3Store(strings.TrimSpace(ep)))
		}
		rp := strings.Trim(*prefix, "/") + "/" + *repoName
		st = store.NewMirror(ms, store.WithMutableOrder(fs.CompareMutable),
			store.WithRepoPrefix(rp), store.WithLazyRepair())
	case "s3":
		st = s3Store(strings.Split(*endpoints, ",")[0])
	case "file":
		os.MkdirAll(*root, 0o755)
		s, err := store.NewFileStore(*root)
		if err != nil {
			fmt.Fprintln(os.Stderr, "store:", err)
			os.Exit(1)
		}
		st = s
	default:
		fmt.Fprintln(os.Stderr, "unknown -store")
		os.Exit(2)
	}
	rp := strings.Trim(*prefix, "/") + "/" + *repoName
	if *rawBench > 0 {
		ctx := context.Background()
		data := make([]byte, 1024)
		var put, get time.Duration
		for i := 0; i < *rawBench; i++ {
			key := fmt.Sprintf("rawbench/%d/%d", os.Getpid(), i)
			t := time.Now()
			if err := st.Put(ctx, key, data); err != nil {
				fmt.Fprintln(os.Stderr, "put:", err)
				os.Exit(1)
			}
			put += time.Since(t)
			t = time.Now()
			if _, err := st.Get(ctx, key, nil); err != nil {
				fmt.Fprintln(os.Stderr, "get:", err)
				os.Exit(1)
			}
			get += time.Since(t)
		}
		n := time.Duration(*rawBench)
		fmt.Printf("raw1KiB put_avg=%s get_avg=%s\n", put/n, get/n)
		return
	}
	r, err := fs.OpenRepo(st, rp)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	head, err := r.HeadFresh()
	if err != nil {
		fmt.Fprintln(os.Stderr, "head:", err)
		os.Exit(1)
	}
	first := *start
	if first < 0 {
		first = int64(head)
	}
	last := first + *target
	fmt.Printf("repo=%s head=%d range=%d..%d workers=%d mixed=%v\n", *repoName, head, first+1, last, *workers, *mixed)

	stop := make(chan struct{})
	var done int64
	var errCount int64
	var sumMs int64
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sig; close(stop) }()

	t0 := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(w)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				n := atomic.AddInt64(&done, 1)
				if first+n > last {
					return
				}
				idx := first + n
				ch := changeFor(idx, rng, *mixed)
				props := map[string]string{
					"svn:author": authors[idx%int64(len(authors))],
					"svn:date":   time.Now().UTC().Format("2006-01-02T15:04:05.000000Z"),
				}
				ct := time.Now()
				_, cerr := r.CommitTxn(0, ch, props)
				d := time.Since(ct)
				atomic.AddInt64(&sumMs, d.Milliseconds())
				if cerr != nil {
					atomic.AddInt64(&errCount, 1)
					fmt.Printf("rev %d ERR %v\n", idx, cerr)
					if *bench {
						return
					}
					continue
				}
				if *bench {
					fmt.Printf("%d %dms\n", idx, d.Milliseconds())
				}
			}
		}(w)
	}

	tick := time.NewTicker(*progress)
	defer tick.Stop()
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	for {
		select {
		case <-tick.C:
			d := atomic.LoadInt64(&done)
			el := time.Since(t0).Seconds()
			var avg float64
			if d > 0 {
				avg = float64(atomic.LoadInt64(&sumMs)) / float64(d)
			}
			fmt.Printf("progress %d/%d elapsed=%.0fs rate=%.1f/s avg_commit=%.1fms errors=%d\n",
				d, *target, el, float64(d)/el, avg, atomic.LoadInt64(&errCount))
		case <-finished:
			el := time.Since(t0).Seconds()
			d := atomic.LoadInt64(&done)
			fmt.Printf("done %d revisions in %.1fs (%.1f/s), errors=%d\n",
				d, el, float64(d)/el, atomic.LoadInt64(&errCount))
			if atomic.LoadInt64(&errCount) > 0 {
				os.Exit(1)
			}
			return
		}
	}
}

// changeFor builds the change set for revision index idx. Adds land on a
// digit-sharded path; mixed mode rewrites or deletes an earlier path instead.
func changeFor(idx int64, rng *rand.Rand, mixed bool) []fs.Change {
	if mixed && rng.Intn(100) < 5 {
		// Delete an earlier file (never revision 0) and turn its parent leaf
		// into a property change sometimes, so deletes, props and mods share
		// the same commit stream.
		prev := idx / 10 * 9
		path, _ := pathFor(prev)
		return []fs.Change{{Path: path, Action: fs.ActionDelete, Kind: fs.KindFile}}
	}
	if mixed && rng.Intn(100) < 10 {
		prev := idx / 10 * 8
		path, _ := pathFor(prev)
		return []fs.Change{{Path: path, Action: fs.ActionModify, Kind: fs.KindFile,
			Data: []byte(fmt.Sprintf("modified by %d\n", idx))}}
	}
	path, _ := pathFor(idx)
	return []fs.Change{{Path: path, Action: fs.ActionAdd, Kind: fs.KindFile,
		Data: []byte(fmt.Sprintf("r%d\n", idx))}}
}

// pathFor maps revision n to /d0/d1/d2/d3/d4/d5/f<d6>.txt with six decimal
// digits (10M revisions). Each directory holds at most ten children, so a
// commit rewrites seven small nodes regardless of repository size.
func pathFor(n int64) (string, string) {
	digits := fmt.Sprintf("%07d", n%10000000)
	dirs := digits[:6]
	file := "f" + digits[6:] + ".txt"
	var b strings.Builder
	for i := 0; i < 6; i++ {
		b.WriteByte('/')
		b.WriteByte(dirs[i])
	}
	p := b.String()
	return p + "/" + file, p
}