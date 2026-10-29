// Command loadbulk writes the 30000×50K source files (chunk0..29, 1000 files
// each) straight into a repository through the fs layer, bypassing the svn CLI
// client (which is slow on large dirs due to per-file PROPFIND). It runs against
// either a local file store or an S3 store, reports per-chunk commit latency and
// the overall wall time, so it doubles as a backend stability + throughput
// comparison harness.
package main

import (
	"flag"
	"fmt"
	iofs "io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"svngod/internal/fs"
	"svngod/internal/fs/store"
)

func main() {
	storeType := flag.String("store", "file", "file|s3")
	root := flag.String("root", "/tmp/loadbulk", "file store base dir")
	dir := flag.String("dir", "/home/user/svngod/bulkchunk", "source chunk dir (chunk0..29)")
	repo := flag.String("repo", "repos/demo", "repo prefix")
	endpoint := flag.String("endpoint", "127.0.0.1:9000", "s3 endpoint")
	ak := flag.String("ak", "rustfsadmin", "s3 access key")
	sk := flag.String("sk", "rustfsadmin", "s3 secret key")
	bucket := flag.String("bucket", "svn", "s3 bucket")
	prefix := flag.String("prefix", "repos", "s3 repo prefix")
	flag.Parse()

	var st store.Store
	var err error
	if *storeType == "s3" {
		st, err = store.NewS3Store(store.S3Config{
			Endpoint: *endpoint, AccessKey: *ak, SecretKey: *sk,
			Bucket: *bucket, RepoPrefix: *prefix,
		})
	} else {
		os.RemoveAll(*root)
		if err := os.MkdirAll(*root, 0o755); err != nil {
			panic(err)
		}
		st, err = store.NewFileStore(*root)
	}
	if err != nil {
		panic(err)
	}

	r, err := fs.CreateRepo(st, *repo, 4096, 64)
	if err != nil {
		panic(err)
	}
	rp := map[string]string{
		"svn:author": "loadbulk",
		"svn:date":   time.Now().UTC().Format("2006-01-02T15:04:05.000000Z"),
	}

	var total int
	loadStart := time.Now()
	for chunk := 0; chunk <= 29; chunk++ {
		chdir := filepath.Join(*dir, fmt.Sprintf("chunk%d", chunk))
		names, err := readNames(chdir)
		if err != nil {
			fmt.Printf("chunk%d read err: %v\n", chunk, err)
			continue
		}
		if len(names) == 0 {
			continue
		}
		changes := make([]fs.Change, 0, len(names))
		for _, n := range names {
			data, err := os.ReadFile(filepath.Join(chdir, n))
			if err != nil {
				panic(err)
			}
			changes = append(changes, fs.Change{
				// Each chunk lands in its own subdir bulk/cN so the root dir
				// stays shallow (30 entries); a flat root of 30000 files makes
				// every commit rewrite the root's full child list => O(n^2).
				Path: fmt.Sprintf("/c%02d/%s", chunk, n),
				Action: fs.ActionAdd, Kind: fs.KindFile, Data: data,
			})
		}
		t0 := time.Now()
		rev, err := r.CommitTxn(changes, rp)
		if err != nil {
			fmt.Printf("chunk%d FAIL: %v\n", chunk, err)
			continue
		}
		total += len(changes)
		fmt.Printf("chunk%d %d files rev=%d %.2fs\n", chunk, len(changes), rev, time.Since(t0).Seconds())
	}
	fmt.Printf("TOTAL %d files in %.2fs\n", total, time.Since(loadStart).Seconds())
}

func readNames(dir string) ([]string, error) {
	var names []string
	err := filepath.WalkDir(dir, func(p string, d iofs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		names = append(names, filepath.Base(p))
		return nil
	})
	sort.Strings(names)
	return names, err
}

var _ = strings.TrimSpace
