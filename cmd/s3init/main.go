// Command s3init creates an svngod repository on an S3-compatible backend so
// that an svngod --config (storage.type=s3) instance can serve it. svngod does
// not auto-create repos, so they are bootstrapped here (mirrors svnadmin create
// for local disk).
package main

import (
	"fmt"
	"os"
	"svngod/internal/fs"
	"svngod/internal/fs/store"
)

func main() {
	endpoint := os.Getenv("FAKE_S3_ADDR")
	if endpoint == "" {
		endpoint = "127.0.0.1:9000"
	}
	bucket := os.Getenv("FAKE_S3_BUCKET")
	if bucket == "" {
		bucket = "svn"
	}
	name := "demo"
	if len(os.Args) > 1 {
		name = os.Args[1]
	}
	ak := os.Getenv("RUSTFS_AK")
	if ak == "" {
		ak = "test"
	}
	sk := os.Getenv("RUSTFS_SK")
	if sk == "" {
		sk = "test"
	}
	st, err := store.NewS3Store(store.S3Config{
		Endpoint:   endpoint,
		AccessKey:  ak,
		SecretKey:  sk,
		Bucket:     bucket,
		Region:     "us-east-1",
		RepoPrefix: "repos",
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "store:", err)
		os.Exit(1)
	}
	if _, err := fs.CreateRepo(st, name, 4096, 8192); err != nil {
		fmt.Fprintln(os.Stderr, "create:", err)
		os.Exit(1)
	}
	fmt.Println("created s3 repo:", name)
}
