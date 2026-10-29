package main

import (
	"context"
	"fmt"
	"os"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// 临时工具：在 S3 兼容后端建 bucket（幂等），用于 RustFS 集成验证。
func main() {
	endpoint := os.Getenv("S3_ADDR")
	if endpoint == "" {
		endpoint = "127.0.0.1:9000"
	}
	ak := os.Getenv("RUSTFS_AK")
	if ak == "" {
		ak = "rustfsadmin"
	}
	sk := os.Getenv("RUSTFS_SK")
	if sk == "" {
		sk = "rustfsadmin"
	}
	bucket := os.Getenv("S3_BUCKET")
	if bucket == "" {
		bucket = "svn"
	}
	mc, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(ak, sk, ""),
		Secure: false,
	})
	if err != nil {
		fmt.Println("new:", err)
		os.Exit(1)
	}
	err = mc.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{})
	fmt.Println("makebucket:", err)
	// 列出 bucket 验证
	bs, lerr := mc.ListBuckets(context.Background())
	fmt.Println("listbuckets:", lerr, len(bs))
}
