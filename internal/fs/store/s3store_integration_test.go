package store

import (
	"bytes"
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// s3TestEnv reads live-S3 connection settings; returns ("", false) to skip.
func s3TestEnv(t *testing.T) (S3Config, bool) {
	t.Helper()
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		return S3Config{}, false
	}
	cfg := S3Config{
		Endpoint:  endpoint,
		AccessKey: or(os.Getenv("S3_ACCESS_KEY"), "test"),
		SecretKey: or(os.Getenv("S3_SECRET_KEY"), "test"),
		Bucket:    or(os.Getenv("S3_BUCKET"), "svngod-test"),
		Region:    os.Getenv("S3_REGION"),
		UseSSL:    os.Getenv("S3_SSL") == "1",
	}
	return cfg, true
}

func or(a, b string) string {
	if a == "" {
		return b
	}
	return a
}

// ensureBucket creates the test bucket idempotently via a raw minio client.
func ensureBucket(t *testing.T, cfg S3Config) {
	t.Helper()
	cl, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	ctx := context.Background()
	if ok, err := cl.BucketExists(ctx, cfg.Bucket); err != nil {
		t.Fatalf("bucket exists check: %v", err)
	} else if !ok {
		if err := cl.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{Region: cfg.Region}); err != nil {
			t.Fatalf("make bucket: %v", err)
		}
	}
}

func TestS3StorePrimitives(t *testing.T) {
	cfg, ok := s3TestEnv(t)
	if !ok {
		t.Skip("S3_ENDPOINT not set; skipping live S3 integration test")
	}
	ensureBucket(t, cfg)
	cfg.RepoPrefix = "it/" + strconv.FormatInt(time.Now().UnixNano(), 36)
	st, err := NewS3Store(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// Put + Get (full and range) + Head
	if err := st.Put(ctx, "a/b", []byte("hello s3")); err != nil {
		t.Fatal(err)
	}
	got, err := st.Get(ctx, "a/b", nil)
	if err != nil || string(got) != "hello s3" {
		t.Fatalf("get: %q err=%v", got, err)
	}
	// range [6..8) → "s3"
	got, err = st.Get(ctx, "a/b", &Range{Offset: 6, Length: 2})
	if err != nil || string(got) != "s3" {
		t.Fatalf("range get: %q err=%v", got, err)
	}
	etag, exists, err := st.Head(ctx, "a/b")
	if err != nil || !exists || etag == "" {
		t.Fatalf("head: %q exists=%v err=%v", etag, exists, err)
	}
	// missing object
	if _, err := st.Get(ctx, "nope", nil); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if _, exists, err := st.Head(ctx, "nope"); err != nil || exists {
		t.Fatalf("head missing: exists=%v err=%v", exists, err)
	}

	// CAS: correct expect succeeds and changes etag; wrong expect fails.
	if _, err := st.CAS(ctx, "a/b", etag, []byte("hello s3 v2")); err != nil {
		t.Fatalf("cas: %v", err)
	}
	got, _ = st.Get(ctx, "a/b", nil)
	if string(got) != "hello s3 v2" {
		t.Fatalf("cas content: %q", got)
	}
	if _, err := st.CAS(ctx, "a/b", etag, []byte("x")); err != ErrPreconditionFailed {
		t.Fatalf("cas stale: got %v, want ErrPreconditionFailed", err)
	}

	// CreateIfAbsent
	if err := st.CreateIfAbsent(ctx, "lock", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateIfAbsent(ctx, "lock", []byte("2")); err != ErrPreconditionFailed {
		t.Fatalf("createIfAbsent existing: got %v", err)
	}

	// ListPrefix enumerates objects under a prefix, and Delete removes them.
	// (Exact continuation paging depends on S3 start-after support, which some
	// mocks like moto ignore; we assert enumeration + deletion here.)
	if err := st.Put(ctx, "dir/x", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := st.Put(ctx, "dir/y", []byte("yy")); err != nil {
		t.Fatal(err)
	}
	objs, _, err := st.ListPrefix(ctx, "dir/", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, o := range objs {
		seen[o.Key] = true
	}
	if !seen["dir/x"] || !seen["dir/y"] {
		t.Fatalf("list missed objects: %v", seen)
	}
	var keys []string
	for _, o := range objs {
		keys = append(keys, o.Key)
	}
	if err := st.Delete(ctx, keys); err != nil {
		t.Fatal(err)
	}
	rest, _, err := st.ListPrefix(ctx, "dir/", "", 100)
	if err != nil || len(rest) != 0 {
		t.Fatalf("after delete, still %d objs: %v", len(rest), err)
	}
	if !bytes.Equal(mustGet(t, st, ctx, "a/b"), []byte("hello s3 v2")) {
		t.Fatal("a/b unexpectedly removed")
	}
}

func mustGet(t *testing.T, st Store, ctx context.Context, key string) []byte {
	t.Helper()
	b, err := st.Get(ctx, key, nil)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	return b
}
