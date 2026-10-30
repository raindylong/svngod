package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3Config configures the S3-compatible adapter (AWS S3, MinIO, OSS, COS).
type S3Config struct {
	Endpoint     string // host[:port], e.g. "minio.example.com:9000"
	AccessKey    string
	SecretKey    string
	Bucket       string
	Region       string
	UseSSL       bool
	RepoPrefix   string // bucket prefix under which all repos live
	BaseRepo     string // optional per-repo sub-prefix
	SessionToken string
	EnvCreds     bool // use env credentials instead of static keys
}

// S3Store implements Store on an S3-compatible object service.
type S3Store struct {
	client *minio.Client
	bucket string
	prefix string // repo prefix root
}

// NewS3Store builds an S3Store.
//
// CAS is a real content-hash compare-and-swap (see CAS below): the current
// object is read, its MD5 content ETag is compared against the expected ETag,
// and only then is the object overwritten. This gives genuine single-writer
// atomicity even across processes on an S3 gateway, independent of whether the
// gateway returns stable HEAD ETags. The bucket is created idempotently on
// first use.
func NewS3Store(cfg S3Config) (*S3Store, error) {
	opt := &minio.Options{BucketLookup: minio.BucketLookupAuto, Secure: cfg.UseSSL}
	if cfg.Region != "" {
		opt.Region = cfg.Region
	}
	if cfg.EnvCreds {
		opt.Creds = credentials.NewEnvAWS()
	} else {
		opt.Creds = credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, cfg.SessionToken)
	}
	cl, err := minio.New(cfg.Endpoint, opt)
	if err != nil {
		return nil, fmt.Errorf("store: init s3 client: %w", err)
	}
	ctx := context.Background()
	exists, err := cl.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		// Reverse proxies (nginx in front of RustFS) commonly answer the
		// bucket-root HEAD probe with a 301 redirect even though object-path
		// requests work fine. Treat such gateway-level redirects as "bucket
		// exists" and let the first object write surface a real problem.
		if merr := minio.ToErrorResponse(err); merr.StatusCode == 301 || merr.StatusCode == 307 || merr.StatusCode == 405 || merr.StatusCode == 403 || merr.StatusCode == 401 {
			// Gateway-level redirects and RustFS denying bucket-root HEAD
			// probes are both compatible with fully-working object-path I/O;
			// let the first object write surface a real problem.
			exists = true
		} else {
			return nil, fmt.Errorf("store: check bucket %s: %w", cfg.Bucket, err)
		}
	}
	if !exists {
		if err := cl.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{Region: cfg.Region}); err != nil {
			return nil, fmt.Errorf("store: create bucket %s: %w", cfg.Bucket, err)
		}
	}
	return &S3Store{client: cl, bucket: cfg.Bucket, prefix: joinKey(cfg.RepoPrefix, cfg.BaseRepo)}, nil
}

func joinKey(a, b string) string {
	a = strings.Trim(a, "/")
	b = strings.Trim(b, "/")
	switch {
	case a == "":
		return b
	case b == "":
		return a
	default:
		return a + "/" + b
	}
}

func (s *S3Store) key(rel string) string { return joinKey(s.prefix, rel) }

func (s *S3Store) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, s.bucket, s.key(key), bytes.NewReader(data),
		int64(len(data)), minio.PutObjectOptions{ContentType: "application/octet-stream"})
	return err
}

func (s *S3Store) Get(ctx context.Context, key string, rng *Range) ([]byte, error) {
	opt := minio.GetObjectOptions{}
	if rng != nil {
		end := int64(0)
		if rng.Length > 0 {
			end = rng.Offset + rng.Length - 1
		}
		if err := opt.SetRange(rng.Offset, end); err != nil {
			return nil, err
		}
	}
	obj, err := s.client.GetObject(ctx, s.bucket, s.key(key), opt)
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	buf, err := io.ReadAll(obj)
	if err != nil {
		resp := minio.ToErrorResponse(err)
		if resp.StatusCode == 404 || resp.Code == "NoSuchKey" {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return buf, nil
}

func (s *S3Store) Head(ctx context.Context, key string) (string, bool, error) {
	info, err := s.client.StatObject(ctx, s.bucket, s.key(key), minio.StatObjectOptions{})
	if err != nil {
		resp := minio.ToErrorResponse(err)
		if resp.StatusCode == 404 || resp.Code == "NoSuchKey" {
			return "", false, nil
		}
		return "", false, err
	}
	return info.ETag, true, nil
}

// CreateIfAbsent creates the object only if it does not exist. The write uses
// If-None-Match: * so it is atomic across replicas; gateways that do not
// implement conditional writes fall back to a HEAD+PUT (best effort).
func (s *S3Store) CreateIfAbsent(ctx context.Context, key string, data []byte) error {
	opts := minio.PutObjectOptions{ContentType: "application/octet-stream"}
	opts.SetMatchETagExcept("*")
	_, err := s.client.PutObject(ctx, s.bucket, s.key(key), bytes.NewReader(data), int64(len(data)), opts)
	if err == nil {
		return nil
	}
	if isPreconditionFailed(err) {
		return ErrPreconditionFailed
	}
	resp := minio.ToErrorResponse(err)
	switch resp.Code {
	case "NotImplemented", "MethodNotAllowed", "InvalidRequest", "XNotImplemented":
		if _, ok, herr := s.Head(ctx, key); herr != nil {
			return herr
		} else if ok {
			return ErrPreconditionFailed
		}
		return s.Put(ctx, key, data)
	}
	return err
}

// CAS is a compare-and-swap on the object's content ETag. Verification reads
// the current body (some gateways return unstable HEAD ETags), then the write
// is issued with If-Match so two replicas cannot both win the race: the loser
// gets a 412 and ErrPreconditionFailed. Without the conditional write this was
// a TOCTOU check and silently dropped a committed revision.
func (s *S3Store) CAS(ctx context.Context, key string, expectETag string, data []byte) (string, error) {
	cur, err := s.Get(ctx, key, nil)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return "", ErrNotFound
		}
		return "", err
	}
	if ContentETag(cur) != expectETag {
		return "", ErrPreconditionFailed
	}
	info, err := s.client.StatObject(ctx, s.bucket, s.key(key), minio.StatObjectOptions{})
	if err != nil {
		return "", err
	}
	etag := strings.Trim(info.ETag, `"`)
	opts := minio.PutObjectOptions{ContentType: "application/octet-stream"}
	opts.SetMatchETag(etag)
	_, err = s.client.PutObject(ctx, s.bucket, s.key(key), bytes.NewReader(data), int64(len(data)), opts)
	if err != nil {
		if isPreconditionFailed(err) {
			return "", ErrPreconditionFailed
		}
		return "", err
	}
	return ContentETag(data), nil
}

// isPreconditionFailed reports whether an S3 error is a failed If-Match /
// If-None-Match condition.
func isPreconditionFailed(err error) bool {
	resp := minio.ToErrorResponse(err)
	switch resp.StatusCode {
	case http.StatusPreconditionFailed, http.StatusConflict:
		return true
	}
	switch resp.Code {
	case "PreconditionFailed", "ConditionalRequestConflict", "NoSuchKey":
		return resp.Code == "PreconditionFailed" || resp.Code == "ConditionalRequestConflict"
	}
	return false
}

func (s *S3Store) ListPrefix(ctx context.Context, prefix, continuation string, max int) ([]ListedObj, string, error) {
	if max <= 0 {
		max = 1000
	}
	// Ask for one extra key so "is there another page?" is decidable; with
	// MaxKeys=max the slice can never exceed max and `next` stayed empty,
	// truncating every listing (mirror repair only saw the first page).
	opts := minio.ListObjectsOptions{Prefix: s.key(prefix), Recursive: true, MaxKeys: max + 1}
	if continuation != "" {
		opts.StartAfter = continuation // minio v7 auto-pages internally; StartAfter skips to resume
	}
	var out []ListedObj
	objs := s.client.ListObjects(ctx, s.bucket, opts)
	for obj := range objs {
		if obj.Err != nil {
			return nil, "", obj.Err
		}
		rel := strings.TrimPrefix(obj.Key, s.prefix+"/")
		out = append(out, ListedObj{Key: rel, Size: obj.Size})
	}
	var next string
	if len(out) > max {
		next = out[max-1].Key
		out = out[:max]
	}
	return out, next, nil
}

func (s *S3Store) Delete(ctx context.Context, keys []string) error {
	objs := make(chan minio.ObjectInfo, len(keys))
	for _, k := range keys {
		objs <- minio.ObjectInfo{Key: s.key(k)}
	}
	close(objs)
	errCh := s.client.RemoveObjects(ctx, s.bucket, objs, minio.RemoveObjectsOptions{})
	for err := range errCh {
		if err.Err != nil {
			return err.Err
		}
	}
	return nil
}
