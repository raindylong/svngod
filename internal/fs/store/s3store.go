package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
// CAS / CreateIfAbsent note: minio-go (v7) does not expose S3 conditional
// PutObject (If-Match / If-None-Match), and CopyObject self-copy cannot
// change an object's body. Therefore these two methods here are single-writer
// best-effort: a Head pre-check followed by an atomic PutObject. This is
// correct for the single-process design, where the process mutex plus S3's
// object-level PutObject atomicity (read-after-write, never a torn object)
// provide the real guarantee. Truly atomic multi-writer CAS requires
// conditional writes (AWS SDK v2, 2024+); that is the documented upgrade path.
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

// CreateIfAbsent is single-writer best-effort (see NewS3Store note).
func (s *S3Store) CreateIfAbsent(ctx context.Context, key string, data []byte) error {
	if _, ok, err := s.Head(ctx, key); err != nil {
		return err
	} else if ok {
		return ErrPreconditionFailed
	}
	return s.Put(ctx, key, data)
}

// CAS is single-writer: Head + atomic PutObject (see NewS3Store note).
func (s *S3Store) CAS(ctx context.Context, key string, expectETag string, data []byte) (string, error) {
	if cur, ok, err := s.Head(ctx, key); err != nil {
		return "", err
	} else if !ok {
		return "", ErrNotFound
	} else if cur != expectETag {
		return "", ErrPreconditionFailed
	}
	if err := s.Put(ctx, key, data); err != nil {
		return "", err
	}
	info, err := s.client.StatObject(ctx, s.bucket, s.key(key), minio.StatObjectOptions{})
	if err != nil {
		return "", err
	}
	return info.ETag, nil
}

func (s *S3Store) ListPrefix(ctx context.Context, prefix, continuation string, max int) ([]ListedObj, string, error) {
	if max <= 0 {
		max = 1000
	}
	opts := minio.ListObjectsOptions{Prefix: s.key(prefix), Recursive: true, MaxKeys: max}
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
