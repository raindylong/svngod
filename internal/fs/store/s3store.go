package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

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
	// A short dial timeout bounds failover latency: without it a dead primary
	// costs several seconds of retries on the first request after it goes away.
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	opt := &minio.Options{BucketLookup: minio.BucketLookupAuto, Secure: cfg.UseSSL, Transport: tr}
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
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	exists, err := cl.BucketExists(ctx, cfg.Bucket)
	cancel()
	if err != nil {
		// Reverse proxies (nginx in front of RustFS) commonly answer the
		// bucket-root HEAD probe with a 301 redirect even though object-path
		// requests work fine. A transport failure is handled the same way: a
		// mirror whose primary is down at startup must still come up so the
		// standby can serve; the first object operation reports a real error.
		merr := minio.ToErrorResponse(err)
		if merr.StatusCode == 0 && merr.Code == "" {
			exists = true
		} else if merr.StatusCode == 301 || merr.StatusCode == 307 || merr.StatusCode == 405 || merr.StatusCode == 403 || merr.StatusCode == 401 {
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

// retry runs op up to three times for transient object-store failures
// (connection resets, timeouts, 5xx, throttling). A long commit uploads tens
// of thousands of small objects; one transient error used to abort the whole
// client commit, so the server retries instead of surfacing it.
func retry(ctx context.Context, op func() error) error {
	backoff := 100 * time.Millisecond
	for attempt := 0; ; attempt++ {
		err := op()
		if err == nil || attempt >= 4 || !isTransient(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 3
	}
}

// isTransient reports whether an S3 error is worth retrying.
func isTransient(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrNotFound) || errors.Is(err, ErrPreconditionFailed) {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	resp := minio.ToErrorResponse(err)
	switch resp.StatusCode {
	case 0, 408, 429, 500, 502, 503, 504:
		return true
	}
	switch resp.Code {
	case "RequestTimeout", "SlowDown", "InternalError", "ServiceUnavailable", "XMinioServerNotInitialized":
		return true
	}
	return false
}

// seekableReader rewinds r for a retry; nil when r cannot be replayed.
func seekableReader(r io.Reader) io.ReadSeeker {
	if s, ok := r.(io.ReadSeeker); ok {
		return s
	}
	return nil
}

// singlePut is used for every object: one PUT up to 5 GiB instead of a
// multipart upload. svngod's largest object is a 512 MiB text, so multipart
// only added round trips (and required the gateway to implement it).
var singlePut = minio.PutObjectOptions{ContentType: "application/octet-stream", PartSize: 5 << 30}

func (s *S3Store) Put(ctx context.Context, key string, data []byte) error {
	return retry(ctx, func() error {
		_, err := s.client.PutObject(ctx, s.bucket, s.key(key), bytes.NewReader(data), int64(len(data)), singlePut)
		return err
	})
}

func (s *S3Store) Get(ctx context.Context, key string, rng *Range) ([]byte, error) {
	var buf []byte
	err := retry(ctx, func() error {
		var gerr error
		buf, gerr = s.getOnce(ctx, key, rng)
		return gerr
	})
	if err != nil {
		if isNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return buf, nil
}

func (s *S3Store) getOnce(ctx context.Context, key string, rng *Range) ([]byte, error) {
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
		return nil, err
	}
	return buf, nil
}

func (s *S3Store) PutFrom(ctx context.Context, key string, r io.Reader, size int64) error {
	sr := seekableReader(r)
	return retry(ctx, func() error {
		if sr != nil {
			if _, err := sr.Seek(0, io.SeekStart); err != nil {
				return err
			}
		}
		_, err := s.client.PutObject(ctx, s.bucket, s.key(key), io.LimitReader(srOr(r, sr), size), size, singlePut)
		return err
	})
}

// srOr prefers the seekable reader when the caller supplied one.
func srOr(r io.Reader, sr io.ReadSeeker) io.Reader {
	if sr != nil {
		return sr
	}
	return r
}

func isNotFound(err error) bool {
	resp := minio.ToErrorResponse(err)
	return resp.StatusCode == http.StatusNotFound || resp.Code == "NoSuchKey" || resp.Code == "NoSuchBucket"
}

func (s *S3Store) Exists(ctx context.Context, key string) (bool, error) {
	var exists bool
	err := retry(ctx, func() error {
		_, err := s.client.StatObject(ctx, s.bucket, s.key(key), minio.StatObjectOptions{})
		if err != nil {
			if isNotFound(err) {
				exists = false
				return nil
			}
			return err
		}
		exists = true
		return nil
	})
	return exists, err
}

func (s *S3Store) GetTo(ctx context.Context, key string, w io.Writer) (int64, error) {
	var n int64
	err := retry(ctx, func() error {
		obj, gerr := s.client.GetObject(ctx, s.bucket, s.key(key), minio.GetObjectOptions{})
		if gerr != nil {
			return gerr
		}
		defer obj.Close()
		var cerr error
		n, cerr = io.Copy(w, obj)
		if cerr != nil && n > 0 {
			// Bytes already reached w; a retry would duplicate them.
			return &noRetryError{cerr}
		}
		return cerr
	})
	if err != nil {
		var nr *noRetryError
		if errors.As(err, &nr) {
			return n, nr.err
		}
		if isNotFound(err) {
			return n, ErrNotFound
		}
		return n, err
	}
	return n, nil
}

// noRetryError marks a failure that must not be retried (partial output).
type noRetryError struct{ err error }

func (e *noRetryError) Error() string { return e.err.Error() }
func (e *noRetryError) Unwrap() error { return e.err }

func (s *S3Store) Head(ctx context.Context, key string) (string, bool, error) {
	var etag string
	err := retry(ctx, func() error {
		info, err := s.client.StatObject(ctx, s.bucket, s.key(key), minio.StatObjectOptions{})
		if err != nil {
			if isNotFound(err) {
				etag = ""
				return nil
			}
			return err
		}
		etag = info.ETag
		return nil
	})
	if err != nil {
		return "", false, err
	}
	return etag, etag != "", nil
}

// CreateIfAbsent creates the object only if it does not exist. The write uses
// If-None-Match: * so it is atomic across replicas; gateways that do not
// implement conditional writes fall back to a HEAD+PUT (best effort).
func (s *S3Store) CreateIfAbsent(ctx context.Context, key string, data []byte) error {
	var err error
	_ = retry(ctx, func() error {
		err = s.createIfAbsentOnce(ctx, key, data)
		return err
	})
	return err
}

func (s *S3Store) createIfAbsentOnce(ctx context.Context, key string, data []byte) error {
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
	var etag string
	err := retry(ctx, func() error {
		var cerr error
		etag, cerr = s.casOnce(ctx, key, expectETag, data)
		return cerr
	})
	return etag, err
}

func (s *S3Store) casOnce(ctx context.Context, key string, expectETag string, data []byte) (string, error) {
	info, err := s.client.StatObject(ctx, s.bucket, s.key(key), minio.StatObjectOptions{})
	if err != nil {
		if isNotFound(err) {
			return "", ErrNotFound
		}
		return "", err
	}
	etag := strings.Trim(info.ETag, `"`)
	// S3 (and MinIO) return the content MD5 as the ETag of a single-part
	// object, so a matching stat ETag proves the stored bytes are the ones the
	// caller based its change on and the body does not need to be read again.
	// Gateways with opaque ETags fall back to reading and hashing the body.
	if etag != expectETag {
		cur, gerr := s.Get(ctx, key, nil)
		if gerr != nil {
			if errors.Is(gerr, ErrNotFound) {
				return "", ErrNotFound
			}
			return "", gerr
		}
		if ContentETag(cur) != expectETag {
			return "", ErrPreconditionFailed
		}
	}
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
	var out []ListedObj
	var next string
	err := retry(ctx, func() error {
		var lerr error
		out, next, lerr = s.listPrefixOnce(ctx, prefix, continuation, max)
		return lerr
	})
	return out, next, err
}

func (s *S3Store) listPrefixOnce(ctx context.Context, prefix, continuation string, max int) ([]ListedObj, string, error) {
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

// DeleteIfMatch uses a conditional put (If-Match) to claim the object, then
// deletes it. The claimed payload has no timestamp, so a crash between the two
// steps looks stale to the next lease waiter.
func (s *S3Store) DeleteIfMatch(ctx context.Context, key, etag string) error {
	if _, err := s.CAS(ctx, key, etag, []byte("deleted")); err != nil {
		return err
	}
	return s.Delete(ctx, []string{key})
}

func (s *S3Store) Delete(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	// A transient failure here (session cleanup, GC) is retried rather than
	// leaking objects.
	return retry(ctx, func() error { return s.deleteOnce(ctx, keys) })
}

func (s *S3Store) deleteOnce(ctx context.Context, keys []string) error {
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
