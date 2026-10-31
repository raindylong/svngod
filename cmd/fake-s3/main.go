// Command fake-s3 is an S3-compatible object store used as a test backend for
// svngod's S3Store and MirrorStore. It implements the object-plane subset
// minio-go uses: bucket create/head, single-object PUT/GET/HEAD/DELETE (with
// Range, If-Match and If-None-Match), ListObjectsV2 with pagination, and
// DeleteObjects. Data lives on local disk; SigV4 signatures are not validated
// (test-only).
//
// Environment:
//
//	FAKE_S3_ROOT       data directory (default /home/user/svngod/s3data)
//	FAKE_S3_ADDR       listen address (default :9000)
//	FAKE_S3_DELAY_MS   artificial per-request latency, to emulate a remote S3
//	FAKE_S3_FAIL       "1" to fail every request (simulate a down backend)
package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

func isChunked(r *http.Request) bool {
	ce := r.Header.Get("Content-Encoding")
	if strings.Contains(ce, "chunked") || strings.Contains(ce, "aws") || strings.Contains(ce, "streaming") {
		return true
	}
	return strings.HasPrefix(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING")
}

// copyChunked decodes an aws-chunked / x-amz-chunked body:
// "<hex-size>[;chunk-signature=...]\r\n<data>\r\n" repeated until "0[...]\r\n\r\n".
func copyChunked(r io.Reader, w io.Writer) (int64, error) {
	br := bufio.NewReader(r)
	var total int64
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return total, err
		}
		line = strings.TrimRight(strings.TrimRight(line, "\n"), "\r")
		sizeHex := line
		if i := strings.Index(line, ";"); i >= 0 {
			sizeHex = line[:i]
		}
		size, err := strconv.ParseInt(strings.TrimSpace(sizeHex), 16, 64)
		if err != nil {
			return total, fmt.Errorf("bad chunk size %q: %w", sizeHex, err)
		}
		if size == 0 {
			io.CopyN(io.Discard, br, 2) // trailing \r\n
			return total, nil
		}
		n, err := io.CopyN(w, br, size)
		total += n
		if err != nil {
			return total, err
		}
		io.CopyN(io.Discard, br, 2) // \r\n after chunk data
	}
}

type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (s *statusWriter) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Write(p []byte) (int, error) {
	if s.status == 0 {
		s.status = 200
	}
	n, err := s.ResponseWriter.Write(p)
	s.bytes += int64(n)
	return n, err
}

func (s *statusWriter) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// objPath maps a bucket-relative key to a file under root/bucket.
func objPath(root, bucket, key string) string {
	return filepath.Join(root, bucket, filepath.FromSlash(key))
}

// metaPath stores the upload ETag (quoted md5) without reading the object.
func metaPath(root, bucket, key string) string {
	return filepath.Join(root, ".etag", bucket, filepath.FromSlash(key)+".etag")
}

func readETag(root, bucket, key string) string {
	b, err := os.ReadFile(metaPath(root, bucket, key))
	if err != nil {
		return ""
	}
	return string(b)
}

func writeETag(root, bucket, key, etag string) {
	p := metaPath(root, bucket, key)
	os.MkdirAll(filepath.Dir(p), 0o755)
	os.WriteFile(p, []byte(etag), 0o644)
}

func removeETag(root, bucket, key string) {
	os.Remove(metaPath(root, bucket, key))
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message><RequestId>fake</RequestId></Error>`, code, msg)
}

func main() {
	root := os.Getenv("FAKE_S3_ROOT")
	if root == "" {
		root = "/home/user/svngod/s3data"
	}
	addr := os.Getenv("FAKE_S3_ADDR")
	if addr == "" {
		addr = ":9000"
	}
	var delay time.Duration
	if ms, _ := strconv.Atoi(os.Getenv("FAKE_S3_DELAY_MS")); ms > 0 {
		delay = time.Duration(ms) * time.Millisecond
	}
	failAll := os.Getenv("FAKE_S3_FAIL") == "1"
	errPct := 0
	if v, err := strconv.Atoi(os.Getenv("FAKE_S3_ERROR_PCT")); err == nil && v > 0 {
		errPct = v
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	if err := os.MkdirAll(root, 0o755); err != nil {
		panic(err)
	}

	listKeys := func(bucket, prefix string) []string {
		var keys []string
		bdir := filepath.Join(root, bucket)
		filepath.Walk(bdir, func(p string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}
			rel := filepath.ToSlash(strings.TrimPrefix(p, bdir+"/"))
			if prefix == "" || strings.HasPrefix(rel, prefix) {
				keys = append(keys, rel)
			}
			return nil
		})
		sort.Strings(keys)
		return keys
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		t0 := time.Now()
		sw := &statusWriter{ResponseWriter: w}
		code := 0
		defer func() {
			if sw.status != 0 {
				code = sw.status
			}
			fmt.Printf("[req] %s %s %d %d %v %s\n", r.Method, r.URL.RequestURI(), code, sw.bytes, time.Since(t0).Round(time.Microsecond), t0.Format("15:04:05.000"))
		}()
		if delay > 0 {
			time.Sleep(delay)
		}
		if failAll || (errPct > 0 && rng.Intn(100) < errPct) {
			writeError(sw, 500, "InternalError", "injected failure")
			return
		}
		path := strings.TrimPrefix(r.URL.Path, "/")
		segs := strings.SplitN(path, "/", 2)
		bucket := segs[0]
		key := ""
		if len(segs) == 2 {
			key = segs[1]
		}
		if bucket == "" {
			writeError(sw, 400, "InvalidRequest", "invalid path")
			return
		}
		sw.Header().Set("x-amz-request-id", "fake")
		sw.Header().Set("x-amz-id-2", "fake")

		switch {
		case r.Method == "PUT" && key == "":
			os.MkdirAll(filepath.Join(root, bucket), 0o755)
			sw.WriteHeader(200)

		case r.Method == "HEAD" && key == "":
			if fi, err := os.Stat(filepath.Join(root, bucket)); err == nil && fi.IsDir() {
				sw.WriteHeader(200)
				return
			}
			writeError(sw, 404, "NoSuchBucket", "no such bucket")

		case r.Method == "PUT":
			full := objPath(root, bucket, key)
			if r.Header.Get("If-None-Match") == "*" {
				if _, err := os.Stat(full); err == nil {
					writeError(sw, 412, "PreconditionFailed", "object exists")
					return
				}
			}
			if im := r.Header.Get("If-Match"); im != "" {
				cur := readETag(root, bucket, key)
				if _, err := os.Stat(full); err != nil || (im != "*" && strings.Trim(im, `"`) != strings.Trim(cur, `"`)) {
					writeError(sw, 412, "PreconditionFailed", "etag mismatch")
					return
				}
			}
			if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
				writeError(sw, 500, "InternalError", err.Error())
				return
			}
			tmp, err := os.CreateTemp(filepath.Dir(full), ".put-")
			if err != nil {
				writeError(sw, 500, "InternalError", err.Error())
				return
			}
			h := md5.New()
			var n int64
			if isChunked(r) {
				n, err = copyChunked(r.Body, io.MultiWriter(tmp, h))
			} else {
				n, err = io.Copy(io.MultiWriter(tmp, h), r.Body)
			}
			if cerr := tmp.Close(); err == nil {
				err = cerr
			}
			if err != nil {
				os.Remove(tmp.Name())
				writeError(sw, 500, "InternalError", err.Error())
				return
			}
			if err := os.Rename(tmp.Name(), full); err != nil {
				os.Remove(tmp.Name())
				writeError(sw, 500, "InternalError", err.Error())
				return
			}
			etag := `"` + hex.EncodeToString(h.Sum(nil)) + `"`
			writeETag(root, bucket, key, etag)
			sw.Header().Set("ETag", etag)
			sw.Header().Set("Content-Length", "0")
			sw.WriteHeader(200)
			_ = n

		case (r.Method == "GET" || r.Method == "HEAD") && key == "":
			if r.URL.Query().Get("list-type") == "2" {
				prefix := r.URL.Query().Get("prefix")
				maxKeys := 1000
				if v, err := strconv.Atoi(r.URL.Query().Get("max-keys")); err == nil && v > 0 {
					maxKeys = v
				}
				start := r.URL.Query().Get("start-after")
				if tok := r.URL.Query().Get("continuation-token"); tok != "" {
					start = tok
				}
				keys := listKeys(bucket, prefix)
				var page []string
				for _, k := range keys {
					if start != "" && k <= start {
						continue
					}
					page = append(page, k)
				}
				truncated := false
				next := ""
				if len(page) > maxKeys {
					page = page[:maxKeys]
					truncated = true
					next = page[len(page)-1]
				}
				var b strings.Builder
				b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>`)
				b.WriteString(bucket)
				b.WriteString(`</Name><Prefix>`)
				b.WriteString(prefix)
				b.WriteString(`</Prefix><KeyCount>` + fmt.Sprint(len(page)) + `</KeyCount><MaxKeys>` + fmt.Sprint(maxKeys) + `</MaxKeys><IsTruncated>` + fmt.Sprint(truncated) + `</IsTruncated>`)
				if next != "" {
					b.WriteString(`<NextContinuationToken>` + next + `</NextContinuationToken>`)
				}
				for _, k := range page {
					fi, err := os.Stat(objPath(root, bucket, k))
					if err != nil {
						continue
					}
					etag := readETag(root, bucket, k)
					if etag == "" {
						etag = `"fake"`
					}
					b.WriteString(`<Contents><Key>` + k + `</Key><LastModified>`)
					b.WriteString(fi.ModTime().UTC().Format("2006-01-02T15:04:05.000Z"))
					b.WriteString(`</LastModified><ETag>` + etag + `</ETag><Size>` + fmt.Sprint(fi.Size()) + `</Size></Contents>`)
				}
				b.WriteString(`</ListBucketResult>`)
				sw.Header().Set("Content-Type", "application/xml")
				io.WriteString(sw, b.String())
				return
			}
			writeError(sw, 404, "NoSuchKey", "not found")

		case (r.Method == "GET" || r.Method == "HEAD") && key != "":
			full := objPath(root, bucket, key)
			info, err := os.Stat(full)
			if err != nil {
				writeError(sw, 404, "NoSuchKey", "not found")
				return
			}
			size := info.Size()
			etag := readETag(root, bucket, key)
			if etag == "" {
				etag = `"fake"`
			}
			sw.Header().Set("ETag", etag)
			sw.Header().Set("Last-Modified", info.ModTime().UTC().Format(http.TimeFormat))
			sw.Header().Set("Accept-Ranges", "bytes")
			start, length := int64(0), size
			if rh := r.Header.Get("Range"); rh != "" && strings.HasPrefix(rh, "bytes=") {
				spec := strings.TrimPrefix(rh, "bytes=")
				parts := strings.SplitN(spec, "-", 2)
				if len(parts) == 2 {
					if parts[0] == "" {
						// suffix range: last N bytes
						if n, err := strconv.ParseInt(parts[1], 10, 64); err == nil && n > 0 {
							if n > size {
								n = size
							}
							start, length = size-n, n
						}
					} else {
						start, _ = strconv.ParseInt(parts[0], 10, 64)
						if parts[1] != "" {
							end, _ := strconv.ParseInt(parts[1], 10, 64)
							if end >= start {
								length = end - start + 1
							}
						} else {
							length = size - start
						}
					}
				}
				if start < 0 || start >= size || length < 0 || start+length > size {
					writeError(sw, 416, "InvalidRange", "range not satisfiable")
					return
				}
				sw.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, size))
				sw.Header().Set("Content-Length", fmt.Sprint(length))
				if r.Method == "HEAD" {
					sw.WriteHeader(206)
					return
				}
				f, err := os.Open(full)
				if err != nil {
					writeError(sw, 500, "InternalError", err.Error())
					return
				}
				defer f.Close()
				f.Seek(start, io.SeekStart)
				sw.WriteHeader(206)
				io.CopyN(sw, f, length)
				return
			}
			sw.Header().Set("Content-Length", fmt.Sprint(size))
			if r.Method == "HEAD" {
				sw.WriteHeader(200)
				return
			}
			f, err := os.Open(full)
			if err != nil {
				writeError(sw, 500, "InternalError", err.Error())
				return
			}
			defer f.Close()
			sw.WriteHeader(200)
			io.Copy(sw, f)

		case r.Method == "DELETE" && key != "":
			os.Remove(objPath(root, bucket, key))
			removeETag(root, bucket, key)
			sw.WriteHeader(204)

		case r.Method == "DELETE" && key == "":
			sw.WriteHeader(204)

		case r.Method == "POST" && key == "" && r.URL.Query().Has("delete"):
			var req struct {
				Objects []struct {
					Key string `xml:"Key"`
				} `xml:"Object"`
			}
			body, _ := io.ReadAll(io.LimitReader(r.Body, 16<<20))
			xml.Unmarshal(body, &req)
			for _, o := range req.Objects {
				os.Remove(objPath(root, bucket, o.Key))
				removeETag(root, bucket, o.Key)
			}
			sw.Header().Set("Content-Type", "application/xml")
			io.WriteString(sw, `<?xml version="1.0" encoding="UTF-8"?><DeleteResult></DeleteResult>`)

		default:
			writeError(sw, 405, "MethodNotAllowed", "unsupported")
		}
	})
	fmt.Printf("fake-s3 listening on %s, root=%s delay=%v\n", addr, root, delay)
	if err := http.ListenAndServe(addr, nil); err != nil {
		panic(err)
	}
}
