// Command fake-s3 is a minimal S3-compatible object store used as a test
// backend for svngod's S3Store. It implements the object-plane subset minio-go
// uses (PUT/GET/HEAD/DELETE of a single object + ListObjectsV2 with prefix),
// stores data on local disk, and skips SigV4 signature validation (test-only).
package main

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
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

func main() {
	root := os.Getenv("FAKE_S3_ROOT")
	if root == "" {
		root = "/home/user/svngod/s3data"
	}
	addr := os.Getenv("FAKE_S3_ADDR")
	if addr == "" {
		addr = ":9000"
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		panic(err)
	}
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("[req] %s %s\n", r.Method, r.URL.String())
		path := strings.TrimPrefix(r.URL.Path, "/")
		segs := strings.SplitN(path, "/", 2)
		if len(segs) < 1 || segs[0] == "" {
			http.Error(w, "invalid path", 400)
			return
		}
		bucket := segs[0]
		key := ""
		if len(segs) == 2 {
			key = segs[1]
		}
		common := func() {
			w.Header().Set("x-amz-request-id", "fake")
			w.Header().Set("x-amz-id-2", "fake")
		}
		switch {
		case r.Method == "PUT" && key == "":
			// bucket create (MakeBucket): tolerate as a no-op / mkdir
			os.MkdirAll(filepath.Join(root, bucket), 0o755)
			common()
			w.WriteHeader(200)
		case r.Method == "PUT":
			// single object upload
			full := filepath.Join(root, bucket, filepath.FromSlash(key))
			if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			h := sha1.New()
			f, err := os.Create(full)
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			if isChunked(r) {
				if _, err := copyChunked(r.Body, io.MultiWriter(f, h)); err != nil {
					f.Close()
					http.Error(w, err.Error(), 500)
					return
				}
			} else {
				if _, err := io.Copy(io.MultiWriter(f, h), r.Body); err != nil {
					f.Close()
					http.Error(w, err.Error(), 500)
					return
				}
			}
			f.Close()
			common()
			w.Header().Set("ETag", `"`+hex.EncodeToString(h.Sum(nil))+`"`)
			w.WriteHeader(200)
		case (r.Method == "GET" || r.Method == "HEAD") && key == "":
			// bucket listing (ListObjectsV2 via ?list-type=2)
			if r.URL.Query().Get("list-type") == "2" {
				prefix := r.URL.Query().Get("prefix")
				bdir := filepath.Join(root, bucket)
				var b strings.Builder
				b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>`)
				b.WriteString(bucket)
				b.WriteString(`</Name><IsTruncated>false</IsTruncated>`)
				filepath.Walk(bdir, func(p string, info os.FileInfo, err error) error {
					if err != nil || info.IsDir() {
						return nil
					}
					rel := filepath.ToSlash(strings.TrimPrefix(p, bdir+"/"))
					if prefix != "" && !strings.HasPrefix(rel, prefix) {
						return nil
					}
					b.WriteString(`<Contents><Key>` + rel + `</Key><LastModified>`)
					b.WriteString(info.ModTime().UTC().Format(time.RFC3339))
					b.WriteString(`</LastModified><ETag>"x"</ETag><Size>` + fmt.Sprint(info.Size()) + `</Size></Contents>`)
					return nil
				})
				b.WriteString(`</ListBucketResult>`)
				common()
				w.Header().Set("Content-Type", "application/xml")
				io.WriteString(w, b.String())
				return
			}
			http.Error(w, "not found", 404)
		case (r.Method == "GET" || r.Method == "HEAD") && key != "":
			full := filepath.Join(root, bucket, filepath.FromSlash(key))
			info, err := os.Stat(full)
			if err != nil {
				http.Error(w, "not found", 404)
				return
			}
			common()
			w.Header().Set("Content-Length", fmt.Sprint(info.Size()))
			w.Header().Set("ETag", `"fake"`)
			w.Header().Set("Last-Modified", info.ModTime().UTC().Format(http.TimeFormat))
			if r.Method == "HEAD" {
				w.WriteHeader(200)
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
				return
			}
			f, _ := os.Open(full)
			defer f.Close()
			w.WriteHeader(200)
			io.Copy(w, f)
		case r.Method == "DELETE":
			full := filepath.Join(root, bucket, filepath.FromSlash(key))
			if key == "" {
				// delete bucket: not supported, treat as ok
				w.WriteHeader(204)
				return
			}
			os.Remove(full)
			common()
			w.WriteHeader(204)
		default:
			http.Error(w, "method not allowed", 405)
		}
	})
	fmt.Printf("fake-s3 listening on %s, root=%s\n", addr, root)
	if err := http.ListenAndServe(addr, nil); err != nil {
		panic(err)
	}
}
