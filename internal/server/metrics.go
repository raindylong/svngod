package server

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"svngod/internal/fs"
)

// metrics is the server's aggregate observability state. Labels are limited
// to HTTP method and status on purpose: repository names, paths and users
// would explode cardinality (and leak names on a scraped endpoint).
type metrics struct {
	mu     sync.Mutex
	reqs   map[string]uint64 // "METHOD STATUS"
	durSum float64
	count  uint64

	readBytes  atomic.Uint64
	writeBytes atomic.Uint64
	inflight   atomic.Int64

	commits       atomic.Uint64
	bundleUploads atomic.Uint64
	bundleReplays atomic.Uint64

	limit int
}

func newMetrics(limit int) *metrics {
	return &metrics{reqs: map[string]uint64{}, limit: limit}
}

func (m *metrics) observe(method string, status int, d time.Duration) {
	if status == 0 {
		status = http.StatusOK
	}
	key := method + " " + fmt.Sprint(status)
	m.mu.Lock()
	m.reqs[key]++
	m.durSum += d.Seconds()
	m.count++
	m.mu.Unlock()
}

func (m *metrics) addRead(n int64)  { m.readBytes.Add(uint64(n)) }
func (m *metrics) addWrite(n int64) { m.writeBytes.Add(uint64(n)) }

func (m *metrics) acquire() { m.inflight.Add(1) }
func (m *metrics) release() { m.inflight.Add(-1) }

// serveMetrics writes the Prometheus text exposition.
func (s *Server) serveMetrics(w http.ResponseWriter) {
	if s.cfg.DisableMetrics {
		http.NotFound(w, nil)
		return
	}
	m := s.m
	m.mu.Lock()
	keys := make([]string, 0, len(m.reqs))
	for k := range m.reqs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	reqs := make(map[string]uint64, len(m.reqs))
	for k, v := range m.reqs {
		reqs[k] = v
	}
	durSum, count := m.durSum, m.count
	m.mu.Unlock()

	var b strings.Builder
	b.WriteString("# HELP svngod_http_requests_total HTTP requests by method and status.\n")
	b.WriteString("# TYPE svngod_http_requests_total counter\n")
	for _, k := range keys {
		method, status, ok := strings.Cut(k, " ")
		if !ok {
			continue
		}
		fmt.Fprintf(&b, "svngod_http_requests_total{method=%q,status=%q} %d\n",
			escapeLabel(method), escapeLabel(status), reqs[k])
	}
	b.WriteString("# HELP svngod_http_request_duration_seconds_sum Total time spent in HTTP handlers.\n")
	b.WriteString("# TYPE svngod_http_request_duration_seconds_sum counter\n")
	fmt.Fprintf(&b, "svngod_http_request_duration_seconds_sum %.6f\n", durSum)
	b.WriteString("# TYPE svngod_http_request_duration_seconds_count counter\n")
	fmt.Fprintf(&b, "svngod_http_request_duration_seconds_count %d\n", count)

	b.WriteString("# HELP svngod_http_request_bytes_total Bytes read from request bodies.\n")
	b.WriteString("# TYPE svngod_http_request_bytes_total counter\n")
	fmt.Fprintf(&b, "svngod_http_request_bytes_total %d\n", m.readBytes.Load())
	b.WriteString("# HELP svngod_http_response_bytes_total Bytes written in responses.\n")
	b.WriteString("# TYPE svngod_http_response_bytes_total counter\n")
	fmt.Fprintf(&b, "svngod_http_response_bytes_total %d\n", m.writeBytes.Load())

	b.WriteString("# HELP svngod_upload_inflight Upload bodies currently in flight.\n")
	b.WriteString("# TYPE svngod_upload_inflight gauge\n")
	fmt.Fprintf(&b, "svngod_upload_inflight %d\n", m.inflight.Load())
	b.WriteString("# HELP svngod_upload_limit Configured global upload concurrency (0 = unlimited).\n")
	b.WriteString("# TYPE svngod_upload_limit gauge\n")
	fmt.Fprintf(&b, "svngod_upload_limit %d\n", m.limit)

	b.WriteString("# HELP svngod_commits_total Revisions committed (DAV and bundle).\n")
	b.WriteString("# TYPE svngod_commits_total counter\n")
	fmt.Fprintf(&b, "svngod_commits_total %d\n", m.commits.Load())
	b.WriteString("# HELP svngod_bundle_uploads_total Bundle upload attempts (excluding replays).\n")
	b.WriteString("# TYPE svngod_bundle_uploads_total counter\n")
	fmt.Fprintf(&b, "svngod_bundle_uploads_total %d\n", m.bundleUploads.Load())
	b.WriteString("# HELP svngod_bundle_replays_total Bundle retries answered from an existing record.\n")
	b.WriteString("# TYPE svngod_bundle_replays_total counter\n")
	fmt.Fprintf(&b, "svngod_bundle_replays_total %d\n", m.bundleReplays.Load())

	// Mirror replication health/lag, for repositories backed by a mirror.
	s.repoMapMu.Lock()
	repos := make(map[string]*fs.Repo, len(s.repoCache))
	for k, v := range s.repoCache {
		repos[k] = v
	}
	s.repoMapMu.Unlock()
	names := make([]string, 0, len(repos))
	for k := range repos {
		names = append(names, k)
	}
	sort.Strings(names)
	wroteMirror := false
	for _, name := range names {
		statuses := repos[name].ReplicaStatus()
		if len(statuses) == 0 {
			continue
		}
		if !wroteMirror {
			b.WriteString("# HELP svngod_mirror_healthy Whether a mirror replica is currently reachable.\n")
			b.WriteString("# TYPE svngod_mirror_healthy gauge\n")
			b.WriteString("# HELP svngod_mirror_primary Whether this replica is the elected writer.\n")
			b.WriteString("# TYPE svngod_mirror_primary gauge\n")
			b.WriteString("# HELP svngod_mirror_last_write_seconds Seconds since the last successful write (-1 unknown).\n")
			b.WriteString("# TYPE svngod_mirror_last_write_seconds gauge\n")
			wroteMirror = true
		}
		for _, st := range statuses {
			idx := fmt.Sprint(st.Index)
			healthy := 0
			if st.Healthy {
				healthy = 1
			}
			primary := 0
			if st.Primary {
				primary = 1
			}
			age := -1.0
			if !st.LastSuccess.IsZero() {
				age = time.Since(st.LastSuccess).Seconds()
			}
			fmt.Fprintf(&b, "svngod_mirror_healthy{repo=%q,replica=%q} %d\n", escapeLabel(name), idx, healthy)
			fmt.Fprintf(&b, "svngod_mirror_primary{repo=%q,replica=%q} %d\n", escapeLabel(name), idx, primary)
			fmt.Fprintf(&b, "svngod_mirror_last_write_seconds{repo=%q,replica=%q} %.3f\n", escapeLabel(name), idx, age)
		}
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	io.WriteString(w, b.String())
}

func escapeLabel(s string) string {
	return strings.NewReplacer("\\", "\\\\", "\n", "\\n", "\"", "\\\"").Replace(s)
}

// countingReadCloser counts the bytes a handler reads from the request body.
type countingReadCloser struct {
	r io.ReadCloser
	n int64
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func (c *countingReadCloser) Close() error { return c.r.Close() }
