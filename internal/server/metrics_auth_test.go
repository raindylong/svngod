package server

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"svngod/internal/auth"
	"svngod/internal/fs"
)

// /_metrics labels every series with the repository name, so an unauthenticated
// endpoint lets anyone who can reach the port enumerate tenants and read their
// failure state. It used to be the first branch in the router, ahead of
// authentication.
//
// Scraping still has to work, so the endpoint accepts either a valid repository
// credential (Basic) or the configured admin token (Bearer). A deployment with no
// authentication configured anywhere keeps the old open behaviour: refusing then
// would break every existing single-node setup with no way to authenticate.

func basicHeader(user, pass string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
}

// metricsServer builds a server with auth required and a repo that reports ready.
func metricsServer(t *testing.T) *Server {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "passwd")
	if err := os.WriteFile(p, []byte("alice:"+shaPass("secret")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return New(Config{
		BasePath:     "/svn",
		Authenticate: auth.NewChain(auth.NewLocalPasswordFile(p)),
		OpenRepo:     func(name string) (*fs.Repo, error) { return nil, fs.ErrRepoNotFound },
		RepoReady:    map[string]bool{"demo": true},
	})
}

// TestMetricsRequiresAuth fails on pre-fix code: /_metrics was served before
// authentication, so an anonymous GET returned the full exposition body.
func TestMetricsRequiresAuth(t *testing.T) {
	s := metricsServer(t)

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/_metrics", nil))

	if rec.Code == http.StatusOK {
		t.Fatalf("anonymous GET /_metrics returned 200 with %d bytes; it is behind auth now",
			rec.Body.Len())
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous /_metrics returned %d, want 401", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "svngod_repo_ready") {
		t.Fatal("unauthenticated response leaked metric series")
	}
}

// TestMetricsAllowsAuthenticatedScrape keeps Prometheus working: one valid
// credential must be enough to scrape.
func TestMetricsAllowsAuthenticatedScrape(t *testing.T) {
	s := metricsServer(t)

	req := httptest.NewRequest("GET", "/_metrics", nil)
	req.Header.Set("Authorization", basicHeader("alice", "secret"))
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("authenticated scrape returned %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "svngod_") {
		t.Fatal("authenticated scrape returned no metrics")
	}
}

// TestMetricsRejectsBadCredentials is the negative control for the test above:
// the 200 must come from the credential, not from the endpoint being open.
func TestMetricsRejectsBadCredentials(t *testing.T) {
	s := metricsServer(t)

	req := httptest.NewRequest("GET", "/_metrics", nil)
	req.Header.Set("Authorization", basicHeader("alice", "wrong"))
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("bad credentials returned %d, want 401", rec.Code)
	}
}

// TestMetricsOpenWhenNoAuthConfigured preserves the single-node setup: with no
// auth chain and no admin token there is nothing to authenticate against, so the
// endpoint stays as it was.
func TestMetricsOpenWhenNoAuthConfigured(t *testing.T) {
	s := New(Config{
		BasePath:  "/svn",
		OpenRepo:  func(name string) (*fs.Repo, error) { return nil, fs.ErrRepoNotFound },
		RepoReady: map[string]bool{"demo": true},
	})

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/_metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("no-auth deployment lost access to its own metrics: %d", rec.Code)
	}
}

// TestMetricsDisableStillWins confirms the off switch is not bypassed by the
// authentication path.
func TestMetricsDisableStillWins(t *testing.T) {
	s := metricsServer(t)
	s.cfg.DisableMetrics = true

	req := httptest.NewRequest("GET", "/_metrics", nil)
	req.Header.Set("Authorization", basicHeader("alice", "secret"))
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code == http.StatusOK {
		t.Fatal("-metrics=false was ignored")
	}
}
