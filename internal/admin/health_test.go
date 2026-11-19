package admin

import (
	"net/http"
	"strings"
	"testing"
)

// /_api/v1/health used to answer a constant "ok": it never consulted repository
// readiness, so an instance whose stores were all unreachable still reported
// healthy. Behind a load balancer or a Kubernetes readiness probe that routes
// traffic to a process that cannot serve a single request, which is the failure
// mode that looks like a successful deploy.
//
// The tests below drive it through Server.SetRepoReady, which is exactly what
// the startup self-check and hot recovery already call.

func TestHealthReportsNotReadyWhenEveryRepoFailed(t *testing.T) {
	svc, srv, dir := testService(t)
	h := svc.Handler()

	// Two repositories, both configured; then both fail their self-check.
	passwd := dir + "/passwd"
	for _, name := range []string{"h1", "h2"} {
		rec, out := call(t, h, http.MethodPost, "/_api/v1/repos?create=1", "tok", repoBody(name, dir, passwd))
		if rec.Code != http.StatusCreated {
			t.Fatalf("create %s: status=%d body=%v", name, rec.Code, out)
		}
	}
	srv.SetRepoReady("h1", false, "store_unreachable")
	srv.SetRepoReady("h2", false, "store_unreachable")

	rec, out := call(t, h, http.MethodGet, "/_api/v1/health", "tok", nil)
	if rec.Code == http.StatusOK && out["status"] == "ok" {
		t.Fatalf("health reported ok while every repo is not ready: %v", out)
	}
}

func TestHealthReportsReadyWhenReposAreReady(t *testing.T) {
	svc, srv, dir := testService(t)
	h := svc.Handler()
	passwd := dir + "/passwd"
	rec, out := call(t, h, http.MethodPost, "/_api/v1/repos?create=1", "tok", repoBody("hr", dir, passwd))
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: status=%d body=%v", rec.Code, out)
	}
	srv.SetRepoReady("hr", true, "")

	rec, out = call(t, h, http.MethodGet, "/_api/v1/health", "tok", nil)
	if rec.Code != http.StatusOK || out["status"] != "ok" {
		t.Fatalf("healthy server: status=%d body=%v", rec.Code, out)
	}
}

// TestHealthReportsNotReadyForPartialFailure pins the choice made here: one
// unready repository is enough to fail the probe. Serving traffic with a
// partially broken repository means some tenants get errors while the
// orchestrator sees a healthy instance and keeps sending them.
func TestHealthReportsNotReadyForPartialFailure(t *testing.T) {
	svc, srv, dir := testService(t)
	h := svc.Handler()
	passwd := dir + "/passwd"
	for _, name := range []string{"pa", "pb"} {
		rec, out := call(t, h, http.MethodPost, "/_api/v1/repos?create=1", "tok", repoBody(name, dir, passwd))
		if rec.Code != http.StatusCreated {
			t.Fatalf("create %s: status=%d body=%v", name, rec.Code, out)
		}
	}
	srv.SetRepoReady("pa", true, "")
	srv.SetRepoReady("pb", false, "format_too_new")

	rec, out := call(t, h, http.MethodGet, "/_api/v1/health", "tok", nil)
	if rec.Code == http.StatusOK && out["status"] == "ok" {
		t.Fatalf("health reported ok with one unready repo: %v", out)
	}
}

// TestHealthCarriesFailingRepoDetail guards the operator experience: a failing
// probe has to say which repository and why, or someone has to scrape
// /_metrics to find out.
func TestHealthCarriesFailingRepoDetail(t *testing.T) {
	svc, srv, dir := testService(t)
	h := svc.Handler()
	passwd := dir + "/passwd"
	rec, out := call(t, h, http.MethodPost, "/_api/v1/repos?create=1", "tok", repoBody("det", dir, passwd))
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: status=%d body=%v", rec.Code, out)
	}
	srv.SetRepoReady("det", false, "auth_denied")

	rec, out = call(t, h, http.MethodGet, "/_api/v1/health", "tok", nil)
	if rec.Code == http.StatusOK {
		t.Fatalf("expected a failing status code, got 200: %v", out)
	}
	body := string(rec.Body.Bytes())
	if !strings.Contains(body, "det") || !strings.Contains(body, "auth_denied") {
		t.Fatalf("health body does not name the failing repo or reason: %s", body)
	}
}

// TestHealthReportsNoReposAsOK pins the empty-config case: a server with zero
// repositories is a valid (if useless) deployment and must not fail its probe,
// otherwise a fresh install can never become ready.
func TestHealthReportsNoReposAsOK(t *testing.T) {
	svc, _, _ := testService(t)
	rec, out := call(t, svc.Handler(), http.MethodGet, "/_api/v1/health", "tok", nil)
	if rec.Code != http.StatusOK || out["status"] != "ok" {
		t.Fatalf("empty config: status=%d body=%v", rec.Code, out)
	}
}
