package admin

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"svngod/internal/config"
	"svngod/internal/server"
)

func testService(t *testing.T) (*Service, *server.Server, string) {
	t.Helper()
	dir := t.TempDir()
	passwd := filepath.Join(dir, "passwd")
	if err := os.WriteFile(passwd, []byte("alice:alicepw\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "svngod.json")
	cfgJSON := map[string]any{"admin-token": "tok", "repos": []any{}}
	b, _ := json.Marshal(cfgJSON)
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	repos, cfgs, err := cfg.BuildRepos(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(server.Config{BasePath: "/svn", Repos: repos})
	svc := New(path, cfg, cfgs, srv, "tok", nil, log.New(io.Discard, "", 0))
	return svc, srv, dir
}

func call(t *testing.T, h http.Handler, method, path, token string, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, path, rd)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec, out
}

func repoBody(name, dir, passwd string) map[string]any {
	return map[string]any{
		"name":    name,
		"storage": map[string]any{"type": "file", "root": filepath.Join(dir, "repos")},
		"auth":    []any{map[string]any{"type": "local", "passwd": passwd}},
	}
}

func TestAdminAuthRequired(t *testing.T) {
	svc, _, _ := testService(t)
	h := svc.Handler()
	rec, _ := call(t, h, http.MethodGet, "/_api/v1/health", "", nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d, want 401", rec.Code)
	}
	rec, out := call(t, h, http.MethodGet, "/_api/v1/health", "tok", nil)
	if rec.Code != http.StatusOK || out["status"] != "ok" {
		t.Fatalf("status=%d body=%v", rec.Code, out)
	}
}

func TestAdminCreateRepoSegments(t *testing.T) {
	svc, _, dir := testService(t)
	h := svc.Handler()
	passwd := filepath.Join(dir, "passwd")
	body := repoBody("segadm", dir, passwd)
	body["segments"] = true
	body["segment-target"] = 64
	rec, out := call(t, h, http.MethodPost, "/_api/v1/repos?create=1", "tok", body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%v", rec.Code, out)
	}
	rc := svc.cfgs["segadm"]
	if rc == nil {
		t.Fatal("descriptor missing")
	}
	rp, err := rc.OpenRepo("segadm")
	if err != nil {
		t.Fatal(err)
	}
	if !rp.SegmentsEnabled() || rp.SegmentTarget() != 64 {
		t.Fatalf("segments=%v target=%d", rp.SegmentsEnabled(), rp.SegmentTarget())
	}
}

func TestAdminCreateRepoAndPersist(t *testing.T) {
	svc, srv, dir := testService(t)
	h := svc.Handler()
	passwd := filepath.Join(dir, "passwd")
	rec, out := call(t, h, http.MethodPost, "/_api/v1/repos?create=1", "tok", repoBody("assets", dir, passwd))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%v", rec.Code, out)
	}
	if _, err := os.Stat(filepath.Join(dir, "repos", "repos", "assets", "format")); err != nil {
		t.Fatalf("repository not created: %v", err)
	}
	disk, err := config.Load(filepath.Join(dir, "svngod.json"))
	if err != nil || len(disk.Repos) != 1 || disk.Repos[0].Name != "assets" {
		t.Fatalf("config not persisted: %+v err=%v", disk, err)
	}
	names := srv.RepoNames()
	if len(names) != 1 || names[0] != "assets" {
		t.Fatalf("server repos=%v", names)
	}
	if rc := svc.cfgs["assets"]; rc == nil {
		t.Fatal("descriptor missing")
	} else if _, err := rc.OpenRepo("assets"); err != nil {
		t.Fatalf("open created repo: %v", err)
	}
}

func TestAdminDryRunDoesNotCreate(t *testing.T) {
	svc, _, dir := testService(t)
	h := svc.Handler()
	rec, _ := call(t, h, http.MethodPost, "/_api/v1/repos?create=1&dry_run=1", "tok", repoBody("dry", dir, filepath.Join(dir, "passwd")))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	if _, err := os.Stat(filepath.Join(dir, "repos", "repos", "dry")); !os.IsNotExist(err) {
		t.Fatalf("dry run created data: %v", err)
	}
}

func TestAdminDeleteKeepsData(t *testing.T) {
	svc, srv, dir := testService(t)
	h := svc.Handler()
	passwd := filepath.Join(dir, "passwd")
	if rec, out := call(t, h, http.MethodPost, "/_api/v1/repos?create=1", "tok", repoBody("gone", dir, passwd)); rec.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%v", rec.Code, out)
	}
	if rec, out := call(t, h, http.MethodDelete, "/_api/v1/repos/gone", "tok", nil); rec.Code != http.StatusOK {
		t.Fatalf("delete status=%d body=%v", rec.Code, out)
	}
	if len(srv.RepoNames()) != 0 {
		t.Fatalf("repo still served: %v", srv.RepoNames())
	}
	if _, err := os.Stat(filepath.Join(dir, "repos", "repos", "gone", "format")); err != nil {
		t.Fatalf("delete removed data: %v", err)
	}
}

func TestAdminReloadRejectsBrokenFile(t *testing.T) {
	svc, srv, dir := testService(t)
	h := svc.Handler()
	if rec, _ := call(t, h, http.MethodPost, "/_api/v1/repos?create=1", "tok", repoBody("keep", dir, filepath.Join(dir, "passwd"))); rec.Code != http.StatusCreated {
		t.Fatalf("create status=%d", rec.Code)
	}
	path := filepath.Join(dir, "svngod.json")
	good, _ := os.ReadFile(path)
	if err := os.WriteFile(path, []byte("{bad"), 0o600); err != nil {
		t.Fatal(err)
	}
	rec, _ := call(t, h, http.MethodPost, "/_api/v1/reload", "tok", nil)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status=%d, want 422", rec.Code)
	}
	if names := srv.RepoNames(); len(names) != 1 || names[0] != "keep" {
		t.Fatalf("old config lost: %v", names)
	}
	_ = os.WriteFile(path, good, 0o600)
}

func TestAdminConfigRedactsSecrets(t *testing.T) {
	svc, _, _ := testService(t)
	h := svc.Handler()
	rec, _ := call(t, h, http.MethodGet, "/_api/v1/config", "tok", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	body := rec.Body.Bytes()
	if bytes.Contains(body, []byte(`"admin-token": "tok"`)) || bytes.Contains(body, []byte(`"admin-token":"tok"`)) {
		t.Fatalf("token leaked: %s", rec.Body.String())
	}
	if !bytes.Contains(body, []byte("***")) {
		t.Fatalf("secrets not redacted: %s", rec.Body.String())
	}
}
func TestAdminReplaceRepo(t *testing.T) {
	svc, srv, dir := testService(t)
	h := svc.Handler()
	passwd := filepath.Join(dir, "passwd")
	if rec, _ := call(t, h, http.MethodPost, "/_api/v1/repos?create=1", "tok", repoBody("swap", dir, passwd)); rec.Code != http.StatusCreated {
		t.Fatalf("create failed")
	}
	body := repoBody("swap", dir, passwd)
	body["authz"] = ""
	rec, out := call(t, h, http.MethodPut, "/_api/v1/repos/swap", "tok", body)
	if rec.Code != http.StatusOK {
		t.Fatalf("replace status=%d body=%v", rec.Code, out)
	}
	if names := srv.RepoNames(); len(names) != 1 || names[0] != "swap" {
		t.Fatalf("repos=%v", names)
	}
	if _, err := svc.cfgs["swap"].OpenRepo("swap"); err != nil {
		t.Fatalf("open after replace: %v", err)
	}
}
