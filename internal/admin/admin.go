// Package admin implements svngod's management API: a separate listener where
// an operator can add, change or remove repository configuration and trigger a
// hot reload of the running server.
package admin

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"svngod/internal/config"
	"svngod/internal/fs"
	"svngod/internal/server"
)

// Service owns the applied configuration and serialises mutations.
type Service struct {
	mu     sync.Mutex
	path   string
	token  string
	allow  []*net.IPNet
	srv    *server.Server
	cfg    *config.Config
	cfgs   map[string]*server.RepoConfig
	gen    uint64
	status Status
	logger *log.Logger
}

// Status is the body of GET /_api/v1/status.
type Status struct {
	Generation uint64    `json:"generation"`
	Repos      []string  `json:"repos"`
	LastReload time.Time `json:"last_reload,omitempty"`
	Result     string    `json:"result,omitempty"`
	Error      string    `json:"error,omitempty"`
	Ignored    []string  `json:"ignored_fields,omitempty"`
}

// New builds the service. path is the JSON config file (mutations require
// one); cfgs are the descriptors the server was started with.
func New(path string, cfg *config.Config, cfgs map[string]*server.RepoConfig, srv *server.Server,
	token string, allow []*net.IPNet, logger *log.Logger) *Service {
	names := make([]string, 0, len(cfg.Repos))
	for i := range cfg.Repos {
		names = append(names, cfg.Repos[i].Name)
	}
	return &Service{
		path: path, token: token, allow: allow, srv: srv, cfg: cfg, cfgs: cfgs,
		gen:    1,
		status: Status{Generation: 1, Repos: names, Result: "startup"},
		logger: logger,
	}
}

// Handler returns the management API handler (already authenticated).
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/_api/v1/health", s.auth(s.health))
	mux.HandleFunc("/_api/v1/status", s.auth(s.getStatus))
	mux.HandleFunc("/_api/v1/config", s.auth(s.getConfig))
	mux.HandleFunc("/_api/v1/repos", s.auth(s.repos))
	mux.HandleFunc("/_api/v1/repos/", s.auth(s.repoByName))
	mux.HandleFunc("/_api/v1/reload", s.auth(s.reload))
	return mux
}

func (s *Service) auth(fn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.allowIP(r) {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "source address not allowed"})
			return
		}
		tok := ""
		if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
			tok = strings.TrimPrefix(h, "Bearer ")
		} else {
			tok = r.Header.Get("X-Admin-Token")
		}
		if tok == "" || subtle.ConstantTimeCompare([]byte(tok), []byte(s.token)) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="svngod-admin"`)
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "admin token required"})
			return
		}
		fn(w, r)
	}
}

func (s *Service) allowIP(r *http.Request) bool {
	if len(s.allow) == 0 {
		return true
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	for _, n := range s.allow {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

func (s *Service) audit(r *http.Request, action, repo, result string) {
	if s.logger == nil {
		return
	}
	s.logger.Printf("admin action=%s repo=%q result=%s remote=%s", action, repo, result, r.RemoteAddr)
}

func (s *Service) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok", "generation": s.gen, "repos": s.srv.RepoNames(),
	})
}

func (s *Service) getStatus(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	st := s.status
	st.Generation = s.gen
	st.Repos = s.srv.RepoNames()
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, st)
}

func (s *Service) getConfig(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	red := s.cfg.Redacted()
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"generation": s.gen, "config": red, "config_file": s.path,
	})
}

type repoView struct {
	Name    string   `json:"name"`
	Storage string   `json:"storage"`
	Authz   string   `json:"authz,omitempty"`
	Auth    []string `json:"auth,omitempty"`
	// Ready/ReadyError mirror the startup and hot-recovery self-check, so an
	// operator can query repository health instead of scraping /_metrics.
	Ready      bool   `json:"ready"`
	ReadyError string `json:"ready_error,omitempty"`
}

func (s *Service) repos(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		readyMap, readyErr := map[string]bool{}, map[string]string{}
		live := map[string]bool{}
		if s.srv != nil {
			readyMap, readyErr = s.srv.RepoReadySnapshot()
			for _, n := range s.srv.RepoNames() {
				live[n] = true
			}
		}
		s.mu.Lock()
		out := make([]repoView, 0, len(s.cfg.Repos))
		for i := range s.cfg.Repos {
			rp := &s.cfg.Repos[i]
			types := make([]string, 0, len(rp.Auth))
			for _, a := range rp.Auth {
				types = append(types, a.Type)
			}
			ready, known := readyMap[rp.Name]
			if !known {
				// Repositories added through the API have no startup entry; they
				// are ready when the server actually serves them.
				ready = live[rp.Name]
			}
			out = append(out, repoView{Name: rp.Name, Storage: rp.Storage.Type, Authz: rp.Authz,
				Auth: types, Ready: ready, ReadyError: readyErr[rp.Name]})
		}
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{"generation": s.gen, "repos": out})
	case http.MethodPost:
		s.addRepo(w, r)
	default:
		w.Header().Set("Allow", "GET, POST")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

func (s *Service) addRepo(w http.ResponseWriter, r *http.Request) {
	var rp config.Repo
	if err := decodeJSON(r, &rp); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	create := queryBool(r, "create")
	dry := queryBool(r, "dry_run")

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.findRepo(rp.Name) != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "repository already configured"})
		return
	}
	if err := rp.Validate(); err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
		return
	}
	if dry {
		if !create {
			rc, err := rp.RepoConfig()
			if err != nil {
				writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
				return
			}
			if rc.Close != nil {
				rc.Close()
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"dry_run": true, "valid": true, "name": rp.Name, "would_create": create})
		return
	}
	if create {
		if err := rp.Provision(); err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, config.ErrRepoExists) {
				status = http.StatusConflict
			}
			writeJSON(w, status, map[string]string{"error": err.Error()})
			return
		}
	}
	rc, err := rp.RepoConfig()
	if err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
		return
	}
	if err := s.writeRepos(append(cloneRepos(s.cfg.Repos), rp)); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.cfg.Repos = append(s.cfg.Repos, rp)
	s.cfgs[rp.Name] = rc
	newList := s.repoConfigs()
	retired, err := s.srv.SetRepos(newList, map[string]bool{rp.Name: true})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.bumpLocked()
	retire(retired)
	s.audit(r, "add-repo", rp.Name, "ok")
	writeJSON(w, http.StatusCreated, map[string]any{"name": rp.Name, "generation": s.gen})
}

func (s *Service) repoByName(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/_api/v1/repos/")
	if name == "" || strings.Contains(name, "/") {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown repository"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.mu.Lock()
		rp := s.findRepo(name)
		var view any
		if rp != nil {
			view = rp
		}
		s.mu.Unlock()
		if rp == nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown repository"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"repo": view})
	case http.MethodPut:
		s.replaceRepo(w, r, name)
	case http.MethodDelete:
		s.deleteRepo(w, r, name)
	default:
		w.Header().Set("Allow", "GET, PUT, DELETE")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

func (s *Service) replaceRepo(w http.ResponseWriter, r *http.Request, name string) {
	var rp config.Repo
	if err := decodeJSON(r, &rp); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if rp.Name != name {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name in body does not match path"})
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.findRepo(name) == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown repository"})
		return
	}
	rc, err := rp.RepoConfig()
	if err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
		return
	}
	next := cloneRepos(s.cfg.Repos)
	for i := range next {
		if next[i].Name == name {
			next[i] = rp
		}
	}
	if err := s.writeRepos(next); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if old := s.cfgs[name]; old != nil && old.Close != nil {
		old.Close()
	}
	s.cfg.Repos = next
	s.cfgs[name] = rc
	retired, err := s.srv.SetRepos(s.repoConfigs(), map[string]bool{name: true})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.bumpLocked()
	retire(retired)
	s.audit(r, "replace-repo", name, "ok")
	writeJSON(w, http.StatusOK, map[string]any{"name": name, "generation": s.gen})
}

func (s *Service) deleteRepo(w http.ResponseWriter, r *http.Request, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.findRepo(name) == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown repository"})
		return
	}
	next := make([]config.Repo, 0, len(s.cfg.Repos)-1)
	for _, rp := range s.cfg.Repos {
		if rp.Name != name {
			next = append(next, rp)
		}
	}
	if err := s.writeRepos(next); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	delete(s.cfgs, name)
	s.cfg.Repos = next
	retired, err := s.srv.SetRepos(s.repoConfigs(), map[string]bool{name: true})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.bumpLocked()
	retire(retired)
	s.audit(r, "delete-repo", name, "ok")
	writeJSON(w, http.StatusOK, map[string]any{
		"name": name, "generation": s.gen,
		"note": "configuration removed; repository data was not deleted",
	})
}

func (s *Service) reload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	res, err := s.ReloadNow()
	if err != nil {
		writeJSON(w, res.code, map[string]string{"error": err.Error()})
		return
	}
	s.audit(r, "reload", "", "ok")
	writeJSON(w, http.StatusOK, map[string]any{
		"generation": s.gen, "repos": s.srv.RepoNames(),
		"changed": res.changed, "ignored_fields": res.ignored,
	})
}

// ReloadResult carries the outcome of a reload (also used by SIGHUP).
type ReloadResult struct {
	code    int
	changed []string
	ignored []string
}

// ReloadNow re-reads the config file and swaps the repository set. Global
// fields are reported but not applied: they require a restart.
func (s *Service) ReloadNow() (*ReloadResult, error) {
	if s.path == "" {
		return &ReloadResult{code: http.StatusConflict}, errors.New("no config file (flag mode cannot reload repositories)")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	disk, err := config.Load(s.path)
	if err != nil {
		s.status.Result = "reload-failed"
		s.status.Error = err.Error()
		s.status.LastReload = time.Now()
		return &ReloadResult{code: http.StatusUnprocessableEntity}, err
	}
	ignored := changedGlobals(s.cfg, disk)
	prevFp := s.cfg.Fingerprints()
	nextFp := disk.Fingerprints()
	nextCfg := *disk
	// Global fields stay as applied at startup; only repositories hot-reload.
	nextCfg.Addr, nextCfg.BasePath, nextCfg.Hooks = s.cfg.Addr, s.cfg.BasePath, s.cfg.Hooks
	nextCfg.TLSCert, nextCfg.TLSKey = s.cfg.TLSCert, s.cfg.TLSKey
	nextCfg.MaxUploads, nextCfg.MaxUserUploads = s.cfg.MaxUploads, s.cfg.MaxUserUploads
	nextCfg.DisableMetrics = s.cfg.DisableMetrics
	nextCfg.AdminAddr, nextCfg.AdminToken, nextCfg.AdminTokenFile, nextCfg.AdminAllowCIDR =
		s.cfg.AdminAddr, s.cfg.AdminToken, s.cfg.AdminTokenFile, s.cfg.AdminAllowCIDR

	newList, newCfgs, err := nextCfg.BuildRepos(s.cfgs, prevFp)
	if err != nil {
		s.status.Result = "reload-failed"
		s.status.Error = err.Error()
		s.status.LastReload = time.Now()
		return &ReloadResult{code: http.StatusUnprocessableEntity, ignored: ignored}, err
	}
	changed := map[string]bool{}
	for _, rp := range nextCfg.Repos {
		if prevFp[rp.Name] != nextFp[rp.Name] {
			changed[rp.Name] = true
		}
	}
	for name := range prevFp {
		if _, ok := nextFp[name]; !ok {
			changed[name] = true
		}
	}
	retired, err := s.srv.SetRepos(newList, changed)
	if err != nil {
		s.status.Result = "reload-failed"
		s.status.Error = err.Error()
		s.status.LastReload = time.Now()
		return &ReloadResult{code: http.StatusInternalServerError, ignored: ignored}, err
	}
	for name := range changed {
		if rc := s.cfgs[name]; rc != nil && rc.Close != nil {
			rc.Close()
		}
	}
	s.cfg = &nextCfg
	s.cfgs = newCfgs
	s.bumpLocked()
	retire(retired)
	s.status.Result = "reload-ok"
	s.status.Error = ""
	s.status.Ignored = ignored
	s.status.LastReload = time.Now()
	return &ReloadResult{code: http.StatusOK, changed: keysOf(changed), ignored: ignored}, nil
}

// --- helpers ---

func (s *Service) findRepo(name string) *config.Repo {
	for i := range s.cfg.Repos {
		if s.cfg.Repos[i].Name == name {
			return &s.cfg.Repos[i]
		}
	}
	return nil
}

func (s *Service) repoConfigs() []*server.RepoConfig {
	out := make([]*server.RepoConfig, 0, len(s.cfg.Repos))
	for i := range s.cfg.Repos {
		out = append(out, s.cfgs[s.cfg.Repos[i].Name])
	}
	return out
}

func (s *Service) bumpLocked() {
	s.gen++
	s.status.Generation = s.gen
}

// writeRepos persists the repository list, preserving the file's global fields.
func (s *Service) writeRepos(repos []config.Repo) error {
	if s.path == "" {
		return errors.New("no config file: mutations require --config")
	}
	disk := s.cfg
	if cur, err := config.Load(s.path); err == nil {
		disk = cur
	}
	disk.Repos = repos
	if err := config.WriteFile(s.path, disk); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	return nil
}

func cloneRepos(in []config.Repo) []config.Repo {
	out := make([]config.Repo, len(in))
	copy(out, in)
	return out
}

func retire(repos []*fs.Repo) {
	for _, r := range repos {
		if r == nil {
			continue
		}
		go func(r *fs.Repo) {
			time.Sleep(30 * time.Second)
			config.CloseStore(r.Store())
		}(r)
	}
}

func changedGlobals(a, b *config.Config) []string {
	var out []string
	add := func(name string, diff bool) {
		if diff {
			out = append(out, name)
		}
	}
	add("addr", a.Addr != b.Addr)
	add("base-path", a.BasePath != b.BasePath)
	add("hooks", a.Hooks != b.Hooks)
	add("tls-cert", a.TLSCert != b.TLSCert)
	add("tls-key", a.TLSKey != b.TLSKey)
	add("max-uploads", a.MaxUploads != b.MaxUploads)
	add("max-user-uploads", a.MaxUserUploads != b.MaxUserUploads)
	add("disable-metrics", a.DisableMetrics != b.DisableMetrics)
	add("admin-addr", a.AdminAddr != b.AdminAddr)
	add("admin-token", a.AdminToken != b.AdminToken)
	add("admin-token-file", a.AdminTokenFile != b.AdminTokenFile)
	add("admin-allow-cidr", a.AdminAllowCIDR != b.AdminAllowCIDR)
	return out
}

func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func decodeJSON(r *http.Request, v any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func queryBool(r *http.Request, key string) bool {
	switch strings.ToLower(r.URL.Query().Get(key)) {
	case "1", "true", "yes":
		return true
	}
	return false
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// ParseCIDRs parses a comma-separated allow list. An empty list means "no
// restriction"; callers normally pass at least the loopback ranges.
func ParseCIDRs(list string) ([]*net.IPNet, error) {
	var out []*net.IPNet
	for _, part := range strings.Split(list, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if !strings.Contains(part, "/") {
			if ip := net.ParseIP(part); ip != nil {
				bits := 32
				if ip.To4() == nil {
					bits = 128
				}
				part = fmt.Sprintf("%s/%d", ip.String(), bits)
			}
		}
		_, n, err := net.ParseCIDR(part)
		if err != nil {
			return nil, fmt.Errorf("invalid CIDR %q: %w", part, err)
		}
		out = append(out, n)
	}
	return out, nil
}
