package server

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"svngod/internal/auth"
	"svngod/internal/authz"
	"svngod/internal/fs"
	"svngod/internal/fs/store"
)

// RepoOpener opens (and caches) a repository by name.
type RepoOpener func(name string) (*fs.Repo, error)

// Config configures the DAV server.
type Config struct {
	// BasePath is the URL prefix under which repositories are served, e.g.
	// "/svn" or "". A repo "demo" is then reachable at BasePath+"/demo/".
	BasePath string

	// Global defaults; a per-repo RepoConfig may override any of these.
	Authenticate *auth.Chain
	Authorize    *authz.Config
	OpenRepo     RepoOpener

	// Repos holds per-repository overrides so distinct repositories can use
	// different backends (File/S3), different authz files and different auth
	// chains (e.g. one LDAP server per repo). A repo not listed here uses the
	// global defaults.
	Repos []*RepoConfig

	// RepoReady is the result of the startup self-check: repo name -> readable.
	// It is exposed as svngod_repo_ready so a misconfigured backend is visible
	// in the metrics too, not only in the boot log.
	RepoReady map[string]bool

	// HooksDir holds optional pre-commit / post-commit executables. They run
	// with SVN_USER/SVN_REPO/SVN_TXN (and SVN_REV for post-commit) in the
	// environment; a non-zero pre-commit exit rejects the commit.
	HooksDir string

	// MaxConcurrentUploads bounds simultaneous PUT/bundle bodies so a few
	// multi-gigabyte uploads cannot exhaust disk or memory. Zero = unlimited.
	MaxConcurrentUploads int

	// MaxUserUploads caps concurrent uploads per authenticated user, so one
	// client cannot occupy every slot. Zero = unlimited.
	MaxUserUploads int

	// DisableMetrics turns off the /_metrics exposition endpoint.
	DisableMetrics bool

	// Logger for request logging; nil disables.
	Logger *log.Logger
}

// RepoConfig overrides storage, authorization and authentication for a single
// repository. Leaving a field nil falls back to the global Config value.
type RepoConfig struct {
	Name         string
	Authenticate *auth.Chain
	Authorize    *authz.Config
	OpenRepo     RepoOpener
	// Close releases the store built for this entry (stops mirror repair
	// loops). Set by the caller that owns the storage.
	Close func()
}

// repoSet is an immutable snapshot of the per-repository overrides. Reload
// swaps the pointer; requests keep the snapshot they started with.
type repoSet struct {
	list   []*RepoConfig
	byName map[string]*RepoConfig
}

func makeRepoSet(list []*RepoConfig) (*repoSet, error) {
	set := &repoSet{list: list, byName: make(map[string]*RepoConfig, len(list))}
	for _, rc := range list {
		if rc.Name == "" {
			return nil, errors.New("server: repository with empty name")
		}
		if _, dup := set.byName[rc.Name]; dup {
			return nil, fmt.Errorf("server: duplicate repository %q", rc.Name)
		}
		set.byName[rc.Name] = rc
	}
	return set, nil
}

func (s *Server) repoCfg(name string) *RepoConfig {
	set := s.repos.Load()
	if set == nil {
		return nil
	}
	return set.byName[name]
}

func (s *Server) authFor(name string) *auth.Chain {
	if rc := s.repoCfg(name); rc != nil && rc.Authenticate != nil {
		return rc.Authenticate
	}
	return s.cfg.Authenticate
}

func (s *Server) authzFor(name string) *authz.Config {
	if rc := s.repoCfg(name); rc != nil && rc.Authorize != nil {
		return rc.Authorize
	}
	return s.cfg.Authorize
}

func (s *Server) openerFor(name string) RepoOpener {
	if rc := s.repoCfg(name); rc != nil && rc.OpenRepo != nil {
		return rc.OpenRepo
	}
	return s.cfg.OpenRepo
}

// RepoNames returns the repositories currently configured (for the admin API).
func (s *Server) RepoNames() []string {
	set := s.repos.Load()
	if set == nil {
		return nil
	}
	out := make([]string, 0, len(set.list))
	for _, rc := range set.list {
		out = append(out, rc.Name)
	}
	return out
}

// SetRepos atomically installs a new repository set. Entries named in
// invalidate (added, changed or removed) are dropped from the per-repo cache;
// their open Repo instances are returned so the caller can close them after
// in-flight requests finish.
func (s *Server) SetRepos(list []*RepoConfig, invalidate map[string]bool) ([]*fs.Repo, error) {
	set, err := makeRepoSet(list)
	if err != nil {
		return nil, err
	}
	var retired []*fs.Repo
	s.repoMapMu.Lock()
	s.repos.Store(set)
	inSet := set.byName
	for name := range invalidate {
		if prev := s.repoCache[name]; prev != nil {
			delete(s.repoCache, name)
			retired = append(retired, prev)
		}
	}
	for name, prev := range s.repoCache {
		if _, ok := inSet[name]; !ok {
			delete(s.repoCache, name)
			retired = append(retired, prev)
		}
	}
	// Locks/activities of removed repositories are no longer reachable.
	for name := range invalidate {
		if _, ok := inSet[name]; !ok {
			s.mu.Lock()
			s.locksLoaded[name] = false
			s.stateLoaded[name] = false
			s.mu.Unlock()
		}
	}
	s.repoMapMu.Unlock()
	return retired, nil
}

// Server is the WebDAV/DeltaV HTTP handler for svngod.
type Server struct {
	cfg Config

	mu          sync.Mutex
	activities  map[string]*activity
	txns        map[string]*txn
	locks       map[string]*lockEntry // key: repo + "\x00" + path
	locksLoaded map[string]bool       // repositories whose persisted locks were restored
	locksETag   map[string]string     // content hash of the persisted lock table last seen
	lockOpsMu   sync.Mutex            // serialises refresh/mutate of the lock table
	stateLoaded map[string]bool       // repositories whose persisted activities were restored
	repoMapMu   sync.Mutex
	repos       atomic.Pointer[repoSet] // current per-repo overrides (hot-swappable)
	repoCache   map[string]*fs.Repo     // per-name repo singletons (shared r.mu)
	txnCleanup  chan txnCleanupItem     // background fragment cleanup
	uploads     chan struct{}           // nil when uploads are unlimited
	upMu        sync.Mutex
	userUploads map[string]int
	m           *metrics
}

// New builds a Server.
func New(cfg Config) *Server {
	s := &Server{
		cfg:         cfg,
		activities:  map[string]*activity{},
		txns:        map[string]*txn{},
		locks:       map[string]*lockEntry{},
		locksLoaded: map[string]bool{},
		locksETag:   map[string]string{},
		stateLoaded: map[string]bool{},
		repoCache:   map[string]*fs.Repo{},
		userUploads: map[string]int{},
		m:           newMetrics(cfg.MaxConcurrentUploads),
		txnCleanup:  make(chan txnCleanupItem, 1024),
	}
	go s.txnCleanupLoop()
	if cfg.MaxConcurrentUploads > 0 {
		s.uploads = make(chan struct{}, cfg.MaxConcurrentUploads)
	}
	if set, err := makeRepoSet(cfg.Repos); err == nil {
		s.repos.Store(set)
	}
	return s
}

// acquireUpload reserves one upload slot, waiting briefly for a busy server,
// then enforces the per-user limit. The returned release must be called by
// the handler.
func (s *Server) acquireUpload(w http.ResponseWriter, r *http.Request, user string) (func(), bool) {
	releaseGlobal := func() {}
	if s.uploads != nil {
		select {
		case s.uploads <- struct{}{}:
			releaseGlobal = func() { <-s.uploads }
		case <-r.Context().Done():
			return nil, false
		case <-time.After(30 * time.Second):
			w.Header().Set("Retry-After", "30")
			http.Error(w, "server busy: too many concurrent uploads", http.StatusServiceUnavailable)
			return nil, false
		}
	}
	s.m.acquire()
	counted := false
	if s.cfg.MaxUserUploads > 0 && user != "" {
		s.upMu.Lock()
		if s.userUploads[user] >= s.cfg.MaxUserUploads {
			s.upMu.Unlock()
			releaseGlobal()
			s.m.release()
			w.Header().Set("Retry-After", "30")
			http.Error(w, "user upload limit reached", http.StatusTooManyRequests)
			return nil, false
		}
		s.userUploads[user]++
		counted = true
		s.upMu.Unlock()
	}
	return func() {
		s.m.release()
		if counted {
			s.upMu.Lock()
			if n := s.userUploads[user] - 1; n > 0 {
				s.userUploads[user] = n
			} else {
				delete(s.userUploads, user)
			}
			s.upMu.Unlock()
		}
		releaseGlobal()
	}, true
}

// WebDAV/DeltaV method names not defined by net/http.
const (
	MethodPropfind   = "PROPFIND"
	MethodProppatch  = "PROPPATCH"
	MethodMkcol      = "MKCOL"
	MethodCopy       = "COPY"
	MethodMove       = "MOVE"
	MethodLock       = "LOCK"
	MethodUnlock     = "UNLOCK"
	MethodMkactivity = "MKACTIVITY"
	MethodCheckout   = "CHECKOUT"
	MethodMerge      = "MERGE"
	MethodReport     = "REPORT"
)

// identityKey carries the authenticated principal in the request context.
type identityKey struct{}

// statusWriter records the response status code for logging.
type statusWriter struct {
	http.ResponseWriter
	code int
	n    int64
}

func (s *statusWriter) WriteHeader(code int) { s.code = code; s.ResponseWriter.WriteHeader(code) }
func (s *statusWriter) Write(b []byte) (int, error) {
	if s.code == 0 {
		s.code = 200
	}
	n, err := s.ResponseWriter.Write(b)
	s.n += int64(n)
	return n, err
}

// IdentityFrom pulls the authenticated identity out of a request context.
func IdentityFrom(ctx context.Context) auth.Identity {
	if id, ok := ctx.Value(identityKey{}).(auth.Identity); ok {
		return id
	}
	return auth.Identity{Username: ""}
}

// Handler returns the full HTTP handler with auth/authz middleware applied.
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(s.route)
}

// route is the top-level dispatcher: auth, then per-method handling.
func (s *Server) route(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/_metrics" {
		s.serveMetrics(w)
		return
	}
	var cr *countingReadCloser
	if r.Body != nil {
		cr = &countingReadCloser{r: r.Body}
		r.Body = cr
	}
	sw := &statusWriter{ResponseWriter: w}
	w = sw
	// One summary line per request (status + user) when a logger is set; the
	// previous unconditional line doubled the log volume of a 30k-file commit.
	user := ""
	start := time.Now()
	defer func() {
		if cr != nil {
			s.m.addRead(cr.n)
		}
		s.m.addWrite(sw.n)
		elapsed := time.Since(start)
		s.m.observe(r.Method, sw.code, elapsed)
		if r.Method == "MERGE" {
			s.m.observeCommit(elapsed)
		}
		if s.cfg.Logger != nil {
			s.cfg.Logger.Printf("HTTP %d %s %s user=%q %v", sw.code, r.Method, r.URL.Path, user, time.Since(start).Round(time.Microsecond))
		}
	}()
	if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/!svn/me") {
		s.handlePostMe(w, r)
		return
	}
	// Negotiated bulk commit channel (svngocli): one request, one atomic
	// revision; no DAV resources are involved, so it bypasses routing.
	if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/!svngod/bundle") {
		s.handleBundleCommit(w, r)
		return
	}
	if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/!svngod/stat") {
		s.handleBundleStat(w, r)
		return
	}
	if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/!svngod/chunks") {
		s.handleBundleChunks(w, r)
		return
	}
	ctx := r.Context()
	res := parseResource(s.cfg.BasePath, r.URL.Path)

	ident, ok := s.authenticate(w, r, res.repo)
	if !ok {
		return // 401 already written
	}
	user = ident.Username
	ctx = context.WithValue(ctx, identityKey{}, ident)
	r = r.WithContext(ctx)

	// Authorize: for methods that mutate, require write to the resource path.
	if !s.authorized(ident.Username, res, r.Method) {
		if ident.Username == "" && s.authFor(res.repo) != nil {
			// Anonymous was denied: challenge for credentials so the SVN
			// client retries with Basic auth, instead of a 403 it cannot
			// recover from. ($anonymous-allowed reads never reach here.)
			s.unauthorized(w)
			return
		}
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	switch r.Method {
	case http.MethodOptions:
		s.handleOptions(w, r, res)
	case MethodPropfind:
		s.handlePropfind(w, r, res)
	case http.MethodGet, http.MethodHead:
		s.handleGet(w, r, res)
	case MethodReport:
		s.handleReport(w, r, res)
	case MethodMkactivity:
		s.handleMkactivity(w, r, res)
	case MethodProppatch:
		s.handleProppatch(w, r, res)
	case http.MethodPut:
		s.handlePut(w, r, res)
	case http.MethodDelete:
		s.handleDelete(w, r, res)
	case "CHECKOUT":
		s.handleCheckout(w, r, res)
	case "MERGE":
		s.handleMerge(w, r, res)
	case MethodMkcol:
		s.handleMkcol(w, r, res)
	case MethodCopy, MethodMove:
		s.handleCopyMove(w, r, res)
	case MethodLock, MethodUnlock:
		s.handleLockUnlock(w, r, res)
	default:
		w.Header().Set("Allow", allowedMethods)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
}

// writeMethods require write permission on the resource path.
var writeMethods = map[string]bool{
	MethodMkactivity: true, MethodProppatch: true, "PUT": true, "DELETE": true,
	"CHECKOUT": true, "MERGE": true, MethodMkcol: true, "COPY": true,
	"MOVE": true, "LOCK": true, "UNLOCK": true,
}

// authorized applies the authz file (if configured) to the request, honoring
// a per-repository override.
func (s *Server) authorized(user string, res resource, method string) bool {
	az := s.authzFor(res.repo)
	if az == nil {
		return true
	}
	path := res.path
	if path == "" {
		path = "/"
	}
	switch method {
	case MethodMkactivity, "CHECKOUT", "MERGE":
		// Creating an activity / opening a working resource / finalizing a
		// commit is not a path operation. Write access is enforced per changed
		// path when the transaction is committed (handleMerge), so users whose
		// write grant is on a subtree (e.g. /trunk) can commit without
		// write on "/".
		return user != "" || s.authFor(res.repo) == nil
	case MethodCopy:
		// Copying needs read access to the source; the destination is
		// checked at commit time. Requiring write on the source made
		// read-only sources impossible to branch from.
		return az.Authorize(user, res.repo, path, false)
	case MethodProppatch:
		// PROPPATCH on the transaction root carries revision properties
		// (svn:log etc.), not a node change; node props are still checked.
		if res.typ == resTxn && (res.path == "" || res.path == "/") {
			return user != "" || s.authFor(res.repo) == nil
		}
	case http.MethodDelete:
		// Aborting an activity / transaction is cleanup, not a path change.
		if res.typ == resActivity || res.typ == resTxn {
			return user != "" || s.authFor(res.repo) == nil
		}
	}
	write := writeMethods[method]
	return az.Authorize(user, res.repo, path, write)
}

// authenticate parses Basic credentials and runs the auth chain selected for
// the target repository (per-repo override or global).
func (s *Server) authenticate(w http.ResponseWriter, r *http.Request, repo string) (auth.Identity, bool) {
	chain := s.authFor(repo)
	if chain == nil {
		return auth.Identity{Username: ""}, true // no auth configured: anonymous
	}
	h := r.Header.Get("Authorization")
	if h == "" {
		// No credentials: proceed as anonymous. authz may allow $anonymous.
		return auth.Identity{Username: ""}, true
	}
	const prefix = "Basic "
	if !strings.HasPrefix(h, prefix) {
		return auth.Identity{}, s.unauthorized(w)
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(h, prefix))
	if err != nil {
		return auth.Identity{}, s.unauthorized(w)
	}
	user, pass, ok := strings.Cut(string(raw), ":")
	if !ok {
		return auth.Identity{}, s.unauthorized(w)
	}
	id, err := chain.Authenticate(r.Context(), user, pass)
	if err != nil {
		return auth.Identity{}, s.unauthorized(w)
	}
	return id, true
}

func (s *Server) unauthorized(w http.ResponseWriter) bool {
	w.Header().Set("WWW-Authenticate", `Basic realm="svngod"`)
	http.Error(w, "Authorization Required", http.StatusUnauthorized)
	return false
}

// --- helpers ---

func (s *Server) repo(w http.ResponseWriter, res resource) (*fs.Repo, bool) {
	opener := s.openerFor(res.repo)
	if opener == nil {
		// No global opener and no per-repo entry: the name is not configured
		// (a repository removed by the admin API must look like 404, not 500).
		http.NotFound(w, nil)
		return nil, false
	}
	// Reuse a per-repo singleton so concurrent requests share the Repo's
	// commit mutex: CommitTxn then serialises cleanly while still committing
	// disjoint-path changes as interleaved revisions (path-level OOD check).
	s.repoMapMu.Lock()
	r := s.repoCache[res.repo]
	s.repoMapMu.Unlock()
	if r != nil {
		s.ensureLocksLoaded(res.repo, r)
		s.ensureActivitiesLoaded(res.repo, r)
		return r, true
	}
	ropen, err := opener(res.repo)
	if err != nil {
		if errors.Is(err, fs.ErrRepoNotFound) {
			http.NotFound(w, nil)
		} else if storageUnavailable(err) {
			// A dead backend is transient: let clients retry instead of
			// reporting a generic server error.
			w.Header().Set("Retry-After", "2")
			http.Error(w, "repository storage is temporarily unavailable", http.StatusServiceUnavailable)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return nil, false
	}
	s.repoMapMu.Lock()
	if prev := s.repoCache[res.repo]; prev != nil {
		ropen = prev
	}
	s.repoCache[res.repo] = ropen
	s.repoMapMu.Unlock()
	s.ensureLocksLoaded(res.repo, ropen)
	s.ensureActivitiesLoaded(res.repo, ropen)
	return ropen, true
}

// storageUnavailable reports whether an open error is a transient backend
// failure (dead node, lost quorum, permission outage) rather than corruption.
func storageUnavailable(err error) bool {
	if errors.Is(err, store.ErrQuorumUnmet) || errors.Is(err, store.ErrReplicaLagging) {
		return true
	}
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, os.ErrPermission) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// absURL builds an absolute URL for a repo-relative private/public href.
func (s *Server) absURL(r *http.Request, repo, href string) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	host := r.Host
	if host == "" {
		host = r.URL.Host
	}
	base := s.cfg.BasePath
	if base == "" {
		base = ""
	} else {
		base = "/" + strings.Trim(base, "/")
	}
	return fmt.Sprintf("%s://%s%s/%s%s", scheme, host, base, repo, href)
}

// notImpl logs and returns 501 for methods not yet implemented.
func (s *Server) notImpl(w http.ResponseWriter, method string) {
	http.Error(w, method+" not yet implemented", http.StatusNotImplemented)
}
