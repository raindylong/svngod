package server

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"svngod/internal/auth"
	"svngod/internal/authz"
	"svngod/internal/fs"
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

	// HooksDir holds optional pre-commit / post-commit executables. They run
	// with SVN_USER/SVN_REPO/SVN_TXN (and SVN_REV for post-commit) in the
	// environment; a non-zero pre-commit exit rejects the commit.
	HooksDir string

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
}

func (c *Config) repoCfg(name string) *RepoConfig {
	for i := range c.Repos {
		if c.Repos[i].Name == name {
			return c.Repos[i]
		}
	}
	return nil
}

func (c *Config) authFor(name string) *auth.Chain {
	if rc := c.repoCfg(name); rc != nil && rc.Authenticate != nil {
		return rc.Authenticate
	}
	return c.Authenticate
}

func (c *Config) authzFor(name string) *authz.Config {
	if rc := c.repoCfg(name); rc != nil && rc.Authorize != nil {
		return rc.Authorize
	}
	return c.Authorize
}

func (c *Config) openerFor(name string) RepoOpener {
	if rc := c.repoCfg(name); rc != nil && rc.OpenRepo != nil {
		return rc.OpenRepo
	}
	return c.OpenRepo
}

// Server is the WebDAV/DeltaV HTTP handler for svngod.
type Server struct {
	cfg Config

	mu          sync.Mutex
	activities  map[string]*activity
	txns        map[string]*txn
	locks       map[string]*lockEntry // key: repo + "\x00" + path
	locksLoaded map[string]bool       // repositories whose persisted locks were restored
	stateLoaded map[string]bool       // repositories whose persisted activities were restored
	repoMapMu   sync.Mutex
	repoCache   map[string]*fs.Repo // per-name repo singletons (shared r.mu)
}

// New builds a Server.
func New(cfg Config) *Server {
	return &Server{
		cfg:         cfg,
		activities:  map[string]*activity{},
		txns:        map[string]*txn{},
		locks:       map[string]*lockEntry{},
		locksLoaded: map[string]bool{},
		stateLoaded: map[string]bool{},
		repoCache:   map[string]*fs.Repo{},
	}
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
}

func (s *statusWriter) WriteHeader(code int) { s.code = code; s.ResponseWriter.WriteHeader(code) }
func (s *statusWriter) Write(b []byte) (int, error) {
	if s.code == 0 {
		s.code = 200
	}
	return s.ResponseWriter.Write(b)
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
	sw := &statusWriter{ResponseWriter: w}
	w = sw
	// One summary line per request (status + user) when a logger is set; the
	// previous unconditional line doubled the log volume of a 30k-file commit.
	user := ""
	start := time.Now()
	defer func() {
		if s.cfg.Logger != nil {
			s.cfg.Logger.Printf("HTTP %d %s %s user=%q %v", sw.code, r.Method, r.URL.Path, user, time.Since(start).Round(time.Microsecond))
		}
	}()
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
		if ident.Username == "" && s.cfg.authFor(res.repo) != nil {
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
	az := s.cfg.authzFor(res.repo)
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
		return user != "" || s.cfg.authFor(res.repo) == nil
	case MethodCopy:
		// Copying needs read access to the source; the destination is
		// checked at commit time. Requiring write on the source made
		// read-only sources impossible to branch from.
		return az.Authorize(user, res.repo, path, false)
	case MethodProppatch:
		// PROPPATCH on the transaction root carries revision properties
		// (svn:log etc.), not a node change; node props are still checked.
		if res.typ == resTxn && (res.path == "" || res.path == "/") {
			return user != "" || s.cfg.authFor(res.repo) == nil
		}
	case http.MethodDelete:
		// Aborting an activity / transaction is cleanup, not a path change.
		if res.typ == resActivity || res.typ == resTxn {
			return user != "" || s.cfg.authFor(res.repo) == nil
		}
	}
	write := writeMethods[method]
	return az.Authorize(user, res.repo, path, write)
}

// authenticate parses Basic credentials and runs the auth chain selected for
// the target repository (per-repo override or global).
func (s *Server) authenticate(w http.ResponseWriter, r *http.Request, repo string) (auth.Identity, bool) {
	chain := s.cfg.authFor(repo)
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
	opener := s.cfg.openerFor(res.repo)
	if opener == nil {
		http.Error(w, "no repositories configured", http.StatusInternalServerError)
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
