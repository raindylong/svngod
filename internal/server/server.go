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

	// Authenticate is the credential chain (LDAP then local table, or custom).
	Authenticate *auth.Chain

	// Authorize is the parsed authz file; nil means allow all authenticated
	// operations (still requires a user for write unless a password file is
	// absent).
	Authorize *authz.Config

	// OpenRepo returns a repository by name, or an error if it does not exist.
	OpenRepo RepoOpener

	// Logger for request logging; nil disables.
	Logger *log.Logger
}

// Server is the WebDAV/DeltaV HTTP handler for svngod.
type Server struct {
	cfg Config

	mu         sync.Mutex
	activities map[string]*activity
	txns       map[string]*txn
	locks      map[string]*lockEntry // key: repo + "\x00" + path
}

// New builds a Server.
func New(cfg Config) *Server {
	return &Server{
		cfg:        cfg,
		activities: map[string]*activity{},
		txns:       map[string]*txn{},
		locks:      map[string]*lockEntry{},
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
	if s.cfg.Logger != nil {
		hasAuth := r.Header.Get("Authorization") != ""
		if r.Method == http.MethodOptions {
			s.cfg.Logger.Printf("RAW %s %s auth=%v svn-caps=%q user-agent=%q accept=%q",
				r.Method, r.URL.Path, hasAuth,
				r.Header.Get("SVN-Client-Capabilities"),
				r.Header.Get("User-Agent"),
				r.Header.Get("Accept"))
		} else {
			s.cfg.Logger.Printf("RAW %s %s auth=%v", r.Method, r.URL.Path, hasAuth)
		}
	}
	ctx := r.Context()
	ident, ok := s.authenticate(w, r)
	if !ok {
		return // 401 already written
	}
	ctx = context.WithValue(ctx, identityKey{}, ident)
	r = r.WithContext(ctx)

	res := parseResource(s.cfg.BasePath, r.URL.Path)

	// Authorize: for methods that mutate, require write to the resource path.
	if !s.authorized(ident.Username, res, r.Method) {
		if ident.Username == "" && s.cfg.Authenticate != nil {
			// Anonymous was denied: challenge for credentials so the SVN
			// client retries with Basic auth, instead of a 403 it cannot
			// recover from. ($anonymous-allowed reads never reach here.)
			s.unauthorized(w)
			return
		}
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	if s.cfg.Logger != nil {
		s.cfg.Logger.Printf("%s %s user=%q", r.Method, r.URL.Path, ident.Username)
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

// authorized applies the authz file (if configured) to the request.
func (s *Server) authorized(user string, res resource, method string) bool {
	if s.cfg.Authorize == nil {
		return true
	}
	repo := res.repo
	path := res.path
	if path == "" {
		path = "/"
	}
	write := writeMethods[method]
	return s.cfg.Authorize.Authorize(user, repo, path, write)
}

// authenticate parses Basic credentials and runs the auth chain.
func (s *Server) authenticate(w http.ResponseWriter, r *http.Request) (auth.Identity, bool) {
	if s.cfg.Authenticate == nil {
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
	id, err := s.cfg.Authenticate.Authenticate(r.Context(), user, pass)
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
	if s.cfg.OpenRepo == nil {
		http.Error(w, "no repositories configured", http.StatusInternalServerError)
		return nil, false
	}
	r, err := s.cfg.OpenRepo(res.repo)
	if err != nil {
		if errors.Is(err, fs.ErrRepoNotFound) {
			http.NotFound(w, nil)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return nil, false
	}
	return r, true
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
