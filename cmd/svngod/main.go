// Command svngod is the SVN server implemented in Go.
//
// It serves Subversion repositories over HTTP(S) WebDAV/DeltaV, authenticates
// via an LDAP chain (OpenLDAP) then a local password table, authorizes against
// a local authz file, and stores version objects in an S3-compatible object
// store (default RustFS) or a local directory.
//
// usage:
//
//	svngod \
//	  [--addr :8080] [--base-path /svn] \
//	  [--repo-root /var/lib/svngod] \
//	  [--passwd /etc/svngod/passwd] [--authz /etc/svngod/authz] \
//	  [--ldap-url ldap://host:389 --ldap-base-dn dc=example,dc=com --ldap-filter '(uid={{username}})' ...] \
//	  [--endpoint 127.0.0.1:9000 --access-key K --secret-key S --bucket B [--prefix repos] [--ssl]]
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"svngod/internal/admin"
	"svngod/internal/auth"
	"svngod/internal/authz"
	"svngod/internal/config"
	"svngod/internal/fs"
	"svngod/internal/fs/store"
	"svngod/internal/server"
)

type options struct {
	addr      string
	basePath  string
	repoRoot  string
	passwd    string
	authzFile string

	ldapURL          string
	ldapStartTLS     bool
	ldapBindDN       string
	ldapBindPassword string
	ldapBaseDN       string
	ldapFilter       string
	ldapDirectBindDN string

	endpoint  string
	accessKey string
	secretKey string
	bucket    string
	prefix    string
	useSSL    bool

	commitBatch    int
	commitWindowMs int

	adminAddr      string
	adminToken     string
	adminAllowCIDR string

	hooksDir       string
	tlsCert        string
	tlsKey         string
	maxUploads     int
	maxUserUploads int
	metrics        bool
	strictStart    bool
}

func (o *options) makeStore() (store.Store, error) {
	if o.endpoint == "" {
		return store.NewFileStore(o.repoRoot)
	}
	return store.NewS3Store(store.S3Config{
		Endpoint:   o.endpoint,
		AccessKey:  o.accessKey,
		SecretKey:  o.secretKey,
		Bucket:     o.bucket,
		Region:     "us-east-1",
		UseSSL:     o.useSSL,
		RepoPrefix: o.prefix,
	})
}

func (o *options) repoPrefix(name string) string {
	if o.endpoint == "" {
		return "repos/" + name
	}
	if o.prefix != "" {
		return o.prefix + "/" + name
	}
	return "repos/" + name
}

func (o *options) buildChain() *auth.Chain {
	var backends []auth.Authenticator
	if o.ldapURL != "" || o.ldapDirectBindDN != "" {
		backends = append(backends, auth.NewLDAP(auth.LDAPConfig{
			URL:                 o.ldapURL,
			StartTLS:            o.ldapStartTLS,
			ServiceBindDN:       o.ldapBindDN,
			ServiceBindPassword: o.ldapBindPassword,
			BaseDN:              o.ldapBaseDN,
			Filter:              o.ldapFilter,
			DirectBindDN:        o.ldapDirectBindDN,
			Timeout:             10 * time.Second,
		}))
	}
	if o.passwd != "" {
		backends = append(backends, auth.NewLocalPasswordFile(o.passwd))
	}
	return auth.NewChain(backends...)
}

func main() {
	cleanupStaleTempFiles()
	var o options
	fl := flag.NewFlagSet("svngod", flag.ExitOnError)
	fl.StringVar(&o.addr, "addr", ":8080", "listen address")
	fl.IntVar(&o.maxUploads, "max-uploads", 16, "maximum concurrent PUT/bundle uploads (0 = unlimited)")
	fl.IntVar(&o.commitBatch, "commit-batch", 16, "group concurrently arriving commits into batches of this size (0/1 = off)")
	fl.IntVar(&o.commitWindowMs, "commit-window-ms", 2, "milliseconds to wait for more commits once a batch starts")
	fl.StringVar(&o.adminAddr, "admin-addr", "", "management API listen address (empty = disabled; requires --config and a token)")
	fl.StringVar(&o.adminToken, "admin-token", "", "management API bearer token (prefer SVNGOD_ADMIN_TOKEN)")
	fl.StringVar(&o.adminAllowCIDR, "admin-allow-cidr", "", "comma-separated CIDRs allowed to call the management API (default loopback)")
	fl.IntVar(&o.maxUserUploads, "max-user-uploads", 4, "maximum concurrent uploads per user (0 = unlimited)")
	fl.BoolVar(&o.metrics, "metrics", true, "expose Prometheus metrics at /_metrics")
	fl.StringVar(&o.basePath, "base-path", "/svn", "URL prefix for repositories")
	fl.StringVar(&o.repoRoot, "repo-root", "./repos", "local directory holding repositories")
	fl.StringVar(&o.passwd, "passwd", "", "local password file (plain/{SHA}/{CRYPT})")
	fl.StringVar(&o.authzFile, "authz", "", "authz file")
	fl.StringVar(&o.ldapURL, "ldap-url", "", "ldap://host:port or ldaps://host:port")
	fl.BoolVar(&o.ldapStartTLS, "ldap-starttls", false, "upgrade LDAP with StartTLS")
	fl.StringVar(&o.ldapBindDN, "ldap-bind-dn", "", "LDAP service bind DN for search")
	fl.StringVar(&o.ldapBindPassword, "ldap-bind-password", "", "LDAP service bind password")
	fl.StringVar(&o.ldapBaseDN, "ldap-base-dn", "", "LDAP search base DN")
	fl.StringVar(&o.ldapFilter, "ldap-filter", "", "LDAP search filter, e.g. (uid={{username}})")
	fl.StringVar(&o.ldapDirectBindDN, "ldap-direct-bind-dn", "", "LDAP direct bind DN template (skips search)")
	fl.StringVar(&o.endpoint, "endpoint", "", "S3 endpoint host:port (empty = local dir)")
	fl.StringVar(&o.accessKey, "access-key", "", "S3 access key")
	fl.StringVar(&o.secretKey, "secret-key", "", "S3 secret key")
	fl.StringVar(&o.bucket, "bucket", "", "S3 bucket")
	fl.StringVar(&o.prefix, "prefix", "", "S3 repo prefix")
	fl.BoolVar(&o.useSSL, "ssl", false, "use https for S3")
	fl.StringVar(&o.hooksDir, "hooks-dir", "", "directory with pre-commit / post-commit hooks")
	fl.StringVar(&o.tlsCert, "tls-cert", "", "TLS certificate file (serve HTTPS)")
	fl.StringVar(&o.tlsKey, "tls-key", "", "TLS key file (serve HTTPS)")
	var configPath string
	fl.StringVar(&configPath, "config", "", "JSON config file (multi-repo; overrides flags)")
	fl.BoolVar(&o.strictStart, "strict-start", false, "refuse to start when a configured repository is unreadable")
	_ = fl.Parse(os.Args[1:])

	if configPath != "" {
		runMultiRepo(configPath, o.commitBatch, o.commitWindowMs, o.adminAddr, o.adminToken, o.adminAllowCIDR, o.strictStart)
		return
	}

	if o.repoRoot == "" {
		log.Fatal("--repo-root required")
	}
	if o.adminAddr != "" {
		log.Printf("admin API disabled: it requires --config (mutations must persist to a JSON file)")
	}
	fs.ConfigureCommitBatch(o.commitBatch, o.commitWindowMs)
	st, err := o.makeStore()
	if err != nil {
		log.Fatal(err)
	}
	chain := o.buildChain()

	var az *authz.Config
	if o.authzFile != "" {
		data, err := os.ReadFile(o.authzFile)
		if err != nil {
			log.Fatal(err)
		}
		az, err = authz.Parse(data)
		if err != nil {
			log.Fatalf("parse authz: %v", err)
		}
	}

	// Startup self-check for the single-repo (local directory) mode: enumerate
	// the repositories under the root and read each HEAD once, so a broken
	// directory shows up at boot and in /_metrics.
	ready := map[string]bool{}
	if o.endpoint == "" {
		if entries, derr := os.ReadDir(filepath.Join(o.repoRoot, "repos")); derr == nil {
			for _, e := range entries {
				if !e.IsDir() {
					continue
				}
				r, oerr := fs.OpenRepo(st, o.repoPrefix(e.Name()))
				if oerr != nil {
					log.Printf("self-check: repository %q NOT READY: %v", e.Name(), oerr)
					ready[e.Name()] = false
					continue
				}
				if _, herr := r.HeadFresh(); herr != nil {
					log.Printf("self-check: repository %q head unreadable: %v", e.Name(), herr)
					ready[e.Name()] = false
					continue
				}
				ready[e.Name()] = true
			}
			if len(ready) > 0 {
				log.Printf("self-check: %d repositories checked", len(ready))
			}
		}
	}
	srv := server.New(server.Config{
		BasePath:     o.basePath,
		RepoReady:    ready,
		Authenticate: chain,
		Authorize:    az,
		OpenRepo: func(name string) (*fs.Repo, error) {
			r, err := fs.OpenRepo(st, o.repoPrefix(name))
			if err != nil {
				return nil, err
			}
			return r, nil
		},
		HooksDir:             o.hooksDir,
		MaxConcurrentUploads: o.maxUploads,
		MaxUserUploads:       o.maxUserUploads,
		DisableMetrics:       !o.metrics,
		Logger:               log.New(os.Stderr, "svngod ", log.LstdFlags),
	})

	log.Printf("svngod listening on %s (base %s, repo-root %s)", o.addr, o.basePath, o.repoRoot)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go srv.RunMaintenance(ctx, 10*time.Minute)
	hs := newHTTPServer(o.addr, srv.Handler())
	if o.tlsCert != "" && o.tlsKey != "" {
		log.Printf("serving HTTPS (cert %s)", o.tlsCert)
		if err := hs.ListenAndServeTLS(o.tlsCert, o.tlsKey); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := hs.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

// newHTTPServer applies the timeouts that keep a stalled client from pinning
// a connection forever. WriteTimeout is deliberately absent: multi-gigabyte
// uploads and downloads are expected to take minutes.
func newHTTPServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
}

// runMultiRepo starts a server configured from a JSON file with one or more
// repositories, each with its own storage backend, authz file and auth chain.
func runMultiRepo(configPath string, commitBatch, commitWindowMs int, adminAddr, adminToken, adminAllowCIDR string, strictStart bool) {
	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	fs.ConfigureCommitBatch(commitBatch, commitWindowMs)
	repos, cfgs, err := cfg.BuildRepos(nil, nil)
	buildFailures := map[string]string{}
	if err != nil {
		// Fault containment: one broken repository must not stop the others from
		// being served. -strict-start keeps the fail-fast behaviour for
		// deployments that prefer it.
		if strictStart {
			log.Fatalf("build repos: %v", err)
		}
		log.Printf("build repos: %v; retrying per repository", err)
		repos = nil
		cfgs = map[string]*server.RepoConfig{}
		for i := range cfg.Repos {
			name := cfg.Repos[i].Name
			one := *cfg
			one.Repos = []config.Repo{cfg.Repos[i]}
			r1, c1, e1 := one.BuildRepos(nil, nil)
			if e1 != nil {
				buildFailures[name] = classifyRepoError(e1)
				log.Printf("self-check: repository %q disabled: %v", name, e1)
				continue
			}
			repos = append(repos, r1...)
			for k, v := range c1 {
				cfgs[k] = v
			}
		}
	}
	addr := cfg.Addr
	if addr == "" {
		addr = ":8080"
	}
	maxUploads := cfg.MaxUploads
	if maxUploads == 0 {
		maxUploads = 16
	}
	maxUserUploads := cfg.MaxUserUploads
	if maxUserUploads == 0 {
		maxUserUploads = 4
	}
	logger := log.New(os.Stderr, "svngod ", log.LstdFlags)
	// Startup self-check: open every configured repository and read its HEAD so
	// a bad endpoint, bucket or permission is visible in the boot log instead of
	// on the first client request. strictStart turns those failures fatal.
	bad := len(buildFailures)
	ready := make(map[string]bool, len(repos)+len(buildFailures))
	readyErr := make(map[string]string, len(buildFailures))
	for name, reason := range buildFailures {
		ready[name] = false
		readyErr[name] = reason
	}
	for _, rc := range repos {
		ready[rc.Name] = false
		if rc.OpenRepo == nil {
			continue
		}
		r, oerr := rc.OpenRepo(rc.Name)
		if oerr != nil {
			bad++
			readyErr[rc.Name] = classifyRepoError(oerr)
			logger.Printf("self-check: repository %q NOT READY: %v", rc.Name, oerr)
			continue
		}
		head, herr := r.HeadFresh()
		if herr != nil {
			bad++
			readyErr[rc.Name] = classifyRepoError(herr)
			logger.Printf("self-check: repository %q head unreadable: %v", rc.Name, herr)
			continue
		}
		uuid, _ := r.UUID()
		ready[rc.Name] = true
		logger.Printf("self-check: repository %q ready (head=%d uuid=%s segments=%v)", rc.Name, head, uuid, r.SegmentsEnabled())
	}
	if strictStart && bad > 0 {
		log.Fatalf("self-check: %d of %d repositories are not ready (strict-start)", bad, len(repos))
	}
	srv := server.New(server.Config{
		BasePath:             cfg.BasePath,
		Repos:                repos,
		RepoReady:            ready,
		RepoReadyError:       readyErr,
		HooksDir:             cfg.Hooks,
		MaxConcurrentUploads: maxUploads,
		MaxUserUploads:       maxUserUploads,
		DisableMetrics:       cfg.DisableMetrics,
		Logger:               logger,
	})
	startAdmin(configPath, cfg, cfgs, srv, adminAddr, adminToken, adminAllowCIDR, logger)
	log.Printf("svngod listening on %s (base %s, %d repos)", addr, cfg.BasePath, len(repos))
	hs := newHTTPServer(addr, srv.Handler())
	if cfg.TLSCert != "" && cfg.TLSKey != "" {
		log.Printf("serving HTTPS (cert %s)", cfg.TLSCert)
		if err := hs.ListenAndServeTLS(cfg.TLSCert, cfg.TLSKey); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := hs.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

// startAdmin runs the management API on its own listener. It is disabled
// unless an address is configured; running it without a token is refused.
func startAdmin(configPath string, cfg *config.Config, cfgs map[string]*server.RepoConfig,
	srv *server.Server, addr, token, allowCIDR string, logger *log.Logger) {
	if addr == "" {
		addr = cfg.AdminAddr
	}
	if addr == "" {
		return
	}
	if token == "" {
		token = os.Getenv("SVNGOD_ADMIN_TOKEN")
	}
	if token == "" {
		token = cfg.AdminToken
	}
	if token == "" && cfg.AdminTokenFile != "" {
		if b, err := os.ReadFile(cfg.AdminTokenFile); err == nil {
			token = strings.TrimSpace(string(b))
		} else {
			log.Fatalf("admin token file: %v", err)
		}
	}
	if token == "" {
		log.Fatal("admin API requires a token: set -admin-token, SVNGOD_ADMIN_TOKEN, or admin-token-file")
	}
	if allowCIDR == "" {
		allowCIDR = cfg.AdminAllowCIDR
	}
	if allowCIDR == "" {
		allowCIDR = "127.0.0.1/32,::1/128"
	}
	allow, err := admin.ParseCIDRs(allowCIDR)
	if err != nil {
		log.Fatalf("admin allow list: %v", err)
	}
	svc := admin.New(configPath, cfg, cfgs, srv, token, allow, logger)
	hs := newHTTPServer(addr, svc.Handler())
	go func() {
		logger.Printf("admin API listening on %s (allow %s)", addr, allowCIDR)
		if err := hs.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Printf("admin API stopped: %v", err)
		}
	}()
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		for range hup {
			if _, err := svc.ReloadNow(); err != nil {
				logger.Printf("SIGHUP reload failed: %v", err)
			} else {
				logger.Printf("SIGHUP reload ok")
			}
		}
	}()
}

// staleTempRe matches temp files created by server/mirror transfers, which
// embed the owning process id: svngod-<kind>-<pid>-<random>.
var staleTempRe = regexp.MustCompile(`^svngod-(?:body|delta|text|txn|put|mirror)-(\d+)-`)

// cleanupStaleTempFiles removes transfer temp files left behind by processes
// that no longer exist (a crash or kill during an upload). The running
// process's own files and those of live processes are kept.
func cleanupStaleTempFiles() {
	cleanupStaleTempFilesIn(os.TempDir(), func(pid int) bool {
		_, err := os.Stat(fmt.Sprintf("/proc/%d", pid))
		return err == nil
	})
}

func cleanupStaleTempFilesIn(dir string, alive func(int) bool) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		m := staleTempRe.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		pid, err := strconv.Atoi(m[1])
		if err != nil || pid == os.Getpid() || alive(pid) {
			continue
		}
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			log.Printf("cleanup temp %s: %v", e.Name(), err)
		}
	}
}

// classifyRepoError maps a self-check failure to a small, bounded label set.
// Prometheus label values must not carry arbitrary error text.
func classifyRepoError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "access denied"), strings.Contains(msg, "forbidden"),
		strings.Contains(msg, "403"), strings.Contains(msg, "unauthorized"):
		return "auth_denied"
	case strings.Contains(msg, "not found"), strings.Contains(msg, "nosuchbucket"),
		strings.Contains(msg, "no such"), strings.Contains(msg, "404"):
		return "not_found"
	case strings.Contains(msg, "timeout"), strings.Contains(msg, "connection"),
		strings.Contains(msg, "dial"), strings.Contains(msg, "refused"),
		strings.Contains(msg, "unreachable"):
		return "network"
	case strings.Contains(msg, "format"), strings.Contains(msg, "decode"),
		strings.Contains(msg, "checksum"), strings.Contains(msg, "corrupt"):
		return "corrupt"
	default:
		return "other"
	}
}
