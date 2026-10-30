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
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"svngod/internal/auth"
	"svngod/internal/authz"
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

	hooksDir string
	tlsCert  string
	tlsKey   string
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
	var o options
	fl := flag.NewFlagSet("svngod", flag.ExitOnError)
	fl.StringVar(&o.addr, "addr", ":8080", "listen address")
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
	_ = fl.Parse(os.Args[1:])

	if configPath != "" {
		runMultiRepo(configPath)
		return
	}

	if o.repoRoot == "" {
		log.Fatal("--repo-root required")
	}
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

	srv := server.New(server.Config{
		BasePath:     o.basePath,
		Authenticate: chain,
		Authorize:    az,
		OpenRepo: func(name string) (*fs.Repo, error) {
			r, err := fs.OpenRepo(st, o.repoPrefix(name))
			if err != nil {
				return nil, err
			}
			return r, nil
		},
		HooksDir: o.hooksDir,
		Logger:   log.New(os.Stderr, "svngod ", log.LstdFlags),
	})

	log.Printf("svngod listening on %s (base %s, repo-root %s)", o.addr, o.basePath, o.repoRoot)
	if o.tlsCert != "" && o.tlsKey != "" {
		log.Printf("serving HTTPS (cert %s)", o.tlsCert)
		if err := http.ListenAndServeTLS(o.addr, o.tlsCert, o.tlsKey, srv.Handler()); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := http.ListenAndServe(o.addr, srv.Handler()); err != nil {
		log.Fatal(err)
	}
}

// runMultiRepo starts a server configured from a JSON file with one or more
// repositories, each with its own storage backend, authz file and auth chain.
func runMultiRepo(configPath string) {
	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	repos, err := cfg.repoConfigs()
	if err != nil {
		log.Fatalf("build repos: %v", err)
	}
	addr := cfg.Addr
	if addr == "" {
		addr = ":8080"
	}
	srv := server.New(server.Config{
		BasePath: cfg.BasePath,
		Repos:    repos,
		HooksDir: cfg.Hooks,
		Logger:   log.New(os.Stderr, "svngod ", log.LstdFlags),
	})
	log.Printf("svngod listening on %s (base %s, %d repos)", addr, cfg.BasePath, len(repos))
	if cfg.TLSCert != "" && cfg.TLSKey != "" {
		log.Printf("serving HTTPS (cert %s)", cfg.TLSCert)
		if err := http.ListenAndServeTLS(addr, cfg.TLSCert, cfg.TLSKey, srv.Handler()); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := http.ListenAndServe(addr, srv.Handler()); err != nil {
		log.Fatal(err)
	}
}
