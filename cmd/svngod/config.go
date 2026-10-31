package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"svngod/internal/auth"
	"svngod/internal/authz"
	"svngod/internal/fs"
	"svngod/internal/fs/store"
	"svngod/internal/server"
)

// fileConfig is the JSON server configuration. It supports multiple
// repositories, each with its own storage backend (local dir or S3-compatible
// object store), its own authz file, and its own authentication chain (one or
// more LDAP servers and/or a local password table).
//
//	{
//	  "addr": "0.0.0.0:19990",
//	  "base-path": "/svn",
//	  "repos": [
//	    { "name": "demo",
//	      "storage": { "type": "file", "root": "/srv/svn/repos" },
//	      "authz": "/etc/svngod/authz-demo",
//	      "auth": [
//	        { "type": "ldap", "url": "ldap://ldap1:389",
//	          "direct-bind-dn": "uid={{username}},ou=people,dc=example,dc=com" },
//	        { "type": "local", "passwd": "/etc/svngod/passwd-demo" }
//	      ] },
//	    { "name": "demo2",
//	      "storage": { "type": "s3", "endpoint": "http://rustfs:8333",
//	                   "bucket": "svn", "access-key": "k", "secret-key": "s",
//	                   "prefix": "repos" },
//	      "authz": "/etc/svngod/authz-demo2",
//	      "auth": [
//	        { "type": "ldap", "url": "ldap://ldap2:389",
//	          "base-dn": "ou=people,dc=other,dc=com", "filter": "(uid={{username}})" }
//	      ] }
//	  ]
//	}
type fileConfig struct {
	Addr     string `json:"addr"`
	BasePath string `json:"base-path"`
	Hooks    string `json:"hooks"`
	TLSCert  string `json:"tls-cert"`
	TLSKey   string `json:"tls-key"`
	// MaxUploads bounds concurrent PUT/bundle bodies (0 = server default 16).
	MaxUploads int        `json:"max-uploads"`
	Repos      []fileRepo `json:"repos"`
}

type fileRepo struct {
	Name    string     `json:"name"`
	Storage fileStore  `json:"storage"`
	Authz   string     `json:"authz"`
	Auth    []fileAuth `json:"auth"`
}

type fileStore struct {
	Type      string      `json:"type"` // "file" | "s3" | "mirror"
	Root      string      `json:"root"`
	Endpoint  string      `json:"endpoint"`
	AccessKey string      `json:"access-key"`
	SecretKey string      `json:"secret-key"`
	Bucket    string      `json:"bucket"`
	Prefix    string      `json:"prefix"`
	SSL       bool        `json:"ssl"`
	Mirrors   []fileStore `json:"mirrors"` // type=mirror: primary + standby backends
}

func (s *fileStore) makeStore() (store.Store, error) {
	switch s.Type {
	case "s3":
		return store.NewS3Store(store.S3Config{
			Endpoint:   s.Endpoint,
			AccessKey:  s.AccessKey,
			SecretKey:  s.SecretKey,
			Bucket:     s.Bucket,
			Region:     "us-east-1",
			UseSSL:     s.SSL,
			RepoPrefix: "", // full repo prefix (e.g. "repos/<name>") is passed
			// in by fs.OpenRepo via its prefix arg; keeping it empty here avoids
			// double-prefixing the S3 keys.
		})
	case "mirror":
		var ms []store.Store
		for i := range s.Mirrors {
			st, err := s.Mirrors[i].makeStore()
			if err != nil {
				return nil, err
			}
			ms = append(ms, st)
		}
		if len(ms) < 2 {
			return nil, fmt.Errorf("mirror storage requires at least 2 backends (got %d)", len(ms))
		}
		return store.NewMirror(ms, store.WithMutableOrder(fs.CompareMutable)), nil
	default:
		return store.NewFileStore(s.Root)
	}
}

type fileAuth struct {
	Type string `json:"type"` // "ldap" | "local"
	// ldap
	URL          string `json:"url"`
	StartTLS     bool   `json:"starttls"`
	BindDN       string `json:"bind-dn"`
	BindPassword string `json:"bind-password"`
	BaseDN       string `json:"base-dn"`
	Filter       string `json:"filter"`
	DirectBindDN string `json:"direct-bind-dn"`
	// local
	Passwd string `json:"passwd"`
}

func loadConfig(path string) (*fileConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c fileConfig
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, err
	}
	if c.BasePath == "" {
		c.BasePath = "/svn"
	}
	return &c, nil
}

func (r *fileRepo) makeStore() (store.Store, error) {
	return r.Storage.makeStore()
}

func (r *fileRepo) repoPrefix(name string) string {
	if r.Storage.Type == "s3" && r.Storage.Prefix != "" {
		return r.Storage.Prefix + "/" + name
	}
	return "repos/" + name
}

func (r *fileRepo) buildChain() *auth.Chain {
	var backends []auth.Authenticator
	for _, a := range r.Auth {
		switch a.Type {
		case "ldap":
			backends = append(backends, auth.NewLDAP(auth.LDAPConfig{
				URL:                 a.URL,
				StartTLS:            a.StartTLS,
				ServiceBindDN:       a.BindDN,
				ServiceBindPassword: a.BindPassword,
				BaseDN:              a.BaseDN,
				Filter:              a.Filter,
				DirectBindDN:        a.DirectBindDN,
				Timeout:             10 * time.Second,
			}))
		case "local":
			if a.Passwd != "" {
				backends = append(backends, auth.NewLocalPasswordFile(a.Passwd))
			}
		}
	}
	if len(backends) == 0 {
		return nil
	}
	return auth.NewChain(backends...)
}

// repoConfigs builds the per-repository server overrides from the config file.
func (c *fileConfig) repoConfigs() ([]*server.RepoConfig, error) {
	var out []*server.RepoConfig
	for i := range c.Repos {
		r := &c.Repos[i]
		st, err := r.makeStore()
		if err != nil {
			return nil, err
		}
		var az *authz.Config
		if r.Authz != "" {
			data, err := os.ReadFile(r.Authz)
			if err != nil {
				return nil, err
			}
			az, err = authz.Parse(data)
			if err != nil {
				return nil, err
			}
		}
		prefix := r.repoPrefix(r.Name)
		out = append(out, &server.RepoConfig{
			Name:         r.Name,
			Authenticate: r.buildChain(),
			Authorize:    az,
			OpenRepo: func(name string) (*fs.Repo, error) {
				return fs.OpenRepo(st, prefix)
			},
		})
	}
	return out, nil
}
