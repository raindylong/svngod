// Package config parses svngod's JSON configuration and builds the runtime
// repository descriptors (storage, auth chain, authz) used by the server and
// by the admin API.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"svngod/internal/auth"
	"svngod/internal/authz"
	"svngod/internal/cdc"
	"svngod/internal/fs"
	"svngod/internal/fs/store"
	"svngod/internal/server"
)

// ErrRepoExists is returned by Provision when the repository already exists.
var ErrRepoExists = errors.New("config: repository already exists")

// Config is the JSON server configuration.
//
//	{
//	  "addr": "0.0.0.0:19990",
//	  "base-path": "/svn",
//	  "repos": [
//	    { "name": "demo",
//	      "storage": { "type": "file", "root": "/srv/svn/repos" },
//	      "authz": "/etc/svngod/authz-demo",
//	      "auth": [ { "type": "local", "passwd": "/etc/svngod/passwd" } ] }
//	  ]
//	}
type Config struct {
	Addr     string `json:"addr"`
	BasePath string `json:"base-path"`
	Hooks    string `json:"hooks"`
	TLSCert  string `json:"tls-cert"`
	TLSKey   string `json:"tls-key"`
	// MaxUploads bounds concurrent PUT/bundle bodies (0 = server default 16).
	MaxUploads     int  `json:"max-uploads"`
	MaxUserUploads int  `json:"max-user-uploads"`
	DisableMetrics bool `json:"disable-metrics"`

	// AdminAddr enables the management API on a separate listener (empty =
	// disabled). AdminToken may also come from SVNGOD_ADMIN_TOKEN or
	// AdminTokenFile; it is redacted from API responses.
	AdminAddr      string `json:"admin-addr,omitempty"`
	AdminToken     string `json:"admin-token,omitempty"`
	AdminTokenFile string `json:"admin-token-file,omitempty"`
	AdminAllowCIDR string `json:"admin-allow-cidr,omitempty"`

	Repos []Repo `json:"repos"`
}

// Repo is one repository entry.
type Repo struct {
	Name    string `json:"name"`
	Storage Store  `json:"storage"`
	Authz   string `json:"authz"`
	Auth    []Auth `json:"auth"`
}

// Store describes a storage backend (or a mirror of backends).
type Store struct {
	Type      string  `json:"type"` // "file" | "s3" | "mirror"
	Root      string  `json:"root"`
	Endpoint  string  `json:"endpoint"`
	AccessKey string  `json:"access-key"`
	SecretKey string  `json:"secret-key"`
	Bucket    string  `json:"bucket"`
	Prefix    string  `json:"prefix"`
	SSL       bool    `json:"ssl"`
	Mirrors   []Store `json:"mirrors"`
	Quorum    int     `json:"quorum"`
}

// Auth is one authentication backend.
type Auth struct {
	Type         string `json:"type"` // "ldap" | "local"
	URL          string `json:"url"`
	StartTLS     bool   `json:"starttls"`
	BindDN       string `json:"bind-dn"`
	BindPassword string `json:"bind-password"`
	BaseDN       string `json:"base-dn"`
	Filter       string `json:"filter"`
	DirectBindDN string `json:"direct-bind-dn"`
	Passwd       string `json:"passwd"`
}

var nameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// Load reads and defaults a configuration file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(data)
}

// Parse decodes a configuration document.
func Parse(data []byte) (*Config, error) {
	var c Config
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, err
	}
	if c.BasePath == "" {
		c.BasePath = "/svn"
	}
	return &c, nil
}

// Marshal renders the configuration as indented JSON.
func (c *Config) Marshal() ([]byte, error) {
	return json.MarshalIndent(c, "", " ")
}

// WriteFile replaces path atomically (temp file + fsync + rename), keeping a
// .bak of the previous file. Permissions follow the existing file (0600 when
// the file is new).
func WriteFile(path string, c *Config) error {
	data, err := c.Marshal()
	if err != nil {
		return err
	}
	mode := os.FileMode(0o600)
	if st, err := os.Stat(path); err == nil {
		mode = st.Mode().Perm()
		if old, rerr := os.ReadFile(path); rerr == nil {
			_ = os.WriteFile(path+".bak", old, mode)
		}
	}
	tmp, err := os.CreateTemp(dirOf(path), ".svngod-config-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func dirOf(path string) string {
	if i := strings.LastIndexByte(path, '/'); i > 0 {
		return path[:i]
	}
	return "."
}

// Prefix is the store-level repository prefix (storage.prefix + name).
func (r *Repo) Prefix() string {
	if (r.Storage.Type == "s3" || r.Storage.Type == "mirror") && r.Storage.Prefix != "" {
		return strings.Trim(r.Storage.Prefix, "/") + "/" + r.Name
	}
	return "repos/" + r.Name
}

// MakeStore builds the configured store. A mirror store starts its background
// repair goroutine, so callers that discard the store must CloseStore it.
func (r *Repo) MakeStore() (store.Store, error) {
	return r.Storage.makeStore(r.Prefix())
}

func (s *Store) makeStore(prefix string) (store.Store, error) {
	switch s.Type {
	case "s3":
		return store.NewS3Store(store.S3Config{
			Endpoint:  s.Endpoint,
			AccessKey: s.AccessKey,
			SecretKey: s.SecretKey,
			Bucket:    s.Bucket,
			Region:    "us-east-1",
			UseSSL:    s.SSL,
		})
	case "mirror":
		var ms []store.Store
		for i := range s.Mirrors {
			st, err := s.Mirrors[i].makeStore(prefix)
			if err != nil {
				return nil, err
			}
			ms = append(ms, st)
		}
		if len(ms) < 2 {
			return nil, fmt.Errorf("mirror storage requires at least 2 backends (got %d)", len(ms))
		}
		opts := []store.MirrorOption{store.WithMutableOrder(fs.CompareMutable), store.WithRepoPrefix(prefix), store.WithLazyRepair()}
		if s.Quorum > 0 {
			opts = append(opts, store.WithWriteQuorum(s.Quorum))
		}
		return store.NewMirror(ms, opts...), nil
	default:
		return store.NewFileStore(s.Root)
	}
}

// CloseStore stops background goroutines when the store supports it.
func CloseStore(st store.Store) {
	if c, ok := st.(interface{ Close() }); ok {
		c.Close()
	}
}

// Chain builds the authentication chain (nil when no backend is configured).
func (r *Repo) Chain() *auth.Chain {
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

// Validate checks the parts that do not require touching a backend.
func (r *Repo) Validate() error {
	if !nameRe.MatchString(r.Name) {
		return fmt.Errorf("invalid repository name %q", r.Name)
	}
	if r.Storage.Type != "file" && r.Storage.Type != "s3" && r.Storage.Type != "mirror" {
		return fmt.Errorf("repository %q: unknown storage type %q", r.Name, r.Storage.Type)
	}
	if err := r.Storage.validate(); err != nil {
		return fmt.Errorf("repository %q: %w", r.Name, err)
	}
	for _, a := range r.Auth {
		switch a.Type {
		case "ldap":
			if a.URL == "" {
				return fmt.Errorf("repository %q: ldap auth needs url", r.Name)
			}
			if !strings.HasPrefix(a.URL, "ldap://") && !strings.HasPrefix(a.URL, "ldaps://") {
				return fmt.Errorf("repository %q: ldap url must start with ldap:// or ldaps://", r.Name)
			}
			if a.DirectBindDN == "" && a.BaseDN == "" {
				return fmt.Errorf("repository %q: ldap auth needs base-dn or direct-bind-dn", r.Name)
			}
		case "local":
			if a.Passwd == "" {
				return fmt.Errorf("repository %q: local auth needs passwd", r.Name)
			}
			if _, err := os.Stat(a.Passwd); err != nil {
				return fmt.Errorf("repository %q: passwd file: %w", r.Name, err)
			}
		default:
			return fmt.Errorf("repository %q: unknown auth type %q", r.Name, a.Type)
		}
	}
	if r.Authz != "" {
		data, err := os.ReadFile(r.Authz)
		if err != nil {
			return fmt.Errorf("repository %q: authz: %w", r.Name, err)
		}
		if _, err := authz.Parse(data); err != nil {
			return fmt.Errorf("repository %q: authz: %w", r.Name, err)
		}
	}
	return nil
}

func (s *Store) validate() error {
	switch s.Type {
	case "file":
		if s.Root == "" {
			return errors.New("file storage needs root")
		}
	case "s3":
		if s.Endpoint == "" || s.Bucket == "" {
			return errors.New("s3 storage needs endpoint and bucket")
		}
		if strings.Contains(s.Endpoint, "://") {
			return errors.New("s3 endpoint must be host:port (no scheme)")
		}
	case "mirror":
		if len(s.Mirrors) < 2 {
			return errors.New("mirror storage requires at least 2 backends")
		}
		if s.Quorum > len(s.Mirrors) {
			return fmt.Errorf("quorum %d exceeds %d backends", s.Quorum, len(s.Mirrors))
		}
		for i := range s.Mirrors {
			if s.Mirrors[i].Type == "mirror" {
				return errors.New("nested mirror storage is not supported")
			}
			if err := s.Mirrors[i].validate(); err != nil {
				return err
			}
		}
	}
	return nil
}

// RepoConfig builds the server descriptor and verifies the repository exists.
func (r *Repo) RepoConfig() (*server.RepoConfig, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	st, err := r.MakeStore()
	if err != nil {
		return nil, err
	}
	prefix := r.Prefix()
	if _, err := fs.OpenRepo(st, prefix); err != nil {
		CloseStore(st)
		return nil, fmt.Errorf("repository %q: %w", r.Name, err)
	}
	var az *authz.Config
	if r.Authz != "" {
		data, err := os.ReadFile(r.Authz)
		if err != nil {
			CloseStore(st)
			return nil, err
		}
		az, err = authz.Parse(data)
		if err != nil {
			CloseStore(st)
			return nil, err
		}
	}
	return &server.RepoConfig{
		Name:         r.Name,
		Authenticate: r.Chain(),
		Authorize:    az,
		OpenRepo: func(string) (*fs.Repo, error) {
			return fs.OpenRepo(st, prefix)
		},
		Close: func() { CloseStore(st) },
	}, nil
}

// Provision creates an empty repository, refusing to overwrite an existing one.
func (r *Repo) Provision() error {
	if err := r.Validate(); err != nil {
		return err
	}
	st, err := r.MakeStore()
	if err != nil {
		return err
	}
	defer CloseStore(st)
	prefix := r.Prefix()
	if _, err := fs.OpenRepo(st, prefix); err == nil {
		return ErrRepoExists
	} else if !errors.Is(err, fs.ErrRepoNotFound) {
		return err
	}
	_, err = fs.CreateRepoFull(st, prefix, 4096, 8192, cdc.DefaultParams(), false)
	return err
}

// RepoConfigs builds every repository descriptor.
func (c *Config) RepoConfigs() ([]*server.RepoConfig, error) {
	out, _, err := c.BuildRepos(nil, nil)
	return out, err
}

// BuildRepos builds descriptors for added/changed repositories and reuses prev
// for entries whose JSON is unchanged, so a reload neither reopens stores nor
// leaks mirror goroutines. It returns the ordered list and a name→descriptor
// map for the next reload.
func (c *Config) BuildRepos(prev map[string]*server.RepoConfig, prevCfg map[string]string) ([]*server.RepoConfig, map[string]*server.RepoConfig, error) {
	out := make([]*server.RepoConfig, 0, len(c.Repos))
	byName := make(map[string]*server.RepoConfig, len(c.Repos))
	seen := map[string]bool{}
	for i := range c.Repos {
		r := &c.Repos[i]
		if seen[r.Name] {
			return nil, nil, fmt.Errorf("duplicate repository %q", r.Name)
		}
		seen[r.Name] = true
		fp := r.Fingerprint()
		if prev != nil && prevCfg != nil && prevCfg[r.Name] == fp && prev[r.Name] != nil {
			rc := prev[r.Name]
			out = append(out, rc)
			byName[r.Name] = rc
			continue
		}
		rc, err := r.RepoConfig()
		if err != nil {
			return nil, nil, err
		}
		out = append(out, rc)
		byName[r.Name] = rc
	}
	return out, byName, nil
}

// Fingerprint returns a stable identity for change detection.
func (r *Repo) Fingerprint() string {
	b, _ := json.Marshal(r)
	return string(b)
}

// Fingerprints maps every repository to its fingerprint.
func (c *Config) Fingerprints() map[string]string {
	out := make(map[string]string, len(c.Repos))
	for i := range c.Repos {
		out[c.Repos[i].Name] = c.Repos[i].Fingerprint()
	}
	return out
}

// Redacted returns a copy safe to expose over the admin API.
func (c *Config) Redacted() *Config {
	b, _ := json.Marshal(c)
	var out Config
	_ = json.Unmarshal(b, &out)
	if out.AdminToken != "" {
		out.AdminToken = "***"
	}
	for i := range out.Repos {
		redactStore(&out.Repos[i].Storage)
		for j := range out.Repos[i].Auth {
			if out.Repos[i].Auth[j].BindPassword != "" {
				out.Repos[i].Auth[j].BindPassword = "***"
			}
		}
	}
	return &out
}

func redactStore(s *Store) {
	if s.AccessKey != "" {
		s.AccessKey = "***"
	}
	if s.SecretKey != "" {
		s.SecretKey = "***"
	}
	for i := range s.Mirrors {
		redactStore(&s.Mirrors[i])
	}
}

// Equal reports whether two repository entries are byte-identical.
func (r *Repo) Equal(o *Repo) bool {
	return bytes.Equal([]byte(r.Fingerprint()), []byte(o.Fingerprint()))
}