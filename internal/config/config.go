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
	MaxUploads int `json:"max-uploads"`
	// MaxBodyBytes caps any request body. 0 uses the built-in default; -1
	// disables the cap. Per-endpoint limits are stricter and unchanged.
	MaxBodyBytes int64 `json:"max-body-bytes,omitempty"`

	// TempDir holds transfer temp files. Unset uses os.TempDir(). This matters
	// most for the config-driven deployments (systemd, containers), where /tmp is
	// commonly a small tmpfs and one large commit would consume its bytes in RAM.
	TempDir string `json:"temp-dir,omitempty"`

	// MinBodyRate is the floor for a request body's average speed in bytes per
	// second, judged after BodyRateGrace. A size cap bounds how much a client may
	// send, not how long it may take: a trickling client stays under the cap and
	// holds its connection and upload slot indefinitely. 0 disables the check.
	MinBodyRate int64 `json:"min-body-rate,omitempty"`
	// BodyRateGrace is how long a body may run before MinBodyRate is applied.
	// Unset uses the server default (30s).
	BodyRateGrace jsonDuration `json:"body-rate-grace,omitempty"`
	// BodyStallTimeout aborts a body read that receives no bytes for this long,
	// which is what a silently departed client looks like. Unset uses the server
	// default (2m); "-1s" disables it.
	BodyStallTimeout jsonDuration `json:"body-stall-timeout,omitempty"`

	// AuthFailureBurst is the per-username failed-login budget per
	// AuthFailureWindow. The per-address budget is 10x this, because addresses
	// are shared behind NAT. 0 disables the limiter.
	AuthFailureBurst  int          `json:"auth-failure-burst,omitempty"`
	AuthFailureWindow jsonDuration `json:"auth-failure-window,omitempty"`

	// MaxConns caps simultaneous TCP connections. This is the only limit that
	// applies before a request exists; the upload limit runs inside a handler,
	// after the socket and its buffers are already allocated. 0 = unlimited.
	MaxConns       int  `json:"max-conns,omitempty"`
	MaxUserUploads int  `json:"max-user-uploads"`
	DisableMetrics bool `json:"disable-metrics"`
	// MetricsToken gates /_metrics. Its labels name every repository, so an
	// unauthenticated endpoint exposes the tenant list. Empty falls back to
	// SVNGOD_METRICS_TOKEN, then to requiring a repository credential.
	MetricsToken string `json:"metrics-token,omitempty"`
	// ScrubInterval enables the background integrity Verifier at this period
	// (0 disables it). Default when unset: 10m.
	ScrubInterval jsonDuration `json:"scrub-interval"`

	// AdminAddr enables the management API on a separate listener (empty =
	// AdminTokenFile; it is redacted from API responses.
	AdminAddr      string `json:"admin-addr,omitempty"`
	AdminToken     string `json:"admin-token,omitempty"`
	AdminTokenFile string `json:"admin-token-file,omitempty"`
	AdminAllowCIDR string `json:"admin-allow-cidr,omitempty"`

	Repos []Repo `json:"repos"`
}

// jsonDuration accepts either a duration string ("10m", "1h30m") or a whole
// number of seconds in JSON.
type jsonDuration time.Duration

func (d *jsonDuration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		v, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		*d = jsonDuration(v)
		return nil
	}
	var n float64
	if err := json.Unmarshal(b, &n); err != nil {
		return err
	}
	*d = jsonDuration(time.Duration(n * float64(time.Second)))
	return nil
}

func (d jsonDuration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d jsonDuration) Std() time.Duration { return time.Duration(d) }

// Repo is one repository entry.
type Repo struct {
	Name    string `json:"name"`
	Storage Store  `json:"storage"`
	Authz   string `json:"authz"`
	Auth    []Auth `json:"auth"`
	// Segments switches a repository created with ?create=1 to the batched
	// segment format (v2). It has no effect on an existing repository.
	Segments      bool   `json:"segments,omitempty"`
	SegmentTarget uint64 `json:"segment-target,omitempty"`
}

// Store describes a storage backend (or a mirror of backends).
type Store struct {
	Type      string `json:"type"` // "file" | "s3" | "mirror"
	Root      string `json:"root"`
	Endpoint  string `json:"endpoint"`
	AccessKey string `json:"access-key"`
	SecretKey string `json:"secret-key"`
	// SecretKeyFile names a file holding the S3 secret key, mirroring
	// admin-token-file: it keeps credentials out of the JSON document. The
	// file is read when the store is built, so hot reloads pick up rotation.
	SecretKeyFile string  `json:"secret-key-file,omitempty"`
	Bucket        string  `json:"bucket"`
	Prefix        string  `json:"prefix"`
	SSL           bool    `json:"ssl"`
	Mirrors       []Store `json:"mirrors"`
	Quorum        int     `json:"quorum"`
	// Geo wiring: Local and Remote are mirror specs (type "mirror") for the
	// writer region and the disaster-read region. Local commits ack on the
	// local quorum; the remote converges asynchronously.
	Local  *Store `json:"local,omitempty"`
	Remote *Store `json:"remote,omitempty"`
	// ShardRanges routes a repository's revision history across several
	// backends (type "sharded"): [from, to] shard indices inclusive.
	ShardRanges []ShardRange `json:"ranges,omitempty"`
	ShardMeta   *Store       `json:"meta,omitempty"`
	ShardSize   uint64       `json:"shard-size,omitempty"` // revisions per shard (default 4096)
}

// geoEntry pairs a GeoStore with its repository prefix for metric labels.
type geoEntry struct {
	prefix string
	g      *store.GeoStore
}

// geoStores collects the GeoStores built by makeStore so the server can
// expose their status and close them on shutdown.
var geoStores []geoEntry

// GeoStores returns the geo stores built so far (process lifetime).
func GeoStores() []geoEntry { return geoStores }

// ShardRange is one row of a sharded store's ownership table.
type ShardRange struct {
	From  uint64  `json:"from"`
	To    *uint64 `json:"to"` // omit for "to infinity"
	Store *Store  `json:"store"`
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

// resolveSecret returns the S3 secret key, reading SecretKeyFile when set.
// Exactly one of secret-key / secret-key-file may be provided. The file is
// read at store-build time so a hot reload picks up rotation without editing
// the JSON document (mirrors admin-token-file).
func (s *Store) resolveSecret() (string, error) {
	if s.SecretKeyFile == "" {
		return s.SecretKey, nil
	}
	if s.SecretKey != "" {
		return "", fmt.Errorf("s3 storage: set either secret-key or secret-key-file, not both")
	}
	b, err := os.ReadFile(s.SecretKeyFile)
	if err != nil {
		return "", fmt.Errorf("s3 secret-key-file: %w", err)
	}
	return strings.TrimSpace(string(b)), nil
}

func (s *Store) makeStore(prefix string) (store.Store, error) {
	switch s.Type {
	case "geo":
		if s.Local == nil || s.Remote == nil {
			return nil, fmt.Errorf("geo storage needs local and remote mirror specs")
		}
		local, err := s.Local.makeStore(prefix)
		if err != nil {
			return nil, err
		}
		remote, err := s.Remote.makeStore(prefix)
		if err != nil {
			return nil, err
		}
		lm, ok := local.(*store.MirrorStore)
		if !ok {
			return nil, fmt.Errorf("geo local must be a mirror (got %s)", s.Local.Type)
		}
		rm, ok := remote.(*store.MirrorStore)
		if !ok {
			return nil, fmt.Errorf("geo remote must be a mirror (got %s)", s.Remote.Type)
		}
		g := store.NewGeoStore(store.GeoConfig{
			Local:       lm,
			Remote:      rm,
			RepoPrefix:  prefix,
			ControlName: store.NewKeys(prefix).Current(),
		})
		gg := g
		server.RegisterGeoStatus(prefix, func() (uint64, uint64, uint64) {
			st := gg.Status()
			return st.Pending, st.Shipped, st.Errors
		})
		geoStores = append(geoStores, geoEntry{prefix: prefix, g: g})
		return g, nil
	case "sharded":
		meta := s.ShardMeta
		if meta == nil || len(s.ShardRanges) == 0 {
			return nil, fmt.Errorf("sharded storage needs meta and ranges")
		}
		mst, err := meta.makeStore(prefix)
		if err != nil {
			return nil, err
		}
		shardSize := s.ShardSize
		if shardSize == 0 {
			shardSize = 4096
		}
		var ranges []store.ShardRange
		for i, r := range s.ShardRanges {
			st, err := r.Store.makeStore(prefix)
			if err != nil {
				return nil, err
			}
			to := ^uint64(0)
			if r.To != nil {
				to = *r.To
			}
			ranges = append(ranges, store.ShardRange{From: r.From, To: to, Store: st})
			_ = i
		}
		return store.NewShardRouter(store.ShardRouterConfig{
			Prefix:    prefix,
			Meta:      mst,
			ShardSize: shardSize,
			Ranges:    ranges,
		})
	case "s3":
		secret, serr := s.resolveSecret()
		if serr != nil {
			return nil, serr
		}
		return store.NewS3Store(store.S3Config{
			Endpoint:  s.Endpoint,
			AccessKey: s.AccessKey,
			SecretKey: secret,
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
	if r.Segments {
		_, err = fs.CreateRepoSegments(st, prefix, 4096, 8192, cdc.DefaultParams(), false, r.SegmentTarget)
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
	if out.MetricsToken != "" {
		out.MetricsToken = "***"
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

// redactStore clears every credential a store can carry, including the stores
// nested inside geo (Local/Remote) and sharded (ShardMeta/ShardRanges) specs.
//
// Redacting only AccessKey/SecretKey/Mirrors was not enough: a geo or sharded
// store holds its own S3 keys one level down, and both shapes are reachable
// through /_api/v1/config - a repo using one is rejected by Validate(), but the
// raw config is still loaded and handed to the admin service, so the nested
// credentials were printed verbatim. redactSecretKeyFile is covered too: a path
// to a key file is as sensitive as the key it holds.
func redactStore(s *Store) {
	if s == nil {
		return
	}
	if s.AccessKey != "" {
		s.AccessKey = "***"
	}
	if s.SecretKey != "" {
		s.SecretKey = "***"
	}
	if s.SecretKeyFile != "" {
		s.SecretKeyFile = "***"
	}
	for i := range s.Mirrors {
		redactStore(&s.Mirrors[i])
	}
	redactStore(s.Local)
	redactStore(s.Remote)
	redactStore(s.ShardMeta)
	for i := range s.ShardRanges {
		redactStore(s.ShardRanges[i].Store)
	}
}

// Equal reports whether two repository entries are byte-identical.
func (r *Repo) Equal(o *Repo) bool {
	return bytes.Equal([]byte(r.Fingerprint()), []byte(o.Fingerprint()))
}
