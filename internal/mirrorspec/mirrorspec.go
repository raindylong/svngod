// Package mirrorspec parses the JSON used by svnadmin --mirror-file and
// revscale -mirror-file, and builds the member stores it describes.
//
// It exists because that JSON is written by operators by hand (and copied out
// of a server config), and two independent copies of the parsing rules had
// already drifted: one read "access"/"secret", the other accepted the config's
// "access-key"/"secret-key", so a config-shaped file silently sent anonymous
// requests and failed with a bare Access Denied. One parser, one answer.
package mirrorspec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"

	"svngod/internal/fs/store"
)

// Member is one backend in a mirror spec.
type Member struct {
	Type     string `json:"type"`
	Root     string `json:"root"`
	Endpoint string `json:"endpoint"`
	Access   string `json:"access"`
	Secret   string `json:"secret"`
	// Config spellings, accepted so a storage.mirrors block can be pasted
	// straight into a --mirror-file argument.
	AccessKey string `json:"access-key"`
	SecretKey string `json:"secret-key"`
	Bucket    string `json:"bucket"`
	SSL       bool   `json:"ssl"`
}

// Creds resolves the access/secret pair under either accepted spelling.
func (m Member) Creds() (string, string) {
	ak, sk := m.Access, m.Secret
	if ak == "" {
		ak = m.AccessKey
	}
	if sk == "" {
		sk = m.SecretKey
	}
	return ak, sk
}

// Spec is a parsed mirror file.
type Spec struct {
	Members []Member
	Quorum  int
	Prefix  string // bucket-level / root-level repository prefix
}

// Parse reads a mirror file. The JSON may be a bare array of members or an
// object {"mirrors":[...],"quorum":N,"prefix":P}.
func Parse(path string) (*Spec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	spec := &Spec{Prefix: "repos"}
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) > 0 && trimmed[0] == '{' {
		var obj struct {
			Mirrors []Member `json:"mirrors"`
			Quorum  int      `json:"quorum"`
			Prefix  string   `json:"prefix"`
		}
		if err := json.Unmarshal(trimmed, &obj); err != nil {
			return nil, fmt.Errorf("parse mirror file: %v", err)
		}
		spec.Members, spec.Quorum = obj.Mirrors, obj.Quorum
		if obj.Prefix != "" {
			spec.Prefix = obj.Prefix
		}
	} else if err := json.Unmarshal(data, &spec.Members); err != nil {
		return nil, fmt.Errorf("parse mirror file: %v", err)
	}
	if len(spec.Members) < 2 {
		return nil, fmt.Errorf("mirror file requires at least 2 backends (got %d)", len(spec.Members))
	}
	return spec, nil
}

// Store opens one member as an independent view of the repository. S3 members
// must carry credentials: failing here with a clear message beats letting the
// request go out anonymously and come back as Access Denied.
func (m Member) Store() (store.Store, error) {
	if m.Type == "s3" {
		ak, sk := m.Creds()
		if ak == "" || sk == "" {
			return nil, fmt.Errorf("mirror %s: missing credentials "+
				"(set \"access\"/\"secret\" or \"access-key\"/\"secret-key\")", m.Endpoint)
		}
		return store.NewS3Store(store.S3Config{
			Endpoint: m.Endpoint, AccessKey: ak, SecretKey: sk,
			Bucket: m.Bucket, Region: "us-east-1", UseSSL: m.SSL,
		})
	}
	if m.Root == "" {
		return nil, fmt.Errorf("mirror member of type %q needs \"root\"", m.Type)
	}
	return store.NewFileStore(m.Root)
}

// Members builds every member store in file order.
func (s *Spec) Stores() ([]store.Store, error) {
	out := make([]store.Store, 0, len(s.Members))
	for _, m := range s.Members {
		st, err := m.Store()
		if err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, nil
}

// RepoPrefix returns the repository prefix for a named repository.
func (s *Spec) RepoPrefix(name string) string {
	return trimSlash(s.Prefix) + "/" + name
}

func trimSlash(p string) string {
	for len(p) > 0 && (p[0] == '/' || p[len(p)-1] == '/') {
		if p[0] == '/' {
			p = p[1:]
			continue
		}
		p = p[:len(p)-1]
	}
	return p
}
