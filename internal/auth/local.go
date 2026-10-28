package auth

import (
	"bufio"
	"context"
	"os"
	"strings"
)

// LocalPasswordFile authenticates against a local password table in
// svnpasswd-style format, one entry per line:
//
//	username:password
//
// where password may be plaintext, {SHA}base64, or {CRYPT}$6$<salt>$<hash>.
// Blank lines and lines starting with '#' are ignored.
type LocalPasswordFile struct {
	path string
	// entries maps username → stored password record. Loaded lazily on first
	// Authenticate; reload when the file mtime changes is left to the server
	// layer (recreate the authenticator), keeping this type stateless.
	entries map[string]string
}

// NewLocalPasswordFile returns an authenticator backed by the given file.
func NewLocalPasswordFile(path string) *LocalPasswordFile {
	return &LocalPasswordFile{path: path}
}

func (l *LocalPasswordFile) load() error {
	if l.entries != nil {
		return nil
	}
	f, err := os.Open(l.path)
	if err != nil {
		return err
	}
	defer f.Close()

	entries := make(map[string]string)
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		i := strings.IndexByte(line, ':')
		if i <= 0 {
			continue
		}
		entries[line[:i]] = line[i+1:]
	}
	if err := sc.Err(); err != nil {
		return err
	}
	l.entries = entries
	return nil
}

// Authenticate verifies username/password against the table.
func (l *LocalPasswordFile) Authenticate(_ context.Context, username, password string) (Identity, error) {
	if err := l.load(); err != nil {
		return Identity{}, err
	}
	stored, ok := l.entries[username]
	if !ok {
		return Identity{}, ErrInvalidCredentials
	}
	if !VerifyPassword(stored, password) {
		return Identity{}, ErrInvalidCredentials
	}
	return Identity{Username: username, AuthenticatedBy: "local"}, nil
}
