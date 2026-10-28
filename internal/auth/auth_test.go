package auth

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-ldap/ldap/v3"
)

func TestLocalPasswordFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "passwd")
	// plaintext + {SHA} + {CRYPT}$6$
	content := "# comment\n" +
		"alice:secret\n" +
		"bob:{SHA}" + sha1B64("pw2") + "\n" +
		"carol:{CRYPT}" + sha512Crypt("pw3", "mysalt", 5000) + "\n" +
		":bad\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	l := NewLocalPasswordFile(path)
	ctx := context.Background()
	for _, c := range []struct {
		u, p string
		ok   bool
	}{
		{"alice", "secret", true}, {"alice", "nope", false},
		{"bob", "pw2", true}, {"bob", "x", false},
		{"carol", "pw3", true}, {"carol", "x", false},
		{"dave", "secret", false},
	} {
		id, err := l.Authenticate(ctx, c.u, c.p)
		if c.ok && (err != nil || id.Username != c.u) {
			t.Fatalf("authenticate(%s) failed: %v", c.u, err)
		}
		if !c.ok && err == nil {
			t.Fatalf("authenticate(%s) unexpectedly succeeded", c.u)
		}
	}
}

func TestChainFallback(t *testing.T) {
	ok := &staticAuth{user: "u1", by: "first"}
	chain := NewChain(ok)
	id, err := chain.Authenticate(context.Background(), "u1", "p")
	if err != nil || id.Username != "u1" || id.AuthenticatedBy != "first" {
		t.Fatalf("chain first: %v %+v", err, id)
	}
	// second backend used when first rejects
	chain2 := NewChain(staticAuth{}, staticAuth{user: "u2", by: "second"})
	id2, err := chain2.Authenticate(context.Background(), "u2", "p")
	if err != nil || id2.AuthenticatedBy != "second" {
		t.Fatalf("chain fallback: %v %+v", err, id2)
	}
	// all reject
	if _, err := NewChain(staticAuth{}, staticAuth{}).Authenticate(context.Background(), "x", "p"); err != ErrInvalidCredentials {
		t.Fatalf("expected ErrInvalidCredentials, got %v", err)
	}
}

type staticAuth struct {
	user string
	by   string
}

func (s staticAuth) Authenticate(_ context.Context, u, _ string) (Identity, error) {
	if s.user == "" || u != s.user {
		return Identity{}, ErrInvalidCredentials
	}
	return Identity{Username: u, AuthenticatedBy: s.by}, nil
}

// --- LDAP fakes ---

type fakeLdapConn struct {
	// userDNByFilter maps a search filter to the user DN to return.
	userDNByFilter map[string]string
	// serviceBindOK controls the service bind.
	serviceBindOK bool
	// userPassOK controls the final user bind.
	userPassOK bool
	lastBindDN string
}

func (f *fakeLdapConn) Bind(dn, password string) error {
	if dn == "cn=admin,dc=example" {
		if !f.serviceBindOK {
			return ldap.NewError(ldap.LDAPResultInvalidCredentials, nil)
		}
		return nil
	}
	if !f.userPassOK {
		return ldap.NewError(ldap.LDAPResultInvalidCredentials, nil)
	}
	f.lastBindDN = dn
	return nil
}

func (f *fakeLdapConn) Search(req *ldap.SearchRequest) (*ldap.SearchResult, error) {
	dn, ok := f.userDNByFilter[req.Filter]
	if !ok {
		return &ldap.SearchResult{}, nil
	}
	return &ldap.SearchResult{Entries: []*ldap.Entry{{DN: dn}}}, nil
}

func (f *fakeLdapConn) Close() error { return nil }

func TestLDAPSearchBind(t *testing.T) {
	a := NewLDAP(LDAPConfig{
		URL:         "ldap://localhost:1389",
		BaseDN:      "ou=people,dc=example",
		Filter:      "(uid={{username}})",
		ServiceBindDN: "cn=admin,dc=example",
	})
	fake := &fakeLdapConn{
		userDNByFilter: map[string]string{"(uid=alice)": "uid=alice,ou=people,dc=example"},
		serviceBindOK:  true,
		userPassOK:     true,
	}
	a.SetDialer(func() (LdapConn, error) { return fake, nil })
	id, err := a.Authenticate(context.Background(), "alice", "pw")
	if err != nil || id.Username != "alice" || fake.lastBindDN != "uid=alice,ou=people,dc=example" {
		t.Fatalf("search-bind: err=%v id=%+v lastBind=%s", err, id, fake.lastBindDN)
	}

	// wrong user password → invalid credentials
	fake.userPassOK = false
	if _, err := a.Authenticate(context.Background(), "alice", "bad"); err != ErrInvalidCredentials {
		t.Fatalf("expected invalid creds, got %v", err)
	}

	// unknown user (no search hit) → invalid creds
	fake.userPassOK = true
	if _, err := a.Authenticate(context.Background(), "ghost", "pw"); err != ErrInvalidCredentials {
		t.Fatalf("expected invalid creds for ghost, got %v", err)
	}
}

func TestLDAPDirectBind(t *testing.T) {
	a := NewLDAP(LDAPConfig{
		URL:          "ldaps://localhost:1636",
		DirectBindDN: "uid={{username}},ou=people,dc=example",
	})
	fake := &fakeLdapConn{userPassOK: true}
	a.SetDialer(func() (LdapConn, error) { return fake, nil })
	id, err := a.Authenticate(context.Background(), "bob", "pw")
	if err != nil || id.AuthenticatedBy != "ldap" || fake.lastBindDN != "uid=bob,ou=people,dc=example" {
		t.Fatalf("direct-bind: err=%v id=%+v lastBind=%s", err, id, fake.lastBindDN)
	}
	fake.userPassOK = false
	if _, err := a.Authenticate(context.Background(), "bob", "bad"); err != ErrInvalidCredentials {
		t.Fatalf("expected invalid creds, got %v", err)
	}
}
