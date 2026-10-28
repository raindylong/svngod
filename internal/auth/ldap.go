package auth

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/go-ldap/ldap/v3"
)

// LdapConn is the minimal LDAP connection surface the authenticator needs.
// *ldap.Conn satisfies it; tests use a fake.
type LdapConn interface {
	Bind(username, password string) error
	Search(request *ldap.SearchRequest) (*ldap.SearchResult, error)
	Close() error
}

// LdapDialer opens a connection. Real usage: dial with ldap.DialURL + StartTLS.
type LdapDialer func() (LdapConn, error)

// LDAPConfig configures an OpenLDAP authenticator.
type LDAPConfig struct {
	// URL is "ldap://host:port" or "ldaps://host:port".
	URL string
	// StartTLS upgrades a plain connection with TLS.
	StartTLS bool
	// ServiceBindDN/ServiceBindPassword are used for the search+bind probe.
	// Leave empty to use anonymous search (often disallowed on LDAP).
	ServiceBindDN       string
	ServiceBindPassword string
	// BaseDN is the search base, e.g. "ou=people,dc=example,dc=com".
	BaseDN string
	// Filter locates the user; "{{username}}" is replaced and escaped,
	// e.g. "(uid={{username}})".
	Filter string
	// DirectBindDN is a DN template for direct bind mode (no search needed),
	// e.g. "uid={{username}},ou=people,dc=example,dc=com". When set, search
	// configuration is ignored.
	DirectBindDN string
	// Timeout bounds each dial.
	Timeout time.Duration
}

// LDAPAuthenticator authenticates against an OpenLDAP directory.
type LDAPAuthenticator struct {
	cfg    LDAPConfig
	dialer LdapDialer
}

// NewLDAP returns an LDAP authenticator with the default dialer.
func NewLDAP(cfg LDAPConfig) *LDAPAuthenticator {
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	return &LDAPAuthenticator{cfg: cfg, dialer: defaultDialer(cfg)}
}

// SetDialer overrides the connection factory (used by tests).
func (l *LDAPAuthenticator) SetDialer(d LdapDialer) { l.dialer = d }

func defaultDialer(cfg LDAPConfig) LdapDialer {
	return func() (LdapConn, error) {
		d := &net.Dialer{Timeout: cfg.Timeout}
		conn, err := ldap.DialURL(cfg.URL, ldap.DialWithDialer(d))
		if err != nil {
			return nil, err
		}
		if cfg.StartTLS {
			if err := conn.StartTLS(nil); err != nil {
				conn.Close()
				return nil, err
			}
		}
		return conn, nil
	}
}

// Authenticate resolves the user and verifies the password via bind.
func (l *LDAPAuthenticator) Authenticate(ctx context.Context, username, password string) (Identity, error) {
	if username == "" || password == "" {
		return Identity{}, ErrInvalidCredentials
	}
	if done := ctxDone(ctx); done {
		return Identity{}, ctx.Err()
	}
	conn, err := l.dialer()
	if err != nil {
		return Identity{}, fmt.Errorf("auth: ldap connect: %w", err)
	}
	defer conn.Close()

	if l.cfg.DirectBindDN != "" {
		dn := strings.ReplaceAll(l.cfg.DirectBindDN, "{{username}}", ldap.EscapeDN(username))
		if err := conn.Bind(dn, password); err != nil {
			return Identity{}, ErrInvalidCredentials
		}
		return Identity{Username: username, AuthenticatedBy: "ldap"}, nil
	}

	// search+bind: bind as service (or anonymous), locate the user DN, then
	// verify by binding as the user.
	if l.cfg.ServiceBindDN != "" {
		if err := conn.Bind(l.cfg.ServiceBindDN, l.cfg.ServiceBindPassword); err != nil {
			return Identity{}, fmt.Errorf("auth: ldap service bind: %w", err)
		}
	}
	filter := strings.ReplaceAll(l.cfg.Filter, "{{username}}", ldap.EscapeFilter(username))
	res, err := conn.Search(&ldap.SearchRequest{
		BaseDN:     l.cfg.BaseDN,
		Scope:      ldap.ScopeWholeSubtree,
		Filter:     filter,
		Attributes: []string{"dn"},
	})
	if err != nil {
		return Identity{}, fmt.Errorf("auth: ldap search: %w", err)
	}
	if len(res.Entries) != 1 {
		return Identity{}, ErrInvalidCredentials
	}
	userDN := res.Entries[0].DN
	if err := conn.Bind(userDN, password); err != nil {
		return Identity{}, ErrInvalidCredentials
	}
	return Identity{Username: username, AuthenticatedBy: "ldap"}, nil
}

func ctxDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}
