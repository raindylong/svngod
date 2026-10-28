package auth

import (
	"context"
	"errors"
)

// ErrInvalidCredentials is returned when authentication fails.
var ErrInvalidCredentials = errors.New("auth: invalid credentials")

// Identity is a successfully authenticated principal.
type Identity struct {
	Username string
	// AuthenticatedBy names the backend that proved the credentials.
	AuthenticatedBy string
}

// Authenticator verifies a username/password pair.
type Authenticator interface {
	// Authenticate returns an Identity when credentials are valid, or
	// ErrInvalidCredentials otherwise.
	Authenticate(ctx context.Context, username, password string) (Identity, error)
}

// Chain tries authenticators in order; the first success wins. A backend that
// cannot be reached (e.g. LDAP down) is treated as a miss so the chain can
// fall through, but the final error reports the most severe failure.
type Chain struct {
	backends []Authenticator
}

// NewChain returns a Chain over the given backends.
func NewChain(backends ...Authenticator) *Chain {
	return &Chain{backends: backends}
}

// Authenticate tries each backend in order.
func (c *Chain) Authenticate(ctx context.Context, username, password string) (Identity, error) {
	var lastErr error
	for _, b := range c.backends {
		id, err := b.Authenticate(ctx, username, password)
		if err == nil {
			return id, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = ErrInvalidCredentials
	}
	return Identity{}, lastErr
}
