package server

import (
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Auth failure throttle.
//
// Failed logins were audited but not slowed. That matters because a failed
// attempt is not free: a local password file runs an iterated SHA-512crypt or
// bcrypt hash, and an LDAP backend costs an outbound round trip (or two, in
// search+bind mode). A few hundred concurrent guesses therefore consume real CPU
// and connection slots while legitimate clients queue behind them.
//
// Two dimensions are tracked because either alone is trivially bypassed:
//
//   - per source address: one host spraying many usernames
//   - per username: one account attacked from many addresses
//
// Whichever trips first applies. Only failures are counted - a client that
// authenticates successfully is never throttled, however busy it is. A success
// clears that identity's own counter and nothing else.
//
// The window is a fixed bucket rather than a rolling one: cheaper, and precise
// ordering does not matter for this. When the window expires the count resets,
// so a limiter is a slowdown, never a permanent ban.

const (
	// The per-username budget is the tight one: guessing one account's password
	// is the attack this exists for, and a legitimate user rarely fails ten times.
	defaultUserFailureBurst = 10
	// The per-address budget is deliberately much looser. Addresses are shared:
	// an office NAT, a CI runner pool, or a reverse proxy in front of the server
	// presents one address for many people. A tight address limit would let one
	// person's typos lock out everyone behind the same egress - turning a defence
	// into a denial of service. This only stops one host hammering many *different*
	// usernames at absurd rates.
	defaultAddrFailureBurst  = 100
	defaultAuthFailureWindow = time.Minute
	// maxThrottleEntries bounds memory. An attacker rotating source addresses
	// must not grow this map without limit - that would trade a CPU problem for a
	// memory one.
	maxThrottleEntries = 20000
)

// authThrottle tracks recent authentication failures.
type authThrottle struct {
	mu      sync.Mutex
	entries map[string]*throttleEntry
	now     func() time.Time // injectable for tests
}

type throttleEntry struct {
	count int
	reset time.Time
}

func newAuthThrottle() *authThrottle {
	return &authThrottle{entries: map[string]*throttleEntry{}, now: time.Now}
}

// throttled reports whether key has exhausted its budget, and how long until the
// window resets. Calling it does not consume budget.
func (a *authThrottle) throttled(key string, burst int, window time.Duration) (bool, time.Duration) {
	if burst <= 0 || key == "" {
		return false, 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	e, ok := a.entries[key]
	if !ok {
		return false, 0
	}
	now := a.now()
	if !now.Before(e.reset) {
		// Window elapsed: forget the entry rather than keep a stale count.
		delete(a.entries, key)
		return false, 0
	}
	if e.count < burst {
		return false, 0
	}
	return true, e.reset.Sub(now)
}

// fail records one failure for key.
func (a *authThrottle) fail(key string, window time.Duration) {
	if key == "" {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.now()
	e, ok := a.entries[key]
	if !ok || !now.Before(e.reset) {
		// Evict when the table is full so a rotating attacker cannot grow it
		// without bound. Dropping entries under pressure can only make the
		// limiter more permissive, never lock anyone out.
		if len(a.entries) >= maxThrottleEntries {
			for k, v := range a.entries {
				if !now.Before(v.reset) {
					delete(a.entries, k)
				}
			}
			if len(a.entries) >= maxThrottleEntries {
				// Still full of live entries: clear the oldest window wholesale.
				// A fresh map keeps memory bounded; the cost is a brief reset of
				// everyone's budget during a sustained flood.
				a.entries = map[string]*throttleEntry{}
			}
		}
		a.entries[key] = &throttleEntry{count: 1, reset: now.Add(window)}
		return
	}
	e.count++
}

// succeed clears key's counter: a correct password ends that identity's streak.
func (a *authThrottle) succeed(key string) {
	if key == "" {
		return
	}
	a.mu.Lock()
	delete(a.entries, key)
	a.mu.Unlock()
}

// authFailBurstFor returns the budget for one key. The configured
// AuthFailureBurst is the per-username budget; the address budget is that scaled
// up, because addresses are shared by many users. A configured 0 disables.
func (s *Server) authFailBurstFor(key string) int {
	base := s.cfg.AuthFailureBurst
	if base < 0 {
		return 0 // explicitly disabled
	}
	if base == 0 {
		base = defaultUserFailureBurst // unset: apply the default
	}
	if strings.HasPrefix(key, "ip:") {
		return base * 10
	}
	return base
}

func (s *Server) authFailWindow() time.Duration {
	if s.cfg.AuthFailureWindow > 0 {
		return s.cfg.AuthFailureWindow
	}
	return defaultAuthFailureWindow
}

// authThrottleKeys returns the keys whose budget applies to this attempt.
func authThrottleKeys(r *http.Request, user string) []string {
	keys := make([]string, 0, 2)
	if host := remoteHost(r); host != "" {
		keys = append(keys, "ip:"+host)
	}
	if user != "" {
		keys = append(keys, "user:"+user)
	}
	return keys
}

// remoteHost extracts the peer address.
//
// X-Forwarded-For is deliberately NOT trusted: it is attacker-controlled unless a
// proxy overwrites it, and trusting it would let a client pick a fresh identity
// per attempt and defeat the limit entirely. Deployments behind a proxy that
// needs per-client limiting should enforce it there.
func remoteHost(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// throttledAuthResponse refuses an attempt without consulting the auth backend.
func (s *Server) throttledAuthResponse(w http.ResponseWriter, wait time.Duration) {
	// Round up to at least one second: Retry-After: 0 invites an immediate retry.
	secs := int(wait.Seconds())
	if secs < 1 {
		secs = 1
	}
	w.Header().Set("Retry-After", strconv.Itoa(secs))
	http.Error(w, "too many failed authentication attempts", http.StatusTooManyRequests)
}
