package server

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"svngod/internal/auth"
	"svngod/internal/fs"
)

// Failed authentication needs a rate limit of its own.
//
// The server already audits "bad_credentials", and that audit exists because
// repeated failures are the signal for credential stuffing. But auditing is
// observation, not defence: nothing slowed an attacker down. Argon/bcrypt cost
// makes each attempt expensive for the server, and LDAP failures mean an
// outbound round trip, so a few hundred concurrent guesses tie up real resources
// while every other client waits behind them.
//
// Limits are per source address and per username, because neither alone is
// enough: a single address can spray many usernames, and one account can be
// attacked from many addresses. Whichever limit trips first applies.

// authTestServer builds a server with a password file and one repo.
func authTestServer(t *testing.T) *Server {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "passwd")
	if err := os.WriteFile(p, []byte("alice:"+shaPass("secret")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return New(Config{
		BasePath:     "/svn",
		Authenticate: auth.NewChain(auth.NewLocalPasswordFile(p)),
		OpenRepo:     func(name string) (*fs.Repo, error) { return nil, fs.ErrRepoNotFound },
	})
}

// basicAuth builds an Authorization header value.
func basicAuth(user, pass string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
}

// attempt sends one request with the given credentials and returns the status.
func attempt(s *Server, user, pass string) int {
	req := httptest.NewRequest("OPTIONS", "/svn/anything", nil)
	req.Header.Set("Authorization", basicAuth(user, pass))
	req.RemoteAddr = "192.0.2.10:1234"
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec.Code
}

// TestFailedAuthIsRateLimited is the core regression: pre-fix, wrong passwords
// are answered 401 forever with no slowdown.
func TestFailedAuthIsRateLimited(t *testing.T) {
	s := authTestServer(t)
	s.cfg.AuthFailureBurst = 3

	codes := make([]int, 0, 6)
	for i := 0; i < 6; i++ {
		codes = append(codes, attempt(s, "alice", "wrong"))
	}
	// The first burst is answered normally (401); after that the limiter must
	// refuse without consulting the auth backend at all.
	tripped := false
	for _, c := range codes {
		if c == http.StatusTooManyRequests {
			tripped = true
			break
		}
	}
	if !tripped {
		t.Fatalf("6 failed logins produced only 401s, no 429: %v", codes)
	}
}

// TestRateLimitedResponseCarriesRetryAfter keeps the refusal actionable: a
// client (or a script) needs to know when to come back.
func TestRateLimitedResponseCarriesRetryAfter(t *testing.T) {
	s := authTestServer(t)
	s.cfg.AuthFailureBurst = 1
	for i := 0; i < 5; i++ {
		attempt(s, "alice", "wrong")
	}
	req := httptest.NewRequest("OPTIONS", "/svn/anything", nil)
	req.Header.Set("Authorization", basicAuth("alice", "wrong"))
	req.RemoteAddr = "192.0.2.10:1234"
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 after the burst, got %d", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("429 with no Retry-After: the client cannot know when to retry")
	}
}

// TestCorrectPasswordStillWorksWithinBurst is the false-positive guard: the
// limit must count failures, not attempts. A busy legitimate client that
// authenticates successfully every time must never be throttled.
func TestCorrectPasswordStillWorksWithinBurst(t *testing.T) {
	s := authTestServer(t)
	s.cfg.AuthFailureBurst = 2

	for i := 0; i < 20; i++ {
		if code := attempt(s, "alice", "secret"); code == http.StatusTooManyRequests {
			t.Fatalf("successful authentication throttled on attempt %d", i+1)
		}
	}
}

// TestSuccessDoesNotResetAnotherUsersCounter: counters are per identity, so a
// success by one user must not clear another user's failures.
//
// Each user gets its own source address here. Sharing one would let the address
// dimension mask the bug: if a success cleared only the address key, the other
// user's own key would still hold its count and the test would pass without
// checking what it claims to.
func TestSuccessDoesNotResetAnotherUsersCounter(t *testing.T) {
	s := authTestServer(t)
	s.cfg.AuthFailureBurst = 2

	failFrom := func(user, addr string) int {
		req := httptest.NewRequest("OPTIONS", "/svn/anything", nil)
		req.Header.Set("Authorization", basicAuth(user, "wrong"))
		req.RemoteAddr = addr
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		return rec.Code
	}
	okFrom := func(user, addr string) int {
		req := httptest.NewRequest("OPTIONS", "/svn/anything", nil)
		req.Header.Set("Authorization", basicAuth(user, "secret"))
		req.RemoteAddr = addr
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		return rec.Code
	}

	for i := 0; i < 3; i++ {
		failFrom("mallory", "203.0.113.5:1111")
	}
	// alice succeeds from her own address. That must not touch mallory's budget.
	if code := okFrom("alice", "203.0.113.6:2222"); code == http.StatusTooManyRequests {
		t.Fatal("alice throttled by mallory's failures")
	}
	if code := failFrom("mallory", "203.0.113.5:1111"); code != http.StatusTooManyRequests {
		t.Fatalf("mallory's counter was cleared by alice's success: got %d", code)
	}
}

// TestUnknownUserIsAlsoLimited: an attacker enumerating names must hit the same
// wall. A limiter keyed only on known accounts would be trivially bypassed.
func TestUnknownUserIsAlsoLimited(t *testing.T) {
	s := authTestServer(t)
	s.cfg.AuthFailureBurst = 2

	// Enumeration shows up on the address counter, not the user counter: every
	// name is a fresh user key, so only the shared address accumulates. The
	// address budget is 10x the user budget, hence the larger loop.
	tripped := false
	for i := 0; i < 60; i++ {
		if attempt(s, "nosuchuser"+strconv.Itoa(i), "x") == http.StatusTooManyRequests {
			tripped = true
			break
		}
	}
	if !tripped {
		t.Fatal("unknown usernames were never throttled")
	}
}

// TestLimitIsPerSourceAddress: one noisy address must not lock out everyone
// else. That would turn a defence into a denial of service.
func TestLimitIsPerSourceAddress(t *testing.T) {
	s := authTestServer(t)
	s.cfg.AuthFailureBurst = 2

	// The noisy address hammers a *different* username each time, so only the
	// address counter accumulates. (Repeating one username here would trip the
	// per-user budget instead, and that budget correctly follows the account
	// across addresses - see TestAccountLockedAcrossAddresses.)
	for i := 0; i < 25; i++ {
		req := httptest.NewRequest("OPTIONS", "/svn/anything", nil)
		req.Header.Set("Authorization", basicAuth("noise"+strconv.Itoa(i), "wrong"))
		req.RemoteAddr = "192.0.2.10:1234"
		s.Handler().ServeHTTP(httptest.NewRecorder(), req)
	}

	// A different address with the correct password must still get through.
	req := httptest.NewRequest("OPTIONS", "/svn/anything", nil)
	req.Header.Set("Authorization", basicAuth("alice", "secret"))
	req.RemoteAddr = "198.51.100.7:5678"
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code == http.StatusTooManyRequests {
		t.Fatal("a second address was throttled by the first address's failures")
	}
}

// TestAccountLockedAcrossAddresses is the counterpart: when one account is under
// attack, moving to a new source address must not reset its budget. Otherwise an
// attacker with a handful of addresses bypasses the per-user limit entirely.
func TestAccountLockedAcrossAddresses(t *testing.T) {
	s := authTestServer(t)
	s.cfg.AuthFailureBurst = 2

	for i := 0; i < 3; i++ {
		req := httptest.NewRequest("OPTIONS", "/svn/anything", nil)
		req.Header.Set("Authorization", basicAuth("alice", "wrong"))
		req.RemoteAddr = "192.0.2.10:1234"
		s.Handler().ServeHTTP(httptest.NewRecorder(), req)
	}
	// Same account, fresh address: still blocked, because the budget is on the
	// account, not the connection.
	req := httptest.NewRequest("OPTIONS", "/svn/anything", nil)
	req.Header.Set("Authorization", basicAuth("alice", "wrong"))
	req.RemoteAddr = "198.51.100.99:9999"
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("a new source address reset the account's failure budget: got %d", rec.Code)
	}
}

// TestNegativeBurstDisables keeps the opt-out: -1 disables, which is what a
// deployment behind an authenticating proxy may want.
//
// 0 does NOT disable - it means "unset" and falls back to the default. That
// distinction exists because a JSON config omitting the field unmarshals to 0,
// and treating that as "off" would silently drop the limiter in exactly the
// deployments that configure everything else carefully.
func TestNegativeBurstDisables(t *testing.T) {
	s := authTestServer(t)
	s.cfg.AuthFailureBurst = -1

	for i := 0; i < 30; i++ {
		if code := attempt(s, "alice", "wrong"); code == http.StatusTooManyRequests {
			t.Fatalf("throttled on attempt %d with the limiter disabled", i+1)
		}
	}
}

// TestZeroBurstUsesDefault is the counterpart: unset must not mean off.
func TestZeroBurstUsesDefault(t *testing.T) {
	s := authTestServer(t)
	s.cfg.AuthFailureBurst = 0 // unset

	tripped := false
	for i := 0; i < defaultUserFailureBurst+5; i++ {
		if attempt(s, "alice", "wrong") == http.StatusTooManyRequests {
			tripped = true
			break
		}
	}
	if !tripped {
		t.Fatal("an unset burst disabled the limiter instead of applying the default")
	}
}

// TestLimitRecoversAfterWindow proves the limiter is not a permanent ban: after
// waiting, the same client must be able to authenticate again. A limiter that
// never forgets is an outage waiting for one fat-fingered password.
func TestLimitRecoversAfterWindow(t *testing.T) {
	s := authTestServer(t)
	s.cfg.AuthFailureBurst = 1
	s.cfg.AuthFailureWindow = 200 * time.Millisecond

	for i := 0; i < 4; i++ {
		attempt(s, "alice", "wrong")
	}
	if code := attempt(s, "alice", "wrong"); code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 before the window elapsed, got %d", code)
	}
	time.Sleep(250 * time.Millisecond)
	if code := attempt(s, "alice", "secret"); code == http.StatusTooManyRequests {
		t.Fatal("limiter did not recover after the window elapsed")
	}
}
