package server

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// lockEntry is one exclusive write lock on a repository path.
type lockEntry struct {
	token   string // urn:uuid:<hex>
	owner   string // authenticated principal
	comment string // free-form text from the LOCK request's <D:owner>
	depth   string
	created time.Time
	timeout int // seconds
}

// lockKey names the lock bucket for a repo path.
func lockKey(repo, path string) string {
	return repo + "\x00" + path
}

// handleLockUnlock implements the LOCK and UNLOCK methods.
func (s *Server) handleLockUnlock(w http.ResponseWriter, r *http.Request, res resource) {
	switch r.Method {
	case MethodLock:
		s.handleLock(w, r, res)
	case MethodUnlock:
		s.handleUnlock(w, r, res)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleLock(w http.ResponseWriter, r *http.Request, res resource) {
	// Locking requires an authenticated principal when the repo has auth
	// configured; otherwise the lockowner would be meaningless and anonymous
	// callers could lock any path. Challenge (401) so the svn client retries
	// with credentials, then the owner is the real user.
	if s.cfg.authFor(res.repo) != nil && IdentityFrom(r.Context()).Username == "" {
		s.unauthorized(w)
		return
	}
	// The lock owner must be the authenticated principal, never the client's
	// <D:owner> (svn lock -m puts the comment there, and LOCK may arrive
	// without credentials / challenge). Anonymous -> "anonymous".
	owner := "anonymous"
	if id := IdentityFrom(r.Context()); id.Username != "" {
		owner = id.Username
	}
	path := res.path
	if path == "" {
		path = "/"
	}
	// svn puts the user's "-m" lock comment in <D:owner>; the authenticated
	// principal is the lock owner. Keep them separate: folding the username
	// into the comment field (and dropping the comment) makes clients show
	// "(null)" as the owner and the username as the comment.
	comment := parseLockOwner(r.Body)
	key := lockKey(res.repo, path)

	s.mu.Lock()
	existing, isLocked := s.locks[key]
	if isLocked && !lockTokenMatches(r.Header.Get("If"), existing.token) {
		// The lock is held by someone else and no If header names its token:
		// refuse unless this is a refresh of the same token (handled below).
		s.mu.Unlock()
		w.WriteHeader(http.StatusLocked)
		return
	}
	// Refresh with an existing token, or create a new lock.
	var tok string
	if isLocked && lockTokenMatches(r.Header.Get("If"), existing.token) {
		existing.created = time.Now()
		s.mu.Unlock()
		s.writeLockDiscovery(w, res, existing)
		return
	}
	tok = newLockToken()
	entry := &lockEntry{token: tok, owner: owner, comment: comment, depth: "infinity", created: time.Now(), timeout: 3600}
	s.locks[key] = entry
	s.mu.Unlock()

	w.Header().Set("Lock-Token", "<"+tok+">")
	s.writeLockDiscovery(w, res, entry)
}

func (s *Server) handleUnlock(w http.ResponseWriter, r *http.Request, res resource) {
	if s.cfg.authFor(res.repo) != nil && IdentityFrom(r.Context()).Username == "" {
		s.unauthorized(w)
		return
	}
	path := res.path
	if path == "" {
		path = "/"
	}
	key := lockKey(res.repo, path)
	tok := strings.TrimSpace(r.Header.Get("Lock-Token"))
	tok = strings.Trim(tok, "<>")

	s.mu.Lock()
	entry, ok := s.locks[key]
	if !ok {
		s.mu.Unlock()
		http.Error(w, "no lock on resource", http.StatusConflict)
		return
	}
	if entry.token != tok {
		s.mu.Unlock()
		http.Error(w, "lock token mismatch", http.StatusConflict)
		return
	}
	delete(s.locks, key)
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func parseLockOwner(body io.Reader) string {
	b, err := io.ReadAll(io.LimitReader(body, 1<<20))
	if err != nil {
		return ""
	}
	var li struct {
		Owner string `xml:"owner"`
	}
	// owner may be element with text or nested; take raw inner text of <D:owner>
	if err := xml.Unmarshal(b, &li); err != nil {
		return ""
	}
	return strings.TrimSpace(li.Owner)
}

func newLockToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return "urn:uuid:" + hex.EncodeToString(b)
}

// lockTokenMatches reports whether the If header references the given token.
func lockTokenMatches(ifHeader, token string) bool {
	return strings.Contains(ifHeader, token)
}

// writeLockDiscovery writes a 200 prop response with a lockdiscovery block.
func (s *Server) writeLockDiscovery(w http.ResponseWriter, res resource, e *lockEntry) {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
	b.WriteString(`<D:prop xmlns:D="` + NS_DAV + `">` + "\n")
	b.WriteString(`  <D:lockdiscovery>` + "\n")
	b.WriteString(`    <D:activelock>` + "\n")
	b.WriteString(`      <D:locktype><D:write/></D:locktype>` + "\n")
	b.WriteString(`      <D:lockscope><D:exclusive/></D:lockscope>` + "\n")
	b.WriteString(`      <D:depth>` + e.depth + `</D:depth>` + "\n")
	if e.comment != "" {
		b.WriteString(`      <D:owner>` + xmlEscape(e.comment) + `</D:owner>` + "\n")
	}
	if e.owner != "" {
		b.WriteString(`      <S:owner>` + xmlEscape(e.owner) + `</S:owner>` + "\n")
		b.WriteString(`      <S:lockowner>` + xmlEscape(e.owner) + `</S:lockowner>` + "\n")
	}
	b.WriteString(fmt.Sprintf(`      <D:timeout>Second-%d</D:timeout>`+"\n", e.timeout))
	b.WriteString(`      <D:locktoken><D:href>` + e.token + `</D:href></D:locktoken>` + "\n")
	b.WriteString(`    </D:activelock>` + "\n")
	b.WriteString(`  </D:lockdiscovery>` + "\n")
	b.WriteString(`</D:prop>` + "\n")
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, b.String())
}

// getLock returns the lock on a repo path, if any.
func (s *Server) getLock(repo, path string) *lockEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.locks[lockKey(repo, path)]
}

// writeActivelock appends a DAV lockdiscovery/activelock block for a lock.
func writeActivelock(b *strings.Builder, e *lockEntry) {
	b.WriteString("      <D:lockdiscovery>\n")
	b.WriteString("        <D:activelock>\n")
	b.WriteString("          <D:locktype><D:write/></D:locktype>\n")
	b.WriteString("          <D:lockscope><D:exclusive/></D:lockscope>\n")
	b.WriteString("          <D:depth>" + e.depth + "</D:depth>\n")
	if e.comment != "" {
		// DAV:owner carries the client-supplied lock comment: svn puts the
		// "-m" text there and reads it back as "Lock Comment".
		b.WriteString("          <D:owner>" + xmlEscape(e.comment) + "</D:owner>\n")
	}
	if e.owner != "" {
		b.WriteString("          <S:owner>" + xmlEscape(e.owner) + "</S:owner>\n")
		b.WriteString("          <S:lockowner>" + xmlEscape(e.owner) + "</S:lockowner>\n")
	}
	b.WriteString(fmt.Sprintf("          <D:timeout>Second-%d</D:timeout>\n", e.timeout))
	b.WriteString("          <D:locktoken><D:href>" + e.token + "</D:href></D:locktoken>\n")
	b.WriteString("        </D:activelock>\n")
	b.WriteString("      </D:lockdiscovery>\n")
}
