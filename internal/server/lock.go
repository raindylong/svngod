package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"svngod/internal/fs"
	"svngod/internal/fs/store"
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
	// Opening the repo also restores persisted locks for this repository.
	repo, ok := s.repo(w, res)
	if !ok {
		return
	}
	switch r.Method {
	case MethodLock:
		s.handleLock(w, r, res, repo)
	case MethodUnlock:
		s.handleUnlock(w, r, res, repo)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleLock(w http.ResponseWriter, r *http.Request, res resource, repo *fs.Repo) {
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
		s.persistLocks(res.repo, repo)
		s.writeLockDiscovery(w, res, existing)
		return
	}
	tok = newLockToken()
	entry := &lockEntry{token: tok, owner: owner, comment: comment, depth: "infinity", created: time.Now(), timeout: 3600}
	s.locks[key] = entry
	s.mu.Unlock()
	s.persistLocks(res.repo, repo)

	w.Header().Set("Lock-Token", "<"+tok+">")
	s.writeLockDiscovery(w, res, entry)
}

func (s *Server) handleUnlock(w http.ResponseWriter, r *http.Request, res resource, repo *fs.Repo) {
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
	s.persistLocks(res.repo, repo)
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
	// svn reads the lock owner and creation time from these headers (the
	// DAV:owner element carries the client's comment, not the username).
	w.Header().Set("X-SVN-Lock-Owner", e.owner)
	w.Header().Set("X-SVN-Creation-Date", e.created.UTC().Format("2006-01-02T15:04:05.000000Z"))
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

// expired reports whether the lock's timeout has elapsed.
func (e *lockEntry) expired() bool {
	return e.timeout > 0 && time.Since(e.created) > time.Duration(e.timeout)*time.Second
}

// getLock returns the lock on a repo path, if any. Expired locks are removed
// lazily so a leftover entry cannot block commits forever.
func (s *Server) getLock(repo, path string) *lockEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getLockLocked(repo, path)
}

// getLockLocked is getLock for callers that already hold s.mu (commit
// handlers hold it while validating a transaction).
func (s *Server) getLockLocked(repo, path string) *lockEntry {
	key := lockKey(repo, path)
	e, ok := s.locks[key]
	if !ok {
		return nil
	}
	if e.expired() {
		delete(s.locks, key)
		return nil
	}
	return e
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

// --- lock persistence -------------------------------------------------------

// lockedJSON is the on-disk form of a lock bucket entry.
type lockedJSON struct {
	Path    string `json:"path"`
	Token   string `json:"token"`
	Owner   string `json:"owner"`
	Comment string `json:"comment,omitempty"`
	Depth   string `json:"depth"`
	Created int64  `json:"created"`
	Timeout int    `json:"timeout"`
}

func lockStateKey(repo *fs.Repo) string {
	return repo.Keys().Prefix + "/dav-locks/locks.json"
}

// persistLocks writes this repository's lock bucket to its object store.
// Best effort: a failed write must not fail the LOCK/UNLOCK request.
func (s *Server) persistLocks(repoName string, repo *fs.Repo) {
	s.mu.Lock()
	prefix := repoName + "\x00"
	out := []lockedJSON{}
	for key, e := range s.locks {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		out = append(out, lockedJSON{
			Path: key[len(prefix):], Token: e.token, Owner: e.owner,
			Comment: e.comment, Depth: e.depth,
			Created: e.created.Unix(), Timeout: e.timeout,
		})
	}
	s.mu.Unlock()
	data, err := json.Marshal(out)
	if err != nil {
		return
	}
	if err := repo.Store().Put(context.Background(), lockStateKey(repo), data); err != nil {
		log.Printf("locks: persist %q: %v", repoName, err)
	}
}

// ensureLocksLoaded restores a repository's locks after a restart (or on
// another replica's first request for it). Locks already in memory win.
func (s *Server) ensureLocksLoaded(repoName string, repo *fs.Repo) {
	s.mu.Lock()
	if s.locksLoaded[repoName] {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	data, err := repo.Store().Get(context.Background(), lockStateKey(repo), nil)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		log.Printf("locks: load %q: %v", repoName, err)
	}
	var loaded []lockedJSON
	if len(data) > 0 {
		if err := json.Unmarshal(data, &loaded); err != nil {
			log.Printf("locks: parse %q: %v", repoName, err)
			loaded = nil
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, l := range loaded {
		key := lockKey(repoName, l.Path)
		if _, exists := s.locks[key]; exists {
			continue
		}
		e := &lockEntry{token: l.Token, owner: l.Owner, comment: l.Comment,
			depth: l.Depth, created: time.Unix(l.Created, 0), timeout: l.Timeout}
		if e.expired() {
			continue
		}
		if e.depth == "" {
			e.depth = "infinity"
		}
		s.locks[key] = e
	}
	s.locksLoaded[repoName] = true
}
