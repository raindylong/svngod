package server

import (
	"bytes"
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
	// callers could lock any path.
	if s.cfg.authFor(res.repo) != nil && IdentityFrom(r.Context()).Username == "" {
		s.unauthorized(w)
		return
	}
	owner := "anonymous"
	if id := IdentityFrom(r.Context()); id.Username != "" {
		owner = id.Username
	}
	path := res.path
	if path == "" {
		path = "/"
	}
	comment := parseLockOwner(r.Body)
	ifHeader := r.Header.Get("If")

	out, err := s.mutateLocks(res.repo, repo, func(list []lockedJSON) ([]lockedJSON, interface{}, error) {
		idx := -1
		for i := range list {
			if list[i].Path == path {
				idx = i
				break
			}
		}
		if idx >= 0 {
			e := lockEntryFromJSON(list[idx])
			if e.expired() {
				list = append(list[:idx], list[idx+1:]...)
				idx = -1
			} else if lockTokenMatches(ifHeader, list[idx].Token) {
				list[idx].Created = time.Now().Unix()
				return list, lockEntryFromJSON(list[idx]), nil
			} else {
				return nil, nil, errLockHeld
			}
		}
		now := time.Now()
		e := &lockEntry{token: newLockToken(), owner: owner, comment: comment,
			depth: "infinity", created: now, timeout: 3600}
		list = append(list, lockedJSON{Path: path, Token: e.token, Owner: owner,
			Comment: comment, Depth: e.depth, Created: now.Unix(), Timeout: e.timeout})
		return list, e, nil
	})
	if err != nil {
		if errors.Is(err, errLockHeld) {
			w.WriteHeader(http.StatusLocked)
			return
		}
		log.Printf("locks: lock %s %s: %v", res.repo, path, err)
		http.Error(w, "lock failed", http.StatusInternalServerError)
		return
	}
	entry := out.(*lockEntry)
	w.Header().Set("Lock-Token", "<"+entry.token+">")
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
	tok := strings.TrimSpace(r.Header.Get("Lock-Token"))
	tok = strings.Trim(tok, "<>")

	_, err := s.mutateLocks(res.repo, repo, func(list []lockedJSON) ([]lockedJSON, interface{}, error) {
		idx := -1
		for i := range list {
			if list[i].Path == path {
				idx = i
				break
			}
		}
		if idx < 0 {
			return nil, nil, errNoLock
		}
		if list[idx].Token != tok {
			return nil, nil, errLockTokenMismatch
		}
		list = append(list[:idx], list[idx+1:]...)
		return list, nil, nil
	})
	if err != nil {
		if errors.Is(err, errNoLock) || errors.Is(err, errLockTokenMismatch) {
			if errors.Is(err, errNoLock) {
				http.Error(w, "no lock on resource", http.StatusConflict)
			} else {
				http.Error(w, "lock token mismatch", http.StatusConflict)
			}
			return
		}
		log.Printf("locks: unlock %s %s: %v", res.repo, path, err)
		http.Error(w, "unlock failed", http.StatusInternalServerError)
		return
	}
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
	b.WriteString(`<D:prop xmlns:D="` + NS_DAV + `" xmlns:S="` + NS_DAVSVN + `">` + "\n")
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

// lockForPathLocked returns the path and entry of the lock that covers path:
// the path's own lock, or the nearest ancestor locked with DAV depth infinity.
// Callers must hold s.mu.
func (s *Server) lockForPathLocked(repoName, path string) (string, *lockEntry) {
	if e := s.getLockLocked(repoName, path); e != nil {
		return path, e
	}
	for p := parentDAVPath(path); p != ""; p = parentDAVPath(p) {
		if e := s.getLockLocked(repoName, p); e != nil && e.depth == "infinity" {
			return p, e
		}
	}
	return "", nil
}

// parentDAVPath returns the parent of a leading-slash DAV path ("" above the
// repository root).
func parentDAVPath(p string) string {
	p = strings.TrimSuffix(p, "/")
	if p == "" {
		return ""
	}
	if i := strings.LastIndex(p, "/"); i > 0 {
		return p[:i]
	}
	return ""
}

// mergeLock is one <S:lock> entry of a MERGE lock-token-list. The client lists
// every lock it used, so the server can validate writes and release them when
// the commit succeeds (X-SVN-Options: release-locks).
type mergeLock struct {
	Path  string
	Token string
}

// parseMergeLocks extracts the lock-token-list from a MERGE body. Local names
// are matched, so any prefix binding works.
func parseMergeLocks(body []byte) []mergeLock {
	dec := xml.NewDecoder(bytes.NewReader(body))
	var out []mergeLock
	inLock := false
	var cur mergeLock
	for {
		tok, err := dec.Token()
		if err != nil {
			break
		}
		switch t := tok.(type) {
		case xml.StartElement:
			switch t.Name.Local {
			case "lock":
				inLock, cur = true, mergeLock{}
			case "lock-path":
				if inLock {
					var s string
					if dec.DecodeElement(&s, &t) == nil {
						cur.Path = strings.TrimSpace(s)
					}
				}
			case "lock-token":
				if inLock {
					var s string
					if dec.DecodeElement(&s, &t) == nil {
						cur.Token = strings.TrimSpace(s)
					}
				}
			}
		case xml.EndElement:
			if t.Name.Local == "lock" && inLock {
				inLock = false
				if cur.Token != "" {
					out = append(out, cur)
				}
			}
		}
	}
	return out
}

// mergeLockAbsPath resolves a MERGE lock-path. svn sends these relative to the
// MERGE target, which is the shallowest modified path (repo root, a subdir, or
// the VCC), not necessarily the repository root.
func mergeLockAbsPath(target, p string) string {
	p = strings.TrimSpace(p)
	if strings.HasPrefix(p, "/") {
		return "/" + strings.Trim(p, "/")
	}
	t := "/" + strings.Trim(target, "/")
	rel := strings.Trim(p, "/")
	switch {
	case rel == "":
		return t
	case t == "/":
		return "/" + rel
	default:
		return t + "/" + rel
	}
}

// mergeLockCovers reports whether the MERGE body listed the lock on lockPath
// with the given token.
func mergeLockCovers(locks []mergeLock, target, lockPath, token string) bool {
	want := "/" + strings.Trim(lockPath, "/")
	for _, l := range locks {
		if l.Token != token {
			continue
		}
		if mergeLockAbsPath(target, l.Path) == want {
			return true
		}
	}
	return false
}

// releaseCommittedLocks drops the locks a successful commit used, mirroring
// mod_dav_svn's X-SVN-Options: release-locks behaviour. Commits made with
// --no-unlock do not send that header and keep their locks.
func (s *Server) releaseCommittedLocks(repoName string, repo *fs.Repo, target string, locks []mergeLock) {
	if len(locks) == 0 {
		return
	}
	want := make(map[string]string, len(locks))
	for _, l := range locks {
		want[mergeLockAbsPath(target, l.Path)] = l.Token
	}
	if _, err := s.mutateLocks(repoName, repo, func(list []lockedJSON) ([]lockedJSON, interface{}, error) {
		kept := list[:0]
		for _, l := range list {
			if tok, ok := want[l.Path]; ok && tok == l.Token {
				continue
			}
			kept = append(kept, l)
		}
		return kept, nil, nil
	}); err != nil {
		log.Printf("locks: release after commit %q: %v", repoName, err)
	}
}

// blockingLock returns the lock that must prevent a write carrying ifHeader,
// or nil when the write may proceed. When the locally cached lock is stale
// (another replica released or replaced it), the shared table is reloaded once
// before the write is rejected: otherwise an unlock on replica A would keep
// blocking commits on replica B until its next MERGE.
func (s *Server) blockingLock(repoName string, repo *fs.Repo, path, ifHeader string) *lockEntry {
	s.mu.Lock()
	_, e := s.lockForPathLocked(repoName, path)
	s.mu.Unlock()
	if e == nil || lockTokenMatches(ifHeader, e.token) {
		return nil
	}
	s.refreshLocks(repoName, repo)
	s.mu.Lock()
	defer s.mu.Unlock()
	_, e = s.lockForPathLocked(repoName, path)
	if e == nil || lockTokenMatches(ifHeader, e.token) {
		return nil
	}
	return e
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

var (
	errLockHeld          = errors.New("locks: path is locked")
	errNoLock            = errors.New("locks: no lock on resource")
	errLockTokenMismatch = errors.New("locks: token mismatch")
	errLockBusy          = errors.New("locks: concurrent update, retry")
)

func lockEntryFromJSON(l lockedJSON) *lockEntry {
	depth := l.Depth
	if depth == "" {
		depth = "infinity"
	}
	return &lockEntry{token: l.Token, owner: l.Owner, comment: l.Comment,
		depth: depth, created: time.Unix(l.Created, 0), timeout: l.Timeout}
}

// readLockState returns the persisted table and its content-hash ETag ("" when
// the object does not exist yet).
func readLockState(repo *fs.Repo) ([]lockedJSON, string, error) {
	b, err := repo.Store().Get(context.Background(), lockStateKey(repo), nil)
	if errors.Is(err, store.ErrNotFound) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	var list []lockedJSON
	if len(b) > 0 {
		if jerr := json.Unmarshal(b, &list); jerr != nil {
			return nil, "", jerr
		}
	}
	return list, store.ContentETag(b), nil
}

// setRepoLocks replaces one repository's in-memory lock bucket.
func (s *Server) setRepoLocks(repoName string, list []lockedJSON) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := repoName + "\x00"
	for k := range s.locks {
		if strings.HasPrefix(k, prefix) {
			delete(s.locks, k)
		}
	}
	for _, l := range list {
		e := lockEntryFromJSON(l)
		if e.expired() {
			continue
		}
		s.locks[lockKey(repoName, l.Path)] = e
	}
}

// refreshLocks reloads the shared table when another replica changed it.
func (s *Server) refreshLocks(repoName string, repo *fs.Repo) {
	s.lockOpsMu.Lock()
	defer s.lockOpsMu.Unlock()
	s.refreshLocksLocked(repoName, repo)
}

func (s *Server) refreshLocksLocked(repoName string, repo *fs.Repo) {
	list, etag, err := readLockState(repo)
	if err != nil {
		log.Printf("locks: refresh %q: %v", repoName, err)
		return
	}
	s.mu.Lock()
	cur := s.locksETag[repoName]
	s.mu.Unlock()
	if cur == etag {
		return
	}
	s.setRepoLocks(repoName, list)
	s.mu.Lock()
	s.locksETag[repoName] = etag
	s.mu.Unlock()
}

// mutateLocks applies fn to the shared lock table with a CAS retry loop, so
// two replicas locking different paths cannot drop each other's entries.
func (s *Server) mutateLocks(repoName string, repo *fs.Repo, fn func([]lockedJSON) ([]lockedJSON, interface{}, error)) (interface{}, error) {
	s.lockOpsMu.Lock()
	defer s.lockOpsMu.Unlock()
	ctx := context.Background()
	for attempt := 0; attempt < 8; attempt++ {
		list, etag, err := readLockState(repo)
		if err != nil {
			return nil, err
		}
		next, out, ferr := fn(list)
		if ferr != nil {
			return nil, ferr
		}
		// Drop expired entries while we are rewriting the table.
		kept := next[:0]
		for _, l := range next {
			if !lockEntryFromJSON(l).expired() {
				kept = append(kept, l)
			}
		}
		next = kept
		data, merr := json.Marshal(next)
		if merr != nil {
			return nil, merr
		}
		if etag == "" {
			err = repo.Store().CreateIfAbsent(ctx, lockStateKey(repo), data)
		} else {
			_, err = repo.Store().CAS(ctx, lockStateKey(repo), etag, data)
		}
		if err == nil {
			s.setRepoLocks(repoName, next)
			s.mu.Lock()
			s.locksETag[repoName] = store.ContentETag(data)
			s.mu.Unlock()
			return out, nil
		}
		if !errors.Is(err, store.ErrPreconditionFailed) {
			return nil, err
		}
	}
	return nil, errLockBusy
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

	loaded, etag, err := readLockState(repo)
	if err != nil {
		log.Printf("locks: load %q: %v", repoName, err)
		loaded = nil
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
	s.locksETag[repoName] = etag
	s.locksLoaded[repoName] = true
}
