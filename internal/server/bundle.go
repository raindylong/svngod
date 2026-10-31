package server

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"svngod/internal/fs"
	"svngod/internal/fs/store"
)

// Bulk commit channel ("svngod bundle", negotiated by svngocli):
//
//	POST <repo-url>/!svngod/bundle
//	body: "SVNGB1\n" | 8-byte big-endian header length | JSON header | texts
//
// Every text follows its change in order. The whole bundle becomes one atomic
// revision through the normal CommitTxn (per-path out-of-date checks apply).
// A client-supplied token makes retries idempotent: after the first success
// the token maps to the revision, so a retried upload returns it instead of
// committing a second revision. One request for tens of thousands of files
// replaces svn's MKACTIVITY/CHECKOUT/PUT dance per file.
const bundleMagic = "SVNGB1\n"

const (
	maxBundleHeader  = 32 << 20
	maxBundleText    = 512 << 20
	maxBundleChanges = 500000
	bundleClaimTTL   = 5 * time.Minute
)

type bundleChange struct {
	Path    string            `json:"path"`
	Action  string            `json:"action"` // put | add | modify | delete
	Kind    string            `json:"kind"`   // file | dir
	BaseRev uint64            `json:"baseRev,omitempty"`
	Props   map[string]string `json:"props,omitempty"`
	TextLen int64             `json:"textLen,omitempty"`
}

type bundleHeader struct {
	Token      string         `json:"token"`
	BaseRev    uint64         `json:"baseRev"`
	Message    string         `json:"message"`
	LockTokens []string       `json:"lockTokens,omitempty"`
	Changes    []bundleChange `json:"changes"`
}

type bundleRecord struct {
	Rev     uint64 `json:"rev"`
	User    string `json:"user,omitempty"`
	Created int64  `json:"created,omitempty"`
}

func bundleKey(repo *fs.Repo, token string) string {
	return repo.Keys().Prefix + "/bundles/" + token
}

// validBundleToken keeps tokens filesystem/object-key safe.
func validBundleToken(tok string) bool {
	if len(tok) < 16 || len(tok) > 128 {
		return false
	}
	for _, c := range tok {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_':
		default:
			return false
		}
	}
	return true
}

// handleBundleCommit serves the negotiated bulk commit channel.
func (s *Server) handleBundleCommit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "bundle endpoint accepts POST", http.StatusMethodNotAllowed)
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, strings.TrimSuffix(s.cfg.BasePath, "/")+"/")
	name := strings.SplitN(rest, "/", 2)[0]
	if name == "" {
		http.Error(w, "repository missing", http.StatusBadRequest)
		return
	}
	repo, ok := s.repo(w, resource{repo: name})
	if !ok {
		return
	}
	ident, ok := s.authenticate(w, r, name)
	if !ok {
		return
	}
	user := ident.Username
	if user == "" && s.cfg.authFor(name) != nil {
		s.unauthorized(w)
		return
	}

	hdr, err := readBundleHeader(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !validBundleToken(hdr.Token) {
		http.Error(w, "invalid or missing token", http.StatusBadRequest)
		return
	}
	if len(hdr.Changes) == 0 || len(hdr.Changes) > maxBundleChanges {
		http.Error(w, "change count out of range", http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	key := bundleKey(repo, hdr.Token)

	// Replay after a retry: the body is drained so the client can finish
	// sending, then the recorded revision is returned.
	if rec, ok := loadBundleRecord(ctx, repo, key); ok {
		io.Copy(io.Discard, io.LimitReader(r.Body, maxBundleText))
		if rec.User != "" && rec.User != user {
			http.Error(w, "token belongs to another user", http.StatusConflict)
			return
		}
		writeBundleResult(w, rec.Rev, true)
		return
	}

	// Serialise concurrent uploads of the same token so only one commit can
	// happen. A crashed claim expires after bundleClaimTTL.
	claimKey := key + ".lock"
	claim := []byte(fmt.Sprintf("%s %d", user, time.Now().Unix()))
	claimErr := repo.Store().CreateIfAbsent(ctx, claimKey, claim)
	if claimErr != nil {
		if !errors.Is(claimErr, store.ErrPreconditionFailed) {
			http.Error(w, claimErr.Error(), http.StatusInternalServerError)
			return
		}
		if stale, ok := staleClaim(ctx, repo, claimKey, bundleClaimTTL); ok && stale {
			if err := repo.Store().Delete(ctx, []string{claimKey}); err == nil {
				claimErr = repo.Store().CreateIfAbsent(ctx, claimKey, claim)
			}
		}
	}
	if claimErr != nil {
		// Another upload is in flight: wait for its result briefly.
		io.Copy(io.Discard, io.LimitReader(r.Body, maxBundleText))
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			if rec, ok := loadBundleRecord(ctx, repo, key); ok {
				writeBundleResult(w, rec.Rev, true)
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		writeBundleError(w, http.StatusConflict, "in-progress", "another upload with this token is in progress")
		return
	}
	committed := false
	defer func() {
		if !committed {
			repo.Store().Delete(context.Background(), []string{claimKey})
		}
	}()

	head, err := repo.HeadFresh()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	base := hdr.BaseRev
	if base == 0 || base > head {
		base = head
	}

	az := s.cfg.authzFor(name)
	changes := make([]fs.Change, 0, len(hdr.Changes))
	var tempFiles []string
	defer func() {
		for _, f := range tempFiles {
			os.Remove(f)
		}
	}()

	var total int64
	for i := range hdr.Changes {
		bc := &hdr.Changes[i]
		path, perr := cleanBundlePath(bc.Path)
		if perr != nil {
			http.Error(w, perr.Error(), http.StatusBadRequest)
			return
		}
		if bc.TextLen < 0 || bc.TextLen > maxBundleText || total+bc.TextLen > maxBundleText {
			http.Error(w, "bundle text too large", http.StatusRequestEntityTooLarge)
			return
		}
		total += bc.TextLen

		if az != nil && !az.Authorize(user, name, path, true) {
			http.Error(w, "Access forbidden to "+path, http.StatusForbidden)
			return
		}
		if e := s.getLock(name, path); e != nil && !tokenInList(e.token, hdr.LockTokens) {
			http.Error(w, "Locked: "+path, http.StatusLocked)
			return
		}

		action, kind := bundleAction(bc, path, repo, head)
		ch := fs.Change{Path: path, Action: action, Kind: kind, Props: bc.Props, BaseRev: bc.BaseRev}
		if ch.BaseRev == 0 && action != fs.ActionAdd {
			ch.BaseRev = base
		}
		if bc.TextLen > 0 {
			if bc.TextLen <= 1<<20 {
				buf := make([]byte, bc.TextLen)
				if _, rerr := io.ReadFull(r.Body, buf); rerr != nil {
					http.Error(w, "short bundle text", http.StatusBadRequest)
					return
				}
				ch.Data = buf
			} else {
				tmp, terr := createTemp("bundle")
				if terr != nil {
					http.Error(w, terr.Error(), http.StatusInternalServerError)
					return
				}
				if _, cerr := io.CopyN(tmp, r.Body, bc.TextLen); cerr != nil {
					tmp.Close()
					os.Remove(tmp.Name())
					http.Error(w, "short bundle text", http.StatusBadRequest)
					return
				}
				tmp.Close()
				tempFiles = append(tempFiles, tmp.Name())
				ch.DataFile = tmp.Name()
			}
		} else if action != fs.ActionDelete && kind == fs.KindFile {
			ch.Data = []byte{}
		}
		changes = append(changes, ch)
	}

	revProps := map[string]string{
		"svn:log":    hdr.Message,
		"svn:author": user,
		"svn:date":   time.Now().UTC().Format("2006-01-02T15:04:05.000000Z"),
	}
	rev, cerr := repo.CommitTxn(base, changes, revProps)
	if cerr != nil {
		var ood *fs.OutOfDateError
		if errors.As(cerr, &ood) {
			writeBundleError(w, http.StatusConflict, "out-of-date", ood.Path+" is out of date")
			return
		}
		writeBundleError(w, http.StatusConflict, "commit-failed", cerr.Error())
		return
	}
	committed = true
	rec, _ := json.Marshal(bundleRecord{Rev: rev, User: user, Created: time.Now().Unix()})
	if err := repo.Store().Put(ctx, key, rec); err != nil {
		// The commit is durable; only the retry shortcut is missing.
		fmt.Fprintf(os.Stderr, "bundle: record %s: %v\n", hdr.Token, err)
	}
	_ = repo.Store().Delete(ctx, []string{claimKey})
	writeBundleResult(w, rev, false)
}

// writeBundleError returns a machine-readable error so the client can tell an
// out-of-date conflict (do not retry; re-run after updating) from a transient
// failure or a concurrent upload with the same token (retry).
func writeBundleError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error": map[string]string{"code": code, "message": msg},
	})
}

func writeBundleResult(w http.ResponseWriter, rev uint64, replayed bool) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{"revision": rev, "replayed": replayed})
}

func readBundleHeader(r io.Reader) (*bundleHeader, error) {
	magic := make([]byte, len(bundleMagic))
	if _, err := io.ReadFull(r, magic); err != nil || string(magic) != bundleMagic {
		return nil, errors.New("bad bundle magic")
	}
	var sz [8]byte
	if _, err := io.ReadFull(r, sz[:]); err != nil {
		return nil, errors.New("short bundle header length")
	}
	n := int64(binary.BigEndian.Uint64(sz[:]))
	if n <= 0 || n > maxBundleHeader {
		return nil, errors.New("bundle header too large")
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, errors.New("short bundle header")
	}
	var hdr bundleHeader
	if err := json.Unmarshal(buf, &hdr); err != nil {
		return nil, fmt.Errorf("bad bundle header: %w", err)
	}
	return &hdr, nil
}

func loadBundleRecord(ctx context.Context, repo *fs.Repo, key string) (bundleRecord, bool) {
	var rec bundleRecord
	b, err := repo.Store().Get(ctx, key, nil)
	if err != nil {
		return rec, false
	}
	if json.Unmarshal(b, &rec) != nil || rec.Rev == 0 {
		return rec, false
	}
	return rec, true
}

func staleClaim(ctx context.Context, repo *fs.Repo, key string, ttl time.Duration) (bool, bool) {
	b, err := repo.Store().Get(ctx, key, nil)
	if err != nil {
		return false, false
	}
	fields := strings.Fields(string(b))
	if len(fields) < 2 {
		return false, true
	}
	var ts int64
	fmt.Sscanf(fields[len(fields)-1], "%d", &ts)
	if ts == 0 {
		return false, true
	}
	return time.Since(time.Unix(ts, 0)) > ttl, true
}

func tokenInList(tok string, list []string) bool {
	for _, t := range list {
		if t == tok {
			return true
		}
	}
	return false
}

// cleanBundlePath normalises a repo-relative path and rejects escapes.
func cleanBundlePath(p string) (string, error) {
	if p == "" || !strings.HasPrefix(p, "/") {
		return "", errors.New("bundle path must be absolute (start with /)")
	}
	clean := "/" + strings.TrimPrefix(pathClean(p), "/")
	if strings.Contains(clean, "..") {
		return "", errors.New("bundle path escapes repository")
	}
	return clean, nil
}

func pathClean(p string) string {
	parts := strings.Split(p, "/")
	out := make([]string, 0, len(parts))
	for _, seg := range parts {
		switch seg {
		case "", ".":
			continue
		case "..":
			if len(out) > 0 {
				out = out[:len(out)-1]
			}
		default:
			out = append(out, seg)
		}
	}
	return "/" + strings.Join(out, "/")
}

// bundleAction resolves an add-or-modify request against HEAD.
func bundleAction(bc *bundleChange, path string, repo *fs.Repo, head uint64) (fs.ChangeAction, byte) {
	kind := fs.KindFile
	if bc.Kind == "dir" {
		kind = fs.KindDir
	}
	switch bc.Action {
	case "delete":
		return fs.ActionDelete, kind
	case "add":
		return fs.ActionAdd, kind
	case "modify":
		return fs.ActionModify, kind
	}
	// "put": resolve existence at HEAD.
	if nr, err := repo.Resolve(head, path); err == nil {
		return fs.ActionModify, nr.Kind
	}
	return fs.ActionAdd, kind
}
