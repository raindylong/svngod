package server

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"svngod/internal/cdc"
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
	maxBundleHeader = 32 << 20
	// Per-text and whole-bundle caps are policy guards, not memory bounds:
	// texts above 1 MiB are streamed into a temp file. A single bundle may
	// carry multi-gigabyte binaries (1 TiB total, 64 GiB per file).
	maxBundleText    = 64 << 30
	maxBundleTotal   = 1 << 40
	maxBundleChanges = 500000
	bundleClaimTTL   = 5 * time.Minute
)

type bundleChange struct {
	Path    string            `json:"path"`
	Action  string            `json:"action"` // put | add | modify | delete | check
	Kind    string            `json:"kind"`   // file | dir
	BaseRev uint64            `json:"baseRev,omitempty"`
	Props   map[string]string `json:"props,omitempty"`
	TextLen int64             `json:"textLen,omitempty"`
	// Chunked upload: Chunks is the full ordered manifest, Missing lists the
	// hashes whose bytes follow the header (in manifest order). MD5/SHA1 are
	// the full text checksums, required so reads can verify the text.
	Chunks     []bundleChunk `json:"chunks,omitempty"`
	Missing    []string      `json:"missing,omitempty"`
	ChunkTotal uint64        `json:"chunkTotal,omitempty"`
	MD5        string        `json:"md5,omitempty"`
	SHA1       string        `json:"sha1,omitempty"`
}

type bundleChunk struct {
	Hash string `json:"h"`
	Size uint32 `json:"n"`
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

func bundleIntentKey(repo *fs.Repo, token string) string {
	return repo.Keys().Prefix + "/bundles/intent-" + token
}

// bundleIntent is written before the commit and kept until the token record
// is stored. It closes the crash window between publishing the revision and
// remembering its token: on a retry the token is found in the revision's
// revprops, so the upload is never committed twice.
type bundleIntent struct {
	Token   string `json:"token"`
	BaseRev uint64 `json:"baseRev"`
	Message string `json:"message"`
	User    string `json:"user"`
	Created int64  `json:"created"`
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
	if user == "" && s.authFor(name) != nil {
		s.unauthorized(w)
		return
	}
	// Pick up locks created on another replica before validating changes.
	s.refreshLocks(name, repo)
	release, ok := s.acquireUpload(w, r, user)
	if !ok {
		return
	}
	defer release()

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
	intentKey := bundleIntentKey(repo, hdr.Token)

	// Replay after a retry: the body is drained so the client can finish
	// sending, then the recorded revision is returned.
	if rec, ok := loadBundleRecord(ctx, repo, key); ok {
		io.Copy(io.Discard, io.LimitReader(r.Body, maxBundleTotal))
		if rec.User != "" && rec.User != user {
			http.Error(w, "token belongs to another user", http.StatusConflict)
			return
		}
		repo.Store().Delete(ctx, []string{intentKey})
		s.m.bundleReplays.Add(1)
		writeBundleResult(w, rec.Rev, true)
		return
	}
	// A previous attempt may have crashed after CommitTxn succeeded but
	// before the token record was stored. The token is part of the revision
	// properties (written atomically with the commit), so a bounded scan
	// recovers the revision instead of committing a duplicate.
	if in, ok := loadBundleIntent(ctx, repo, intentKey); ok {
		if rev, found := findCommittedToken(ctx, repo, hdr.Token, in.Created); found {
			saveBundleRecord(ctx, repo, key, hdr.Token, user, rev)
			repo.Store().Delete(ctx, []string{intentKey})
			s.m.bundleReplays.Add(1)
			io.Copy(io.Discard, io.LimitReader(r.Body, maxBundleTotal))
			writeBundleResult(w, rev, true)
			return
		}
	}
	s.m.bundleUploads.Add(1)
	// Persist the intent before claiming/committing so the recovery path above
	// knows which token to look for.
	if b, merr := json.Marshal(bundleIntent{
		Token: hdr.Token, BaseRev: hdr.BaseRev, Message: hdr.Message,
		User: user, Created: time.Now().Unix(),
	}); merr == nil {
		if perr := repo.Store().Put(ctx, intentKey, b); perr != nil {
			fmt.Fprintf(os.Stderr, "bundle: intent %s: %v", hdr.Token, perr)
		}
	}

	// Serialise concurrent uploads of the same token so only one commit can
	// happen. A crashed claim expires after bundleClaimTTL.
	claimKey := key + ".lock"
	claim := []byte(fmt.Sprintf("%s %d", user, time.Now().Unix()))
	claimErr := repo.Store().CreateIfAbsent(ctx, claimKey, claim)
	if claimErr != nil {
		if !errors.Is(claimErr, store.ErrPreconditionFailed) {
			if storageUnavailable(claimErr) {
				// A fenced replica (cold start with the peer down) is a
				// transient condition: let the client retry instead of
				// reporting a generic server error.
				w.Header().Set("Retry-After", "2")
				writeBundleError(w, http.StatusServiceUnavailable, "unavailable", claimErr.Error())
				return
			}
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
		io.Copy(io.Discard, io.LimitReader(r.Body, maxBundleTotal))
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
			repo.Store().Delete(context.Background(), []string{claimKey, intentKey})
		}
	}()

	head, err := repo.HeadFresh()
	if err != nil {
		if storageUnavailable(err) {
			w.Header().Set("Retry-After", "2")
			writeBundleError(w, http.StatusServiceUnavailable, "unavailable", err.Error())
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	base := hdr.BaseRev
	if base == 0 || base > head {
		base = head
	}

	az := s.authzFor(name)
	changes := make([]fs.Change, 0, len(hdr.Changes))
	var tempFiles []string
	defer func() {
		for _, f := range tempFiles {
			os.Remove(f)
		}
	}()

	var total int64
	var chunkBytes int64
	for i := range hdr.Changes {
		bc := &hdr.Changes[i]
		path, perr := cleanBundlePath(bc.Path)
		if perr != nil {
			http.Error(w, perr.Error(), http.StatusBadRequest)
			return
		}
		if bc.TextLen < 0 || bc.TextLen > maxBundleText || total+bc.TextLen > maxBundleTotal {
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
		if bc.Action == "check" {
			if bc.TextLen != 0 {
				http.Error(w, "check change must not carry text", http.StatusBadRequest)
				return
			}
			ch.Noop = true
			ch.Action = fs.ActionModify
			ch.Kind = fs.KindFile
		}
		if ch.BaseRev == 0 && action != fs.ActionAdd {
			ch.BaseRev = base
		}
		if len(bc.Chunks) > 0 {
			// Chunked upload: the header carries the full manifest and the
			// hashes whose bytes follow. The server re-checks each hash and
			// stores only what it lacks.
			if kind != fs.KindFile || action == fs.ActionDelete || bc.TextLen != 0 {
				http.Error(w, "chunked change must be a file put", http.StatusBadRequest)
				return
			}
			if len(bc.MD5) != 32 || len(bc.SHA1) != 40 {
				http.Error(w, "chunked change requires md5 and sha1", http.StatusBadRequest)
				return
			}
			md5sum, merr := hex.DecodeString(strings.ToLower(bc.MD5))
			shaSum, serr := hex.DecodeString(strings.ToLower(bc.SHA1))
			if merr != nil || serr != nil || len(md5sum) != 16 || len(shaSum) != 20 {
				http.Error(w, "bad chunked checksums", http.StatusBadRequest)
				return
			}
			refs := make([]cdc.Ref, len(bc.Chunks))
			for i, c := range bc.Chunks {
				hb, herr := hex.DecodeString(strings.ToLower(c.Hash))
				if herr != nil || len(hb) != cdc.HashLen || c.Size == 0 || c.Size > cdc.AbsoluteMax {
					http.Error(w, "bad chunk entry", http.StatusBadRequest)
					return
				}
				copy(refs[i].Hash[:], hb)
				refs[i].Size = c.Size
			}
			if cdc.Size(refs) != bc.ChunkTotal {
				http.Error(w, "chunk total mismatch", http.StatusBadRequest)
				return
			}
			missing := make(map[string]bool, len(bc.Missing))
			for _, h := range bc.Missing {
				missing[strings.ToLower(h)] = true
			}
			var missingBytes int64
			for _, ref := range refs {
				if missing[cdc.HashHex(ref)] {
					missingBytes += int64(ref.Size)
				}
			}
			chunkBytes += missingBytes
			if chunkBytes > maxBundleTotal {
				http.Error(w, "bundle chunks too large", http.StatusRequestEntityTooLarge)
				return
			}
			if err := storeChunksParallel(ctx, repo, refs, missing, r.Body); err != nil {
				switch {
				case errors.Is(err, errBundleChunkMissing):
					writeBundleError(w, http.StatusConflict, "chunk-missing", "a chunk disappeared; retry the upload")
				case errors.Is(err, errBundleChunkStore):
					http.Error(w, err.Error(), http.StatusInternalServerError)
				default:
					http.Error(w, "chunk upload: "+err.Error(), http.StatusBadRequest)
				}
				return
			}
			ch.ChunkManifest = cdc.Marshal(refs)
			ch.ChunkTotal = bc.ChunkTotal
			copy(ch.DataMD5[:], md5sum)
			ch.HasMD5 = true
			copy(ch.DataSHA1[:], shaSum)
			ch.HasSHA1 = true
		} else if bc.TextLen > 0 {
			if bc.TextLen > cdc.Threshold && kind == fs.KindFile {
				// Split the request stream directly into chunks: no temp file
				// and no second full read for a multi-gigabyte upload.
				h := md5.New()
				hs := sha1.New()
				limited := io.LimitReader(r.Body, bc.TextLen)
				manifest, total, cerr := repo.StoreChunkedReader(io.TeeReader(limited, io.MultiWriter(h, hs)))
				if cerr != nil {
					http.Error(w, "chunk store: "+cerr.Error(), http.StatusInternalServerError)
					return
				}
				if total != bc.TextLen {
					http.Error(w, "short bundle text", http.StatusBadRequest)
					return
				}
				ch.ChunkManifest = manifest
				ch.ChunkTotal = uint64(total)
				copy(ch.DataMD5[:], h.Sum(nil))
				ch.HasMD5 = true
				copy(ch.DataSHA1[:], hs.Sum(nil))
				ch.HasSHA1 = true
			} else if bc.TextLen <= 1<<20 {
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
				// Hash while spooling: the commit can then chunk the file
				// without a second full read for MD5/SHA-1.
				h := md5.New()
				hs := sha1.New()
				if _, cerr := io.CopyN(io.MultiWriter(tmp, h, hs), r.Body, bc.TextLen); cerr != nil {
					tmp.Close()
					os.Remove(tmp.Name())
					http.Error(w, "short bundle text", http.StatusBadRequest)
					return
				}
				tmp.Close()
				copy(ch.DataMD5[:], h.Sum(nil))
				ch.HasMD5 = true
				copy(ch.DataSHA1[:], hs.Sum(nil))
				ch.HasSHA1 = true
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
		// Part of the atomic commit: a retry that lost the token record can
		// still prove this revision is its own.
		"svngod:bundle-token": hdr.Token,
	}
	rev, cerr := repo.CommitTxn(base, changes, revProps)
	if cerr == nil {
		s.m.commits.Add(1)
	}
	if cerr != nil {
		var ood *fs.OutOfDateError
		if errors.As(cerr, &ood) {
			writeBundleError(w, http.StatusConflict, "out-of-date", ood.Path+" is out of date")
			return
		}
		if errors.Is(cerr, store.ErrReplicaLagging) || errors.Is(cerr, store.ErrQuorumUnmet) ||
			errors.Is(cerr, fs.ErrCommitBusy) || errors.Is(cerr, fs.ErrLeaseLost) ||
			errors.Is(cerr, fs.ErrLeaseUnverified) {
			writeBundleError(w, http.StatusServiceUnavailable, "unavailable", cerr.Error())
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

// bundleStatRequest asks for the content hash of paths the client already has
// locally, so unchanged files can be skipped instead of re-uploaded.
type bundleStatRequest struct {
	Path  string   `json:"path"`
	Files []string `json:"files"`
}

type bundleStatEntry struct {
	SHA1 string `json:"sha1,omitempty"`
	Size int64  `json:"size,omitempty"`
	Kind string `json:"kind,omitempty"` // "dir" for directories
}

// handleBundleStat serves POST <repo>/!svngod/stat: for each requested path,
// return the HEAD file's sha1 when known. The client compares it with its
// local hash and omits matching files from the bundle body.
func (s *Server) handleBundleStat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "stat endpoint accepts POST", http.StatusMethodNotAllowed)
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
	if user == "" && s.authFor(name) != nil {
		s.unauthorized(w)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var req bundleStatRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "bad stat request", http.StatusBadRequest)
		return
	}
	if len(req.Files) > maxBundleChanges {
		http.Error(w, "too many files", http.StatusBadRequest)
		return
	}
	head, err := repo.HeadFresh()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	az := s.authzFor(name)
	out := make(map[string]bundleStatEntry, len(req.Files))
	for _, p := range req.Files {
		path, perr := cleanBundlePath(p)
		if perr != nil {
			continue
		}
		if az != nil && !az.Authorize(user, name, path, false) {
			continue
		}
		nr, rerr := repo.Resolve(head, path)
		if rerr != nil {
			continue
		}
		if nr.Kind == fs.KindDir {
			out[path] = bundleStatEntry{Kind: "dir"}
			continue
		}
		if nr.Kind != fs.KindFile {
			continue
		}
		if nr.TextSHA1 == ([20]byte{}) {
			continue // unknown; the client will upload it
		}
		out[path] = bundleStatEntry{SHA1: hex.EncodeToString(nr.TextSHA1[:]), Size: int64(nr.TotalSize)}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"files": out})
}

// chunkQuery asks which chunk objects the repository already has.
type chunkQuery struct {
	Hashes []string `json:"hashes"`
}

// handleBundleChunks answers POST <repo>/!svngod/chunks with the subset the
// repository lacks, so a client uploads only new chunks of a changed file.
func (s *Server) handleBundleChunks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "chunks endpoint accepts POST", http.StatusMethodNotAllowed)
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
	if _, ok := s.authenticate(w, r, name); !ok {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var req chunkQuery
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "bad chunk query", http.StatusBadRequest)
		return
	}
	if len(req.Hashes) > maxBundleChanges {
		http.Error(w, "too many chunks", http.StatusBadRequest)
		return
	}
	ctx := context.Background()
	// Existence checks are round trips on object storage; run them on a
	// bounded pool instead of serialising hundreds of HEADs.
	valid := make([]string, 0, len(req.Hashes))
	for _, h := range req.Hashes {
		h = strings.ToLower(h)
		if len(h) == 64 && strings.Trim(h, "0123456789abcdef") == "" {
			valid = append(valid, h)
		}
	}
	var mu sync.Mutex
	missing := make([]string, 0)
	var firstErr error
	const workers = 16
	ch := make(chan string, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for h := range ch {
				ok, eerr := repo.Store().Exists(ctx, repo.Keys().Chunk(h))
				mu.Lock()
				if eerr != nil && firstErr == nil {
					firstErr = eerr
				}
				if eerr == nil && !ok {
					missing = append(missing, h)
				}
				mu.Unlock()
			}
		}()
	}
	for _, h := range valid {
		ch <- h
	}
	close(ch)
	wg.Wait()
	if firstErr != nil {
		http.Error(w, firstErr.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"missing": missing})
}

var (
	errBundleChunkMissing = errors.New("bundle: chunk missing")
	errBundleChunkStore   = errors.New("bundle: chunk store")
)

// storeChunksParallel reads the missing chunks from body in manifest order
// and writes them on a small pool; non-missing chunks must already exist.
func storeChunksParallel(ctx context.Context, repo *fs.Repo, refs []cdc.Ref, missing map[string]bool, body io.Reader) error {
	const workers = 8
	type job struct {
		key  string
		data []byte
	}
	jobs := make(chan job, workers)
	var wg sync.WaitGroup
	var firstErr error
	var mu sync.Mutex
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if err := repo.Store().Put(ctx, j.key, j.data); err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("%w: %v", errBundleChunkStore, err)
					}
					mu.Unlock()
				}
			}
		}()
	}
	for _, ref := range refs {
		hexh := cdc.HashHex(ref)
		if missing[hexh] {
			data := make([]byte, ref.Size)
			if _, rerr := io.ReadFull(body, data); rerr != nil {
				close(jobs)
				wg.Wait()
				return fmt.Errorf("short chunk: %w", rerr)
			}
			if sum := sha256.Sum256(data); sum != ref.Hash {
				close(jobs)
				wg.Wait()
				return errors.New("chunk checksum mismatch")
			}
			jobs <- job{key: repo.Keys().Chunk(hexh), data: data}
			continue
		}
		ok, eerr := repo.Store().Exists(ctx, repo.Keys().Chunk(hexh))
		if eerr != nil {
			close(jobs)
			wg.Wait()
			return fmt.Errorf("%w: %v", errBundleChunkStore, eerr)
		}
		if !ok {
			close(jobs)
			wg.Wait()
			return errBundleChunkMissing
		}
	}
	close(jobs)
	wg.Wait()
	return firstErr
}

// writeBundleError returns a machine-readable error so the client can tell an// writeBundleError returns a machine-readable error so the client can tell an
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

func loadBundleIntent(ctx context.Context, repo *fs.Repo, key string) (bundleIntent, bool) {
	var in bundleIntent
	b, err := repo.Store().Get(ctx, key, nil)
	if err != nil {
		return in, false
	}
	if json.Unmarshal(b, &in) != nil || in.Token == "" {
		return in, false
	}
	return in, true
}

func saveBundleRecord(ctx context.Context, repo *fs.Repo, key, token, user string, rev uint64) {
	rec, _ := json.Marshal(bundleRecord{Rev: rev, User: user, Created: time.Now().Unix()})
	if err := repo.Store().Put(ctx, key, rec); err != nil {
		fmt.Fprintf(os.Stderr, "bundle: record %s: %v", token, err)
	}
}

// findCommittedToken looks for a revision carrying this bundle token. It is a
// crash-recovery path only, bounded to recent revisions.
func findCommittedToken(ctx context.Context, repo *fs.Repo, token string, since int64) (uint64, bool) {
	head, err := repo.HeadFresh()
	if err != nil {
		return 0, false
	}
	const maxScan = 512
	for i := uint64(0); i < maxScan && head > i; i++ {
		rev := head - i
		rp, rerr := repo.GetRevProps(rev)
		if rerr != nil || rp == nil {
			continue
		}
		if rp.Props["svngod:bundle-token"] == token {
			return rev, true
		}
		// Stop once revisions clearly predate the intent.
		if d := rp.Props["svn:date"]; d != "" {
			if t, perr := time.Parse("2006-01-02T15:04:05.000000Z", d); perr == nil && t.Unix()+120 < since {
				return 0, false
			}
		}
	}
	return 0, false
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
	case "modify", "check":
		return fs.ActionModify, fs.KindFile
	}
	// "put": resolve existence at HEAD.
	if nr, err := repo.Resolve(head, path); err == nil {
		return fs.ActionModify, nr.Kind
	}
	return fs.ActionAdd, kind
}
