package server

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"svngod/internal/fs"
	"svngod/internal/svndiff"
)

// activity is a server-side commit session, keyed by the MKACTIVITY uuid.
type activity struct {
	id       string
	repo     string            // repository name, for reaping staged objects
	created  int64             // unix seconds; bounds abandoned sessions
	revProps map[string]string // svn:log / svn:author / svn:date (set via PROPPATCH)
	txn      *txn
}

// txnChange is one pending path change inside a transaction.
type txnChange struct {
	action       fs.ChangeAction
	kind         byte
	data         []byte
	props        map[string]string
	copyFromPath string
	copyFromRev  uint64
	// baseRev is the client's working-copy revision for this path
	// (X-SVN-Version-Name), 0 when the path is new. It is the correct base
	// for svndiff PUTs and for the out-of-date check: using the transaction's
	// HEAD instead silently merged deltas into the wrong text.
	baseRev uint64
	// dataRef names the content-addressed body in the repo store for sessions
	// restored after a restart (the bytes are fetched on first use).
	dataRef string
	// dataFile is a local temp file holding the body (streamed large PUT);
	// streamed marks bodies that must not be loaded into memory on restore.
	dataFile string
	streamed bool
	// dataSize is the streamed body's length.
	dataSize int64
	// dataMD5 is computed while the body is streamed/decoded, so the commit
	// does not need a second hash pass over the file.
	dataMD5 [16]byte
	hasMD5  bool
	// dataTextRef points at an already stored external text object (large or
	// svndiff bodies are written to texts/ during the upload, so the commit
	// only records the reference and persistence needs no body copy).
	dataTextRef  string
	dataTextSize uint64
	// dataSHA1 is the body's SHA-1 when hashed while streaming; it is stored
	// in the node so update reports need not read the text back.
	dataSHA1 [20]byte
	hasSHA1  bool
	// delta holds the client's svndiff stream for a large-text edit kept as a
	// delta (see fs.Change.Delta); deltaRef is its persisted object name.
	delta      []byte
	deltaRef   string
	deltaBase  fs.NodeID
	deltaTotal uint64
	// gen bumps whenever the body reference changes, so a concurrent
	// persistActivity cannot attach a reference to newer bytes.
	gen uint64
}

// txn is a checked-out working resource bound to an activity.
type txn struct {
	id      string
	baseRev uint64
	changes map[string]*txnChange
	// changesLoaded is false for a transaction restored from the store: its
	// fragments are read on demand, so an abandoned large session does not
	// slow down every server start.
	changesLoaded bool
}

func (s *Server) activityByID(id string) (*activity, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.activities[id]
	return a, ok
}

func (s *Server) txnByID(id string) (*txn, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.txns[id]
	return t, ok
}

// --- MKACTIVITY ---

func (s *Server) handleMkactivity(w http.ResponseWriter, r *http.Request, res resource) {
	if res.typ != resActivity || res.uuid == "" {
		http.Error(w, "MKACTIVITY requires an activity URI", http.StatusBadRequest)
		return
	}
	repo, rok := s.repo(w, res)
	if !rok {
		return
	}
	s.mu.Lock()
	if _, ok := s.activities[res.uuid]; !ok {
		s.activities[res.uuid] = &activity{id: res.uuid, repo: res.repo, created: time.Now().Unix(), revProps: map[string]string{}}
	}
	s.mu.Unlock()
	s.persistActivity(repo, res.uuid)
	w.Header().Set("Location", s.absURL(r, res.repo, "/!svn/act/"+res.uuid+"/"))
	w.WriteHeader(http.StatusCreated)
}

// --- CHECKOUT ---

func (s *Server) handleCheckout(w http.ResponseWriter, r *http.Request, res resource) {
	// The activity is named in the Destination header, or (svn clients) in the
	// request body's <D:activity-set><D:href>.
	dst := r.Header.Get("Destination")
	if dst == "" {
		dst = extractActivityFromBody(r)
	}
	if dst == "" {
		http.Error(w, "CHECKOUT requires a Destination header naming the activity", http.StatusBadRequest)
		return
	}
	actID := extractActivityID(dst)
	if actID == "" {
		http.Error(w, "CHECKOUT Destination is not an activity URI", http.StatusConflict)
		return
	}
	if repo, ok := s.repo(w, res); ok {
		s.loadTxnChanges(repo, res.repo, "txn-"+actID)
	} else {
		return
	}
	s.mu.Lock()
	a, ok := s.activities[actID]
	if !ok {
		s.mu.Unlock()
		http.Error(w, "activity does not exist", http.StatusConflict)
		return
	}
	created := a.txn == nil
	var repoHandle *fs.Repo
	if a.txn == nil {
		repo, rok := s.openRepoOnly(w, res.repo)
		if !rok {
			s.mu.Unlock()
			return
		}
		head, err := repo.HeadFresh()
		if err != nil {
			s.mu.Unlock()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		a.txn = &txn{id: "txn-" + actID, baseRev: head, changes: map[string]*txnChange{}, changesLoaded: true}
		s.txns[a.txn.id] = a.txn
		repoHandle = repo
	}
	tx := a.txn
	s.mu.Unlock()
	// Only a newly created transaction needs durable state; a per-file
	// CHECKOUT only opens a working resource and is recreated by its PUT.
	if created && repoHandle != nil {
		s.persistActivity(repoHandle, actID)
	}

	// Return the working resource for this transaction: the checked-out
	// resource path (e.g. CHECKOUT bln/1/hello.txt -> txn/<id>/hello.txt),
	// or the txn root when checking out the VCC / whole baseline.
	target := "/!svn/txn/" + tx.id
	if res.path != "" && res.path != "/" {
		target += res.path
	} else {
		target += "/"
	}
	w.Header().Set("Location", s.absURL(r, res.repo, target))
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?><D:checkout-response xmlns:D="%s"></D:checkout-response>`, NS_DAV)
}

// extractActivityID pulls the activity uuid out of a Destination activity URL
// or, for HTTPv2, a transaction URL (!svn/txn/txn-<uuid> and its txr alias).
func extractActivityID(dst string) string {
	markers := []string{"/!svn/act/", "/!svn/txn/", "/!svn/txr/"}
	for _, marker := range markers {
		i := strings.Index(dst, marker)
		if i < 0 {
			continue
		}
		rest := strings.Trim(dst[i+len(marker):], "/")
		if j := strings.IndexAny(rest, "/?"); j >= 0 {
			rest = rest[:j]
		}
		return strings.TrimPrefix(rest, "txn-")
	}
	return ""
}

// extractActivityFromBody reads the CHECKOUT request body and returns the
// activity URL from its <D:activity-set><D:href> element, if present.
func extractActivityFromBody(r *http.Request) string {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	var root xmlEl
	if xml.Unmarshal(body, &root) != nil {
		return ""
	}
	if act, ok := root.child("activity-set"); ok {
		if href, ok := act.child("href"); ok {
			return strings.TrimSpace(href.Content)
		}
	}
	return ""
}

func (s *Server) openRepoOnly(w http.ResponseWriter, name string) (*fs.Repo, bool) {
	// Prefer the shared instance: a fresh Repo per call means duplicate caches
	// (format/uuid/rev blocks) and duplicate commit mutexes.
	s.repoMapMu.Lock()
	r := s.repoCache[name]
	s.repoMapMu.Unlock()
	if r != nil {
		return r, true
	}
	opener := s.cfg.openerFor(name)
	if opener == nil {
		http.Error(w, "no repositories configured", http.StatusInternalServerError)
		return nil, false
	}
	r, err := opener(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return nil, false
	}
	s.repoMapMu.Lock()
	if prev := s.repoCache[name]; prev != nil {
		r = prev
	} else {
		s.repoCache[name] = r
	}
	s.repoMapMu.Unlock()
	return r, true
}

// clientBaseRevision returns the working-copy revision a mutating request is
// based on (X-SVN-Version-Name), or 0 when the node is new.
func clientBaseRevision(r *http.Request) uint64 {
	if v := r.Header.Get("X-SVN-Version-Name"); v != "" {
		if n, err := strconv.ParseUint(strings.TrimSpace(v), 10, 64); err == nil {
			return n
		}
	}
	return 0
}

// writeHookFailure reports a pre-commit veto the way mod_dav_svn does: 403
// with errcode 165001 so svn prints the hook's own message (E165001).
func writeHookFailure(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusForbidden)
	fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?>`+"\n"+
		`<D:error xmlns:D="DAV:" xmlns:m="http://apache.org/dav/xmlns" xmlns:C="svn:">`+"\n"+
		`<C:error/>`+"\n"+
		`<m:human-readable errcode="165001">%s</m:human-readable>`+"\n"+
		`</D:error>`+"\n", xmlEscape("Commit blocked by pre-commit hook: "+msg))
}

// writeOutOfDate rejects a stale working copy the way mod_dav_svn does: 409
// with a DAV error body whose errcode makes the client report E170004
// ("File ... is out of date") instead of a generic HTTP failure.
func writeOutOfDate(w http.ResponseWriter, path string) {
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusConflict)
	fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?>`+"\n"+
		`<D:error xmlns:D="DAV:" xmlns:m="http://apache.org/dav/xmlns" xmlns:C="svn:">`+"\n"+
		`<C:error/>`+"\n"+
		`<m:human-readable errcode="170004">File '%s' is out of date</m:human-readable>`+"\n"+
		`</D:error>`+"\n", xmlEscape(path))
}

// --- PROPPATCH ---

func (s *Server) handleProppatch(w http.ResponseWriter, r *http.Request, res resource) {
	repo, ok := s.repo(w, res)
	if !ok {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if res.typ == resTxn {
		s.loadTxnChanges(repo, res.repo, res.uuid)
	}
	sets, removes := parsePropertyUpdate(body)

	switch res.typ {
	case resActivity:
		// A commit may have been started on another replica.
		s.restoreActivity(res.repo, repo, res.uuid)
		// Setting revision properties on the activity (svn:log, author, date).
		s.mu.Lock()
		a, ok := s.activities[res.uuid]
		if !ok {
			s.mu.Unlock()
			http.Error(w, "activity does not exist", http.StatusConflict)
			return
		}
		for _, p := range sets {
			if a.revProps == nil {
				a.revProps = map[string]string{}
			}
			a.revProps[svnPropertyKey(p.ns, p.local)] = p.value
		}
		s.mu.Unlock()
		s.persistActivity(repo, res.uuid)
		w.WriteHeader(http.StatusOK)
	case resTxn:
		s.mu.Lock()
		tx, ok := s.txns[res.uuid]
		if !ok {
			s.mu.Unlock()
			http.Error(w, "transaction does not exist", http.StatusConflict)
			return
		}
		// PROPPATCH on the txn root carries revision properties (svn:log,
		// svn:date, ...). Store them on the activity's revProps, not as a
		// node property (that would create an empty root change).
		if res.path == "" || res.path == "/" {
			actID := strings.TrimPrefix(tx.id, "txn-")
			if a, ok := s.activities[actID]; ok {
				if a.revProps == nil {
					a.revProps = map[string]string{}
				}
				for _, p := range sets {
					a.revProps[svnPropertyKey(p.ns, p.local)] = p.value
				}
				for _, p := range removes {
					delete(a.revProps, svnPropertyKey(p.ns, p.local))
				}
			}
			s.mu.Unlock()
			s.persistActivity(repo, strings.TrimPrefix(tx.id, "txn-"))
			// 207 multistatus acknowledging the revision props.
			var b strings.Builder
			b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
			b.WriteString(`<D:multistatus xmlns:D="DAV:" xmlns:svn="http://subversion.tigris.org/xmlns/svn/">` + "\n")
			b.WriteString(`  <D:response>` + "\n")
			b.WriteString(`    <D:href>` + s.absURL(r, res.repo, "/!svn/txn/"+tx.id+"/") + `</D:href>` + "\n")
			b.WriteString(`    <D:propstat><D:prop>` + "\n")
			for _, p := range sets {
				b.WriteString("      " + davPropElement(svnPropertyKey(p.ns, p.local), p.value) + "\n")
			}
			b.WriteString(`    </D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat>` + "\n")
			b.WriteString(`  </D:response>` + "\n")
			b.WriteString(`</D:multistatus>` + "\n")
			w.Header().Set("Content-Type", "text/xml; charset=utf-8")
			w.WriteHeader(http.StatusMultiStatus)
			io.WriteString(w, b.String())
			return
		}
		if e := s.getLockLocked(res.repo, res.path); e != nil && !lockTokenMatches(r.Header.Get("If"), e.token) {
			s.mu.Unlock()
			http.Error(w, "Locked", http.StatusLocked)
			return
		}
		ch := tx.changeFor(res.path)
		// A node-property PROPPATCH may be the only change to this path (no
		// PUT/MKCOL); inherit its kind from the base revision so the commit
		// stores a file/dir rather than a zero-kind node (which would commit
		// the file as a directory).
		if ch.kind == 0 && tx.baseRev > 0 {
			if r2, ok2 := s.openRepoOnly(w, res.repo); ok2 {
				if nr, err := r2.Resolve(tx.baseRev, res.path); err == nil {
					ch.kind = nr.Kind
				}
			}
		}
		if ch.props == nil {
			ch.props = map[string]string{}
		}
		if br := clientBaseRevision(r); br > 0 {
			ch.baseRev = br
		}
		for _, p := range sets {
			ch.props[svnPropertyKey(p.ns, p.local)] = p.value
		}
		for _, p := range removes {
			delete(ch.props, svnPropertyKey(p.ns, p.local))
		}
		s.mu.Unlock()
		s.persistChange(repo, strings.TrimPrefix(tx.id, "txn-"), res.path)
		// 207 multistatus acknowledging each prop
		var b strings.Builder
		b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
		b.WriteString(`<D:multistatus xmlns:D="` + NS_DAV + `">` + "\n")
		b.WriteString(`  <D:response>` + "\n")
		b.WriteString(`    <D:href>` + s.absURL(r, res.repo, "/!svn/txn/"+tx.id+res.path) + `</D:href>` + "\n")
		b.WriteString(`    <D:propstat><D:prop>` + "\n")
		for _, p := range sets {
			b.WriteString("      " + davPropElement(svnPropertyKey(p.ns, p.local), p.value) + "\n")
		}
		b.WriteString(`    </D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat>` + "\n")
		b.WriteString(`  </D:response>` + "\n")
		b.WriteString(`</D:multistatus>` + "\n")
		w.Header().Set("Content-Type", "text/xml; charset=utf-8")
		w.WriteHeader(http.StatusMultiStatus)
		io.WriteString(w, b.String())
	default:
		_ = repo
		http.Error(w, "PROPPATCH not supported on this resource", http.StatusMethodNotAllowed)
	}
}

// --- PUT / DELETE / MKCOL on a transaction working resource ---

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request, res resource) {
	if res.typ != resTxn {
		http.Error(w, "PUT only applies to a transaction working resource", http.StatusBadRequest)
		return
	}
	release, ok := s.acquireUpload(w, r)
	if !ok {
		return
	}
	defer release()
	repo, rok := s.repo(w, res)
	if !rok {
		return
	}
	s.loadTxnChanges(repo, res.repo, res.uuid)
	s.mu.Lock()
	tx, ok := s.txns[res.uuid]
	if !ok {
		s.mu.Unlock()
		http.Error(w, "transaction does not exist", http.StatusConflict)
		return
	}
	actID := strings.TrimPrefix(tx.id, "txn-")
	txnBase := tx.baseRev
	s.mu.Unlock()

	if e := s.getLock(res.repo, res.path); e != nil && !lockTokenMatches(r.Header.Get("If"), e.token) {
		// mod_dav_svn answers 423 here; the client reports "no lock token
		// available" instead of silently overwriting a locked file.
		http.Error(w, "Locked", http.StatusLocked)
		return
	}

	// The delta (and the out-of-date decision) must use the revision the
	// client based its edit on, never the transaction's HEAD: another commit
	// may have moved HEAD in between, and applying an old delta to the new
	// text corrupts the file.
	clientRev := clientBaseRevision(r)
	isNew := true
	var baseNode *fs.NodeRev
	var baseLen int64
	// baseSrc reads base windows on demand. Delta application against a
	// multi-gigabyte file must not materialise that file: svndiff only needs
	// one source window at a time, and ReadTextRange can serve it from the
	// object store (ranged GET) or a local pread.
	baseSrc := svndiff.SourceFunc(func(int64, int64) ([]byte, error) { return nil, nil })
	if clientRev > 0 {
		// Resolve the client's own base only. Whether HEAD moved is checked by
		// CommitTxn against the exact node (one fewer round trip per PUT, which
		// matters for object stores); a stale change fails there with the same
		// out-of-date answer instead of before the upload.
		if nr, err := repo.Resolve(clientRev, res.path); err == nil && nr.Kind == fs.KindFile {
			isNew = false
			baseNode = nr
			baseLen, _ = repo.TextSizeOf(nr)
			if nr.DeltaDepth == 0 {
				// A full text can be read window-by-window straight from the
				// store: applying a 1 MiB delta to a 1 GiB base costs ~50 MiB
				// of memory instead of loading the whole base.
				baseSrc = rangeSource(repo, nr, baseLen)
			} else {
				// The stored node is itself a delta, and an edit may have
				// shifted bytes: source windows must come from the
				// reconstructed text. Rebuild it level by level into a temp
				// file so a chain of edits stays memory-bounded.
				path, size, cleanup, merr := materializeText(repo, nr)
				if merr != nil {
					http.Error(w, "read base text: "+merr.Error(), http.StatusInternalServerError)
					return
				}
				defer cleanup()
				f, oerr := os.Open(path)
				if oerr != nil {
					http.Error(w, "read base text: "+oerr.Error(), http.StatusInternalServerError)
					return
				}
				defer f.Close()
				baseLen = size
				baseSrc = func(off, length int64) ([]byte, error) {
					if off < 0 || length < 0 || off+length > size {
						return nil, errors.New("svndiff: base range out of bounds")
					}
					b := make([]byte, length)
					if _, rerr := f.ReadAt(b, off); rerr != nil {
						return nil, rerr
					}
					return b, nil
				}
			}
			if want := r.Header.Get("X-SVN-Base-Fulltext-MD5"); want != "" {
				sum, herr := hashSource(baseSrc, baseLen)
				if herr != nil {
					http.Error(w, "read base text: "+herr.Error(), http.StatusInternalServerError)
					return
				}
				if !strings.EqualFold(hex.EncodeToString(sum[:]), want) {
					writeOutOfDate(w, res.path)
					return
				}
			}
		} else if err != nil {
			clientRev = 0 // not present at the client's base: a new file
		}
	}
	_ = txnBase

	isSvndiff := strings.Contains(r.Header.Get("Content-Type"), "svndiff")
	var data []byte
	var dataSize int64
	var dataTextRef string
	var dataTextSize uint64
	var dataSHA1 [20]byte
	var dataMD5 [16]byte
	var hasMD5 bool
	var deltaBytes []byte
	var deltaBase fs.NodeID
	var deltaTotal uint64

	const maxPutBody = 64 << 30
	storeText := func(path string, sum [16]byte) (string, int64, error) {
		st, serr := os.Stat(path)
		if serr != nil {
			os.Remove(path)
			return "", 0, serr
		}
		ref := hex.EncodeToString(sum[:])
		if err := repo.PutTextFile(context.Background(), path, ref); err != nil {
			os.Remove(path)
			return "", 0, err
		}
		os.Remove(path) // PutFile may have linked instead of moved
		return ref, st.Size(), nil
	}

	// A large svndiff body is decoded straight from the socket into its text
	// object: spooling it first would write and re-read the whole payload.
	// Small bodies (the common delta case) are kept inline so edits of small
	// files do not create per-revision objects.
	// Streaming is chosen by the *larger* of body and base: a 1 MiB edit of
	// a 1 GiB file has a small body but must still be decoded window by
	// window, and its delta kept, instead of materialising a fresh 1 GiB
	// text in memory and storing a full copy.
	if isSvndiff && (r.ContentLength < 0 || r.ContentLength > putStreamThreshold ||
		baseLen > putStreamThreshold) {
		// Keep the raw delta when it is tiny: an edit of a large binary file
		// often sends a few KiB against a 30 MiB base, and storing that delta
		// instead of a fresh full copy is the difference between 5 KiB and
		// 30 MiB on disk (Helix Core's +S binary delta storage).
		var deltaBuf bytes.Buffer
		sink := &capWriter{w: &deltaBuf, left: deltaInlineCap}
		var bodyReader io.Reader = r.Body
		if !isNew {
			// Only an edit can reuse the client delta; teeing a new file's
			// full text would allocate up to deltaInlineCap for nothing.
			bodyReader = io.TeeReader(r.Body, sink)
		}
		outPath, sum, sha, derr := decodeDeltaToFileSource(baseLen, baseSrc, bodyReader, maxPutBody)
		if derr != nil {
			if errors.Is(derr, errBodyTooLarge) {
				http.Error(w, derr.Error(), http.StatusRequestEntityTooLarge)
			} else {
				http.Error(w, "svndiff decode: "+derr.Error(), http.StatusBadRequest)
			}
			return
		}
		st, serr := os.Stat(outPath)
		if serr != nil {
			os.Remove(outPath)
			http.Error(w, serr.Error(), http.StatusInternalServerError)
			return
		}
		// Large text, small delta: store the delta instead of a fresh copy.
		// The target is always a full-text node, so a read stays one apply
		// (a chain would make reads and later uploads progressively slower).
		size := st.Size()
		useDelta := false
		if size > putStreamThreshold && !isNew && baseNode != nil {
			clientOK := deltaBuf.Len() > 0 && sink.ok() &&
				deltaBuf.Len() <= deltaInlineCap && int64(deltaBuf.Len()) < size/2
			// A chain (base is itself a delta) is fine up to the repository's
			// limit: an edit of a 1 GiB binary should stay a small delta even
			// after previous edits, not degrade to a full copy each time.
			// Reconstructing a chain costs one pass per level, so large texts
			// are compacted to a full text after a short chain instead of
			// paying dozens of gigabytes of temp I/O on every later edit.
			depthCap := repo.MaxDeltaDepth()
			if size > reencodeCap && depthCap > 8 {
				depthCap = 8
			}
			if clientOK && baseNode.DeltaDepth < depthCap {
				// The client diffed against a full text: reuse its stream.
				deltaBytes = append([]byte(nil), deltaBuf.Bytes()...)
				deltaBase = baseNode.Node
				useDelta = true
			} else if size <= reencodeCap {
				// Otherwise re-encode against the last full revision (one
				// ancestor lookup) so future reads and edits stay at depth 1.
				if anc, aerr := repo.FullTextBase(baseNode); aerr == nil && anc.Node != baseNode.Node {
					if newText, rerr := os.ReadFile(outPath); rerr == nil {
						if ancText, gerr := repo.GetText(anc.Node); gerr == nil {
							if d, eerr := svndiff.Encode(ancText, newText); eerr == nil &&
								len(d) <= deltaInlineCap && len(d) < len(newText)/2 {
								deltaBytes = d
								deltaBase = anc.Node
								useDelta = true
							}
						}
					}
				}
			}
			if useDelta {
				deltaTotal = uint64(size)
				dataMD5, hasMD5 = sum, true
				dataSHA1 = sha
				os.Remove(outPath)
			}
		}
		if !useDelta {
			if size > putStreamThreshold {
				ref, refSize, xerr := storeText(outPath, sum)
				if xerr != nil {
					http.Error(w, xerr.Error(), http.StatusInternalServerError)
					return
				}
				dataTextRef, dataTextSize, dataSize = ref, uint64(refSize), refSize
				dataSHA1 = sha
			} else {
				b, rerr := os.ReadFile(outPath)
				os.Remove(outPath)
				if rerr != nil {
					http.Error(w, rerr.Error(), http.StatusInternalServerError)
					return
				}
				data, dataSize = b, int64(len(b))
			}
		}
	} else if body, bodyFile, bodyMD5, bodySHA1, berr := readBody(r.Body, putStreamThreshold, maxPutBody); berr != nil {
		http.Error(w, berr.Error(), statusForSpool(berr))
		return
	} else if bodyFile != "" {
		ref, size, xerr := storeText(bodyFile, bodyMD5)
		if xerr != nil {
			http.Error(w, xerr.Error(), http.StatusInternalServerError)
			return
		}
		dataTextRef, dataTextSize, dataSize = ref, uint64(size), size
		dataSHA1 = bodySHA1
	} else {
		if isSvndiff {
			var out bytes.Buffer
			if derr := svndiff.ApplyToSource(baseLen, baseSrc, bytes.NewReader(body), &out); derr != nil {
				http.Error(w, "svndiff decode: "+derr.Error(), http.StatusBadRequest)
				return
			}
			body = out.Bytes()
		}
		data = body
		dataSize = int64(len(body))
	}

	s.mu.Lock()
	tx, ok = s.txns[res.uuid]
	if !ok {
		s.mu.Unlock()
		http.Error(w, "transaction does not exist", http.StatusConflict)
		return
	}
	ch := tx.changeFor(res.path)
	if old := ch.dataFile; old != "" {
		os.Remove(old)
	}
	ch.kind = fs.KindFile
	ch.gen++
	ch.data = data
	ch.dataFile = ""
	ch.dataSize = dataSize
	ch.streamed = false
	ch.dataTextRef = dataTextRef
	ch.dataTextSize = dataTextSize
	ch.dataSHA1 = dataSHA1
	ch.hasSHA1 = dataSHA1 != ([20]byte{})
	ch.dataMD5 = dataMD5
	ch.hasMD5 = hasMD5
	ch.delta = deltaBytes
	ch.deltaBase = deltaBase
	ch.deltaTotal = deltaTotal
	ch.deltaRef = ""
	ch.baseRev = clientRev
	ch.dataRef = ""
	if ch.props == nil {
		ch.props = map[string]string{}
	}
	s.mu.Unlock()
	s.persistChange(repo, actID, res.path)

	w.Header().Set("Location", s.absURL(r, res.repo, "/!svn/txn/"+tx.id+res.path))
	if isNew {
		w.WriteHeader(http.StatusCreated)
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
}

// putStreamThreshold: bodies above this are streamed to disk instead of being
// buffered. It matches the external-text threshold so large files go straight
// from the socket to their content-addressed object.
const putStreamThreshold = 1 << 20

// deltaInlineCap bounds the client delta kept inline in a node-rev. It is
// deliberately much larger than a typical small-file edit: a 1 MiB change to
// a multi-gigabyte binary must be stored as that 1 MiB delta, not as a fresh
// full copy (which is how a 1 GiB + 1 MiB edit used to cost 1 GiB of storage).
const deltaInlineCap = 64 << 20

// reencodeCap bounds the text size for re-encoding a delta against the last
// full revision. Larger texts still use the client's delta when its base is a
// full text, otherwise a full copy; that keeps memory bounded while never
// growing a chain beyond one link.
const reencodeCap = 32 << 20

var errBodyTooLarge = errors.New("file too large")

// readBody reads a PUT body, keeping at most threshold bytes in memory. Larger
// bodies are spilled to a temp file and returned by path.
func readBody(r io.Reader, threshold, limit int64) ([]byte, string, [16]byte, [20]byte, error) {
	var sum [16]byte
	var sha [20]byte
	head := make([]byte, threshold+1)
	n, err := io.ReadFull(r, head)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return nil, "", sum, sha, err
	}
	if int64(n) <= threshold {
		return head[:n], "", sum, sha, nil
	}
	f, cerr := createTemp("body")
	if cerr != nil {
		return nil, "", sum, sha, cerr
	}
	path := f.Name()
	h := md5.New()
	hs := sha1.New()
	dst := io.MultiWriter(f, h, hs)
	if _, werr := dst.Write(head[:n]); werr != nil {
		f.Close()
		os.Remove(path)
		return nil, "", sum, sha, werr
	}
	rest, cerr2 := io.Copy(dst, io.LimitReader(r, limit-int64(n)+1))
	cerr3 := f.Close()
	if cerr2 == nil {
		cerr2 = cerr3
	}
	if cerr2 != nil {
		os.Remove(path)
		return nil, "", sum, sha, cerr2
	}
	if int64(n)+rest > limit {
		os.Remove(path)
		return nil, "", sum, sha, errBodyTooLarge
	}
	copy(sum[:], h.Sum(nil))
	copy(sha[:], hs.Sum(nil))
	return nil, path, sum, sha, nil
}

func statusForSpool(err error) int {
	if errors.Is(err, errBodyTooLarge) {
		return http.StatusRequestEntityTooLarge
	}
	return http.StatusBadRequest
}

// rangeSource serves svndiff source windows from a full-text node with a
// sequential readahead, so applying a 1 MiB delta to a 1 GiB base costs one
// ranged object read per 8 MiB instead of one per 100 KiB window.
func rangeSource(repo *fs.Repo, nr *fs.NodeRev, size int64) svndiff.SourceFunc {
	const chunk = int64(8 << 20)
	var buf []byte
	var bufOff int64
	return func(off, length int64) ([]byte, error) {
		if off < 0 || length < 0 {
			return nil, errors.New("svndiff: bad source range")
		}
		if off >= bufOff && off+length <= bufOff+int64(len(buf)) {
			return buf[off-bufOff : off-bufOff+length], nil
		}
		want := chunk
		if length > want {
			want = length
		}
		if off+want > size {
			want = size - off
		}
		if want <= 0 {
			return nil, errors.New("svndiff: source range out of bounds")
		}
		b, err := repo.ReadTextRange(nr, off, want)
		if err != nil {
			return nil, err
		}
		if int64(len(b)) < length {
			return nil, errors.New("svndiff: short base text")
		}
		buf, bufOff = b, off
		return buf[:length], nil
	}
}

// materializeText reconstructs a delta-chain node's text into a temp file,
// one stored delta at a time. Each level's output is deleted once the next
// level has consumed it, so a chain of edits costs at most two temp files
// (plus the page cache) instead of the whole text in RAM.
func materializeText(repo *fs.Repo, nr *fs.NodeRev) (string, int64, func(), error) {
	noop := func() {}
	if nr.DeltaDepth == 0 {
		return "", 0, noop, errors.New("materializeText: node is not a delta")
	}
	if nr.Base == nil {
		return "", 0, noop, errors.New("materializeText: delta without base")
	}
	bn, err := repo.GetNodeRev(*nr.Base)
	if err != nil {
		return "", 0, noop, err
	}
	var src svndiff.SourceFunc
	var srcSize int64
	cleanupBase := noop
	if bn.DeltaDepth == 0 {
		srcSize, err = repo.TextSizeOf(bn)
		if err != nil {
			return "", 0, noop, err
		}
		src = rangeSource(repo, bn, srcSize)
	} else {
		basePath, baseSize, baseCleanup, merr := materializeText(repo, bn)
		if merr != nil {
			return "", 0, noop, merr
		}
		f, oerr := os.Open(basePath)
		if oerr != nil {
			baseCleanup()
			return "", 0, noop, oerr
		}
		srcSize = baseSize
		src = func(off, length int64) ([]byte, error) {
			if off < 0 || length < 0 || off+length > baseSize {
				return nil, errors.New("svndiff: base range out of bounds")
			}
			b := make([]byte, length)
			if _, rerr := f.ReadAt(b, off); rerr != nil {
				return nil, rerr
			}
			return b, nil
		}
		cleanupBase = func() {
			f.Close()
			os.Remove(basePath)
		}
	}
	out, terr := createTemp("base")
	if terr != nil {
		cleanupBase()
		return "", 0, noop, terr
	}
	outPath := out.Name()
	if aerr := svndiff.ApplyToSource(srcSize, src, bytes.NewReader(nr.Text), out); aerr != nil {
		out.Close()
		os.Remove(outPath)
		cleanupBase()
		return "", 0, noop, aerr
	}
	if cerr := out.Close(); cerr != nil {
		os.Remove(outPath)
		cleanupBase()
		return "", 0, noop, cerr
	}
	cleanupBase()
	st, serr := os.Stat(outPath)
	if serr != nil {
		os.Remove(outPath)
		return "", 0, noop, serr
	}
	return outPath, st.Size(), func() { os.Remove(outPath) }, nil
}

// hashSource computes the MD5 of a source text in fixed-size chunks.
func hashSource(src svndiff.SourceFunc, size int64) ([16]byte, error) {
	var sum [16]byte
	h := md5.New()
	const chunk = int64(8 << 20)
	for off := int64(0); off < size; {
		n := chunk
		if off+n > size {
			n = size - off
		}
		b, err := src(off, n)
		if err != nil {
			return sum, err
		}
		if int64(len(b)) != n {
			return sum, errors.New("svndiff: short base text")
		}
		h.Write(b)
		off += n
	}
	copy(sum[:], h.Sum(nil))
	return sum, nil
}

// decodeDeltaToFileSource streams a svndiff delta into a text temp file,
// failing once the decoded text exceeds limit (a small delta can expand
// arbitrarily). The source is read window-by-window.
func decodeDeltaToFileSource(srcLen int64, src svndiff.SourceFunc, delta io.Reader, limit int64) (string, [16]byte, [20]byte, error) {
	var sum [16]byte
	var sha [20]byte
	out, err := createTemp("text")
	if err != nil {
		return "", sum, sha, err
	}
	outPath := out.Name()
	h := md5.New()
	hs := sha1.New()
	if err := svndiff.ApplyToSource(srcLen, src, delta, &strictCapWriter{w: io.MultiWriter(out, h, hs), left: limit}); err != nil {
		out.Close()
		os.Remove(outPath)
		return "", sum, sha, err
	}
	if err := out.Close(); err != nil {
		os.Remove(outPath)
		return "", sum, sha, err
	}
	copy(sum[:], h.Sum(nil))
	copy(sha[:], hs.Sum(nil))
	return outPath, sum, sha, nil
}

// capWriter forwards writes until the byte budget is exhausted. In strict
// mode (used for decoded text) the overflow is an error; in tee mode it just
// marks the stream as too large and swallows the rest.
type capWriter struct {
	w        io.Writer
	left     int64
	overflow bool
}

func (c *capWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > c.left {
		c.overflow = true
		return len(p), nil // tee mode: stop capturing, keep the stream flowing
	}
	c.left -= int64(len(p))
	return c.w.Write(p)
}

func (c *capWriter) ok() bool { return !c.overflow }

// strictCapWriter fails the write once the budget is exhausted.
type strictCapWriter struct {
	w    io.Writer
	left int64
}

func (c *strictCapWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > c.left {
		return 0, errBodyTooLarge
	}
	c.left -= int64(len(p))
	return c.w.Write(p)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, res resource) {
	if res.typ == resActivity {
		var files []string
		s.mu.Lock()
		a, had := s.activities[res.uuid]
		delete(s.activities, res.uuid)
		if had && a.txn != nil {
			delete(s.txns, a.txn.id)
			for _, ch := range a.txn.changes {
				if ch.dataFile != "" {
					files = append(files, ch.dataFile)
				}
			}
		}
		s.mu.Unlock()
		for _, f := range files {
			os.Remove(f)
		}
		if repo, ok := s.repo(w, res); ok {
			s.dropActivityState(repo, res.uuid)
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if res.typ != resTxn {
		http.Error(w, "DELETE only applies to a transaction working resource", http.StatusBadRequest)
		return
	}
	if repo, ok := s.repo(w, res); ok {
		s.loadTxnChanges(repo, res.repo, res.uuid)
	} else {
		return
	}
	if e := s.getLockLocked(res.repo, res.path); e != nil && !lockTokenMatches(r.Header.Get("If"), e.token) {
		http.Error(w, "Locked", http.StatusLocked)
		return
	}
	s.mu.Lock()
	tx, ok := s.txns[res.uuid]
	if !ok {
		s.mu.Unlock()
		http.Error(w, "transaction does not exist", http.StatusConflict)
		return
	}
	ch := tx.changeFor(res.path)
	ch.action = fs.ActionDelete
	ch.baseRev = clientBaseRevision(r)
	actID := strings.TrimPrefix(tx.id, "txn-")
	s.mu.Unlock()
	if repo, ok := s.repo(w, res); ok {
		s.persistChange(repo, actID, res.path)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleMkcol(w http.ResponseWriter, r *http.Request, res resource) {
	if res.typ != resTxn {
		http.Error(w, "MKCOL only applies to a transaction working resource", http.StatusBadRequest)
		return
	}
	if repo, ok := s.repo(w, res); ok {
		s.loadTxnChanges(repo, res.repo, res.uuid)
	} else {
		return
	}
	s.mu.Lock()
	tx, ok := s.txns[res.uuid]
	if !ok {
		s.mu.Unlock()
		http.Error(w, "transaction does not exist", http.StatusConflict)
		return
	}
	ch := tx.changeFor(res.path)
	ch.kind = fs.KindDir
	if ch.props == nil {
		ch.props = map[string]string{}
	}
	actID := strings.TrimPrefix(tx.id, "txn-")
	s.mu.Unlock()
	if repo, ok := s.repo(w, res); ok {
		s.persistChange(repo, actID, res.path)
	}
	w.WriteHeader(http.StatusCreated)
}

// --- MERGE (finalize the commit) ---

func (s *Server) handleMerge(w http.ResponseWriter, r *http.Request, res resource) {
	// The client MERGEs to the shallowest modified path (repo root, a subdir,
	// or the VCC); the whole transaction is committed regardless of target.
	if res.typ != resVcc && res.typ != resRoot && res.typ != resPublic {
		http.Error(w, "MERGE must target a version-controlled resource", http.StatusBadRequest)
		return
	}
	repo, ok := s.repo(w, res)
	if !ok {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	actID := extractActivityFromMerge(body)
	if actID == "" {
		http.Error(w, "MERGE body must name a source activity", http.StatusBadRequest)
		return
	}
	s.loadTxnChanges(repo, res.repo, "txn-"+actID)
	s.mu.Lock()
	a, ok := s.activities[actID]
	if !ok || a.txn == nil {
		s.mu.Unlock()
		http.Error(w, "activity not found or not checked out", http.StatusConflict)
		return
	}
	tx := a.txn
	changes, cerr := collectChanges(repo, tx)
	if cerr != nil {
		s.mu.Unlock()
		http.Error(w, "commit data unavailable: "+cerr.Error(), http.StatusInternalServerError)
		return
	}
	// Streamed bodies live in temp files; they are consumed by the commit (or
	// dropped on any early exit), so clean them up when the request ends.
	defer func() {
		for _, c := range changes {
			if c.DataFile != "" {
				os.Remove(c.DataFile)
			}
		}
	}()
	// Per-path authorization: the individual requests were authorized against
	// their own resource, but a transaction can contain paths it never named
	// (server-side copies, MOVE destinations). Re-check every changed path,
	// plus read access to copy sources, exactly where mod_dav_svn does.
	if az := s.cfg.authzFor(res.repo); az != nil {
		user := IdentityFrom(r.Context()).Username
		for _, ch := range changes {
			if !az.Authorize(user, res.repo, ch.Path, true) {
				s.mu.Unlock()
				http.Error(w, "Access forbidden to "+ch.Path, http.StatusForbidden)
				return
			}
			if e := s.getLockLocked(res.repo, ch.Path); e != nil && !bytes.Contains(body, []byte(e.token)) {
				s.mu.Unlock()
				http.Error(w, "Locked: "+ch.Path, http.StatusLocked)
				return
			}
			if ch.Action == fs.ActionCopy && ch.CopyFromPath != "" &&
				!az.Authorize(user, res.repo, ch.CopyFromPath, false) {
				s.mu.Unlock()
				http.Error(w, "Access forbidden to "+ch.CopyFromPath, http.StatusForbidden)
				return
			}
		}
	}
	revProps := a.revProps
	if revProps == nil {
		revProps = map[string]string{}
	}
	// Server-side revision properties: svn:author (the authenticated user)
	// and svn:date (commit time). The client supplies svn:log via PROPPATCH.
	if revProps["svn:author"] == "" {
		if ident := IdentityFrom(r.Context()); ident.Username != "" {
			revProps["svn:author"] = ident.Username
		} else {
			revProps["svn:author"] = "anonymous"
		}
	}
	if revProps["svn:date"] == "" {
		revProps["svn:date"] = time.Now().UTC().Format("2006-01-02T15:04:05.000000Z")
	}
	// stop tracking
	delete(s.activities, actID)
	delete(s.txns, tx.id)
	s.mu.Unlock()

	user := IdentityFrom(r.Context()).Username
	// pre-commit hooks can veto the transaction (policy checks that authz
	// cannot express: log message rules, file names, sizes...). They run
	// outside the server lock so a slow hook does not block other requests.
	if s.cfg.HooksDir != "" {
		if herr := runHook(s.cfg.HooksDir, "pre-commit", res.repo, tx.id, map[string]string{
			"SVN_USER": user,
			"SVN_REPO": res.repo,
			"SVN_TXN":  tx.id,
		}, nil); herr != nil {
			writeHookFailure(w, herr.Error())
			return
		}
	}

	rev, err := repo.CommitTxn(tx.baseRev, changes, revProps)
	s.dropActivityState(repo, actID)
	if err != nil {
		log.Printf("CommitTxn failed: %v", err)
		for _, c := range changes {
			log.Printf("  change path=%q kind=%v action=%v textlen=%d", c.Path, c.Kind, c.Action, len(c.Data))
		}
		var ood *fs.OutOfDateError
		if errors.As(err, &ood) {
			writeOutOfDate(w, ood.Path)
			return
		}
		if errors.Is(err, fs.ErrOutOfDate) {
			writeOutOfDate(w, "/")
			return
		}
		http.Error(w, "commit failed: "+err.Error(), http.StatusConflict)
		return
	}
	if s.cfg.HooksDir != "" {
		go func(rev uint64) {
			if herr := runHook(s.cfg.HooksDir, "post-commit", res.repo, tx.id, map[string]string{
				"SVN_USER": user,
				"SVN_REPO": res.repo,
				"SVN_REV":  strconv.FormatUint(rev, 10),
			}, nil); herr != nil {
				log.Printf("post-commit hook: %v", herr)
			}
		}(rev)
	}
	// Always return a full merge-response so the client's XML parser gets a
	// root element; a bare 200 triggers "no element found" in ra_serf even when
	// X-SVN-Options: no-merge-response was requested.
	s.mergeResponse(w, res.repo, rev, changes)
}

func (s *Server) baseOf(r *http.Request, repo string) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	base := "/" + strings.Trim(s.cfg.BasePath, "/")
	return fmt.Sprintf("%s://%s%s/%s/", scheme, r.Host, base, repo)
}

// collectChanges converts a txn's pending changes into fs.Change list.
func collectChanges(repo *fs.Repo, tx *txn) ([]fs.Change, error) {
	paths := make([]string, 0, len(tx.changes))
	for p := range tx.changes {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	out := make([]fs.Change, 0, len(paths))
	for _, p := range paths {
		ch := tx.changes[p]
		if len(ch.delta) == 0 && ch.deltaRef != "" {
			b, err := repo.Store().Get(context.Background(), activityDataKey(repo, ch.deltaRef), nil)
			if err != nil {
				// Committing without the delta would silently write an empty
				// file, so refuse instead.
				return nil, fmt.Errorf("load delta %s: %w", ch.deltaRef, err)
			}
			ch.delta = b
		}
		if ch.dataTextRef == "" && ch.dataRef != "" && len(ch.data) == 0 && ch.dataFile == "" {
			if ch.streamed {
				if tmp, terr := createTemp("txn"); terr == nil {
					if _, gerr := repo.Store().GetTo(context.Background(), activityDataKey(repo, ch.dataRef), tmp); gerr == nil {
						ch.dataFile = tmp.Name()
					} else {
						os.Remove(tmp.Name())
					}
					tmp.Close()
				}
			} else {
				loadChangeData(repo, ch)
			}
		}
		action := ch.action
		// default action from presence in base revision
		if action == fs.ActionAdd && ch.kind == 0 {
			action = fs.ActionAdd
		}
		if action == fs.ActionAdd {
			if _, err := repo.Resolve(tx.baseRev, p); err == nil {
				action = fs.ActionModify
			}
		}
		out = append(out, fs.Change{
			Path:         p,
			Action:       action,
			Kind:         ch.kind,
			Data:         ch.data,
			Props:        ch.props,
			CopyFromPath: ch.copyFromPath,
			CopyFromRev:  ch.copyFromRev,
			BaseRev:      ch.baseRev,
			DataFile:     ch.dataFile,
			DataMD5:      ch.dataMD5,
			HasMD5:       ch.hasMD5,
			TextRef:      ch.dataTextRef,
			TextSize:     ch.dataTextSize,
			DataSHA1:     ch.dataSHA1,
			HasSHA1:      ch.hasSHA1,
			Delta:        ch.delta,
			DeltaBase:    ch.deltaBase,
			TextTotal:    ch.deltaTotal,
		})
	}
	return out, nil
}

// mergeResponse writes the <D:merge-response> containing the new revision.
// hrefs are emitted without a host (repo-relative paths) so the client's
// svn_urlpath__skip_ancestor(merge_url, href) / fspath canonicalization works.
func (s *Server) mergeResponse(w http.ResponseWriter, repo string, rev uint64, changes []fs.Change) {
	var b strings.Builder
	pathBase := "/" + strings.Trim(s.cfg.BasePath, "/") + "/" + repo
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
	b.WriteString(`<D:merge-response xmlns:D="` + NS_DAV + `">` + "\n")
	b.WriteString(`  <D:updated-set>` + "\n")
	// Baseline response carrying the new revision; the client reads
	// commit_info->revision only from a response whose resourcetype is
	// "baseline".
	bc := pathBase + `/!svn/bc/` + strconv.FormatUint(rev, 10)
	b.WriteString(`    <D:response>` + "\n")
	b.WriteString(`      <D:href>` + bc + `</D:href>` + "\n")
	b.WriteString(`      <D:propstat><D:prop>` + "\n")
	b.WriteString(`        <D:resourcetype><D:baseline/></D:resourcetype>` + "\n")
	b.WriteString(`        <D:version-name>` + strconv.FormatUint(rev, 10) + `</D:version-name>` + "\n")
	b.WriteString(`        <D:checked-in><D:href>` + bc + `</D:href></D:checked-in>` + "\n")
	b.WriteString(`      </D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat>` + "\n")
	b.WriteString(`    </D:response>` + "\n")
	seen := map[string]bool{}
	for _, c := range changes {
		if seen[c.Path] {
			continue
		}
		seen[c.Path] = true
		href := pathBase + c.Path
		kind := ""
		if c.Kind == fs.KindDir {
			kind = "<D:collection/>"
		}
		b.WriteString(`    <D:response>` + "\n")
		b.WriteString(`      <D:href>` + xmlEscape(href) + `</D:href>` + "\n")
		b.WriteString(`      <D:propstat><D:prop>` + "\n")
		b.WriteString(`        <D:resourcetype>` + kind + `</D:resourcetype>` + "\n")
		b.WriteString(`        <D:version-name>` + strconv.FormatUint(rev, 10) + `</D:version-name>` + "\n")
		b.WriteString(`        <D:checked-in><D:href>` + pathBase + `/!svn/bc/` + strconv.FormatUint(rev, 10) + c.Path + `</D:href></D:checked-in>` + "\n")
		b.WriteString(`      </D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat>` + "\n")
		b.WriteString(`    </D:response>` + "\n")
	}
	b.WriteString(`  </D:updated-set>` + "\n")
	b.WriteString(`</D:merge-response>` + "\n")
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, b.String())
}

// extractActivityFromMerge pulls the activity href from a <D:merge> body.
func extractActivityFromMerge(body []byte) string {
	var root struct {
		XMLName xml.Name
		Inner   string `xml:",innerxml"`
	}
	if err := xml.Unmarshal(body, &root); err != nil {
		return ""
	}
	// find <D:source><D:href>URL</D:href></D:source>
	var src struct {
		Href string `xml:"href"`
	}
	// naive: parse inner for source/href
	dec := xml.NewDecoder(strings.NewReader(root.Inner))
	inSource := false
	for {
		tok, err := dec.Token()
		if err != nil {
			break
		}
		switch t := tok.(type) {
		case xml.StartElement:
			if t.Name.Local == "source" {
				inSource = true
			}
		case xml.CharData:
			if inSource && strings.TrimSpace(string(t)) != "" {
				u := strings.TrimSpace(string(t))
				return extractActivityID(u)
			}
		case xml.EndElement:
			if t.Name.Local == "source" {
				inSource = false
			}
		}
	}
	_ = src
	return ""
}

// changeFor returns (creating if needed) the pending change for a path.
func (tx *txn) changeFor(path string) *txnChange {
	if path == "" || path == "/" {
		path = "/"
	}
	ch, ok := tx.changes[path]
	if !ok {
		ch = &txnChange{action: fs.ActionAdd}
		tx.changes[path] = ch
	}
	return ch
}
