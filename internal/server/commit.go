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
}

// txn is a checked-out working resource bound to an activity.
type txn struct {
	id      string
	baseRev uint64
	changes map[string]*txnChange
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
		s.activities[res.uuid] = &activity{id: res.uuid, revProps: map[string]string{}}
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
	s.mu.Lock()
	a, ok := s.activities[actID]
	if !ok {
		s.mu.Unlock()
		http.Error(w, "activity does not exist", http.StatusConflict)
		return
	}
	var repoHandle *fs.Repo
	if a.txn == nil {
		repo, rok := s.openRepoOnly(w, res.repo)
		if !rok {
			s.mu.Unlock()
			return
		}
		head, err := repo.Head()
		if err != nil {
			s.mu.Unlock()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		a.txn = &txn{id: "txn-" + actID, baseRev: head, changes: map[string]*txnChange{}}
		s.txns[a.txn.id] = a.txn
		repoHandle = repo
	} else if op := s.cfg.openerFor(res.repo); op != nil {
		if h, err := op(res.repo); err == nil {
			repoHandle = h
		}
	}
	tx := a.txn
	s.mu.Unlock()
	if repoHandle != nil {
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

// extractActivityID pulls the uuid out of a Destination activity URL.
func extractActivityID(dst string) string {
	i := strings.Index(dst, "/!svn/act/")
	if i < 0 {
		return ""
	}
	rest := dst[i+len("/!svn/act/"):]
	rest = strings.Trim(rest, "/")
	if j := strings.IndexAny(rest, "/?"); j >= 0 {
		rest = rest[:j]
	}
	return rest
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
	sets, removes := parsePropertyUpdate(body)

	switch res.typ {
	case resActivity:
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
		s.persistActivity(repo, strings.TrimPrefix(tx.id, "txn-"))
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
	repo, rok := s.repo(w, res)
	if !rok {
		return
	}
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
	var baseText []byte
	if clientRev > 0 {
		head, herr := repo.Head()
		if herr == nil {
			if hnr, err := repo.Resolve(head, res.path); err == nil {
				if hnr.Node.Rev > clientRev {
					writeOutOfDate(w, res.path)
					return
				}
			} else if _, err := repo.Resolve(clientRev, res.path); err == nil {
				writeOutOfDate(w, res.path) // deleted since the client's base
				return
			} else {
				clientRev = 0 // not present at the client's base either
			}
		}
		if clientRev > 0 {
			if nr, err := repo.Resolve(clientRev, res.path); err == nil && nr.Kind == fs.KindFile {
				isNew = false
				bt, gerr := repo.GetText(nr.Node)
				if gerr == nil {
					baseText = bt
					if want := r.Header.Get("X-SVN-Base-Fulltext-MD5"); want != "" {
						sum := md5.Sum(bt)
						if !strings.EqualFold(hex.EncodeToString(sum[:]), want) {
							writeOutOfDate(w, res.path)
							return
						}
					}
				}
			}
		}
	}
	_ = txnBase

	isSvndiff := strings.Contains(r.Header.Get("Content-Type"), "svndiff")
	var data []byte
	var dataSize int64
	var dataTextRef string
	var dataTextSize uint64
	var dataSHA1 [20]byte

	const maxPutBody = 512 << 20
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

	body, bodyFile, bodyMD5, bodySHA1, berr := readBody(r.Body, putStreamThreshold, maxPutBody)
	if berr != nil {
		http.Error(w, berr.Error(), statusForSpool(berr))
		return
	}
	if bodyFile != "" {
		// Large body: decode (svndiff) straight into a text temp, then keep it
		// as an external text object. Small bodies (the common delta case) stay
		// inline so edits of small files do not create per-revision objects.
		if isSvndiff {
			in, oerr := os.Open(bodyFile)
			if oerr != nil {
				os.Remove(bodyFile)
				http.Error(w, oerr.Error(), http.StatusInternalServerError)
				return
			}
			outPath, sum, sha, derr := decodeDeltaToFile(baseText, in)
			in.Close()
			os.Remove(bodyFile)
			if derr != nil {
				http.Error(w, "svndiff decode: "+derr.Error(), http.StatusBadRequest)
				return
			}
			st, serr := os.Stat(outPath)
			if serr != nil || st.Size() > maxPutBody {
				os.Remove(outPath)
				http.Error(w, "file too large", http.StatusRequestEntityTooLarge)
				return
			}
			if st.Size() > putStreamThreshold {
				ref, size, xerr := storeText(outPath, sum)
				if xerr != nil {
					http.Error(w, xerr.Error(), http.StatusInternalServerError)
					return
				}
				dataTextRef, dataTextSize, dataSize = ref, uint64(size), size
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
		} else {
			ref, size, xerr := storeText(bodyFile, bodyMD5)
			if xerr != nil {
				http.Error(w, xerr.Error(), http.StatusInternalServerError)
				return
			}
			dataTextRef, dataTextSize, dataSize = ref, uint64(size), size
			dataSHA1 = bodySHA1
		}
	} else {
		if isSvndiff {
			var derr error
			if isNew {
				body, derr = svndiff.Apply(nil, body)
			} else {
				body, derr = svndiff.Apply(baseText, body)
			}
			if derr != nil {
				http.Error(w, "svndiff decode: "+derr.Error(), http.StatusBadRequest)
				return
			}
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
	ch.data = data
	ch.dataFile = ""
	ch.dataSize = dataSize
	ch.streamed = false
	ch.dataTextRef = dataTextRef
	ch.dataTextSize = dataTextSize
	ch.dataSHA1 = dataSHA1
	ch.hasSHA1 = dataSHA1 != ([20]byte{})
	ch.baseRev = clientRev
	ch.dataRef = ""
	if ch.props == nil {
		ch.props = map[string]string{}
	}
	s.mu.Unlock()
	s.persistActivity(repo, actID)

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

// decodeDeltaToFile streams a spooled svndiff delta into a text temp file.
func decodeDeltaToFile(base []byte, delta io.Reader) (string, [16]byte, [20]byte, error) {
	var sum [16]byte
	var sha [20]byte
	out, err := createTemp("text")
	if err != nil {
		return "", sum, sha, err
	}
	outPath := out.Name()
	h := md5.New()
	hs := sha1.New()
	if err := svndiff.ApplyTo(base, delta, io.MultiWriter(out, h, hs)); err != nil {
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
		s.persistActivity(repo, actID)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleMkcol(w http.ResponseWriter, r *http.Request, res resource) {
	if res.typ != resTxn {
		http.Error(w, "MKCOL only applies to a transaction working resource", http.StatusBadRequest)
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
		s.persistActivity(repo, actID)
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
	s.mu.Lock()
	a, ok := s.activities[actID]
	if !ok || a.txn == nil {
		s.mu.Unlock()
		http.Error(w, "activity not found or not checked out", http.StatusConflict)
		return
	}
	tx := a.txn
	changes := collectChanges(repo, tx)
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
func collectChanges(repo *fs.Repo, tx *txn) []fs.Change {
	paths := make([]string, 0, len(tx.changes))
	for p := range tx.changes {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	out := make([]fs.Change, 0, len(paths))
	for _, p := range paths {
		ch := tx.changes[p]
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
		})
	}
	return out
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
