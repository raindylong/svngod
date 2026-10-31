package server

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"svngod/internal/fs"
)

// HTTPv2 transaction channel. It is enabled by default because ra_serf
// negotiates it when the OPTIONS response advertises SVN-Me-Resource, which
// cuts a commit from ~8 round trips (MKACTIVITY, PROPFIND, CHECKOUT per file,
// PROPPATCH, MERGE) to one POST + PUTs + MERGE. SVNGOD_HTTPV2=0 keeps a
// deployment on the v1 activity workflow for old clients with v2 bugs.
//
// mod_dav_svn advertises v2 through response headers, not the body:
//
//	SVN-Repository-Root: /svn/repo
//	SVN-Supported-Posts: create-txn
//	SVN-Supported-Posts: create-txn-with-props
//
// The client then POSTs !svn/me <S:create-txn/>, uploads to the returned txn
// URL with the existing PUT/MKCOL handlers, and finalizes the commit. The
// transaction id is the activity id, so persistence, restart recovery and the
// MERGE finalize path are shared with v1.
func httpV2Enabled() bool { return os.Getenv("SVNGOD_HTTPV2") != "0" }

func v2UUID() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func (s *Server) handlePostMe(w http.ResponseWriter, r *http.Request) {
	if !httpV2Enabled() {
		http.Error(w, "HTTPv2 disabled", http.StatusNotImplemented)
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
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// ra_serf posts the transaction request as a skel
	// (Content-Type: application/vnd.svn-skel); older/newer clients may use
	// the XML form, so accept both.
	if strings.Contains(r.Header.Get("Content-Type"), "svn-skel") ||
		(len(body) > 0 && body[0] == '(') {
		s.postMeSkel(w, r, repo, name, body)
		return
	}
	var root xmlEl
	if xml.Unmarshal(body, &root) != nil {
		http.Error(w, "bad POST body", http.StatusBadRequest)
		return
	}
	switch root.XMLName.Local {
	case "create-txn", "create-txn-with-props":
		props := map[string]string{}
		for _, c := range root.Children {
			if c.XMLName.Local == "rev-prop" {
				if pn := c.attr("name"); pn != "" && !strings.HasPrefix(pn, "svn:txn-") {
					props[pn] = strings.TrimSpace(c.Content)
				}
			}
		}
		s.createV2Txn(w, r, repo, name, props)
	case "commit-txn":
		txnName := ""
		if c, ok := root.child("txn-name"); ok {
			txnName = strings.TrimSpace(c.Content)
		}
		s.commitV2Txn(w, r, name, txnName)
	case "update-report":
		// HTTPv2 sends the update report as POST to the session URL. The
		// v1 REPORT implementation already parses this exact body.
		s.reportUpdate(w, repo, s.absURL(r, name, "/"), name, body)
	default:
		http.Error(w, "unsupported POST: "+root.XMLName.Local, http.StatusNotImplemented)
	}
}

// postMeSkel handles the skel-encoded HTTPv2 transaction requests.
//
// On the wire (SVN 1.14) the bodies look like:
//
//	(create-txn)
//	(create-txn-with-props (svn:txn-client-compat-version 6 1.14.1
//	                        svn:txn-user-agent 43 SVN/1.14.1 ...
//	                        svn:log 5 hello))
//
// where "6 1.14.1" is a length-prefixed atom (subversion writes the length,
// a separator, then exactly that many bytes). The client then PUTs to
// !svn/txr/<txn-name>/<path> and finalizes with MERGE whose source href is
// !svn/txn/<txn-name>.
func (s *Server) postMeSkel(w http.ResponseWriter, r *http.Request, repo *fs.Repo, name string, body []byte) {
	v, err := parseSkel(body)
	if err != nil {
		http.Error(w, "bad skel POST body", http.StatusBadRequest)
		return
	}
	list, ok := v.([]any)
	if !ok || len(list) == 0 {
		http.Error(w, "bad skel POST body", http.StatusBadRequest)
		return
	}
	cmd, _ := list[0].(string)
	switch cmd {
	case "create-txn", "create-txn-with-props":
		props := map[string]string{}
		if cmd == "create-txn-with-props" && len(list) > 1 {
			if pl, ok := list[1].([]any); ok {
				for i := 0; i+1 < len(pl); i += 2 {
					k, _ := pl[i].(string)
					val, _ := pl[i+1].(string)
					if k != "" && !strings.HasPrefix(k, "svn:txn-") {
						props[k] = val
					}
				}
			}
		}
		s.createV2Txn(w, r, repo, name, props)
	case "commit-txn":
		txnName := ""
		if len(list) > 1 {
			txnName, _ = list[1].(string)
		}
		s.commitV2Txn(w, r, name, txnName)
	case "update-report":
		s.reportUpdate(w, repo, s.absURL(r, name, "/"), name, body)
	default:
		http.Error(w, "unsupported POST: "+cmd, http.StatusNotImplemented)
	}
}

func (s *Server) createV2Txn(w http.ResponseWriter, r *http.Request, repo *fs.Repo, name string, props map[string]string) {
	head, herr := repo.HeadFresh()
	if herr != nil {
		http.Error(w, herr.Error(), http.StatusInternalServerError)
		return
	}
	uuid := v2UUID()
	if props == nil {
		props = map[string]string{}
	}
	act := &activity{id: uuid, repo: name, created: time.Now().Unix(), revProps: props,
		txn: &txn{id: "txn-" + uuid, baseRev: head, changes: map[string]*txnChange{}, changesLoaded: true}}
	s.mu.Lock()
	s.activities[uuid] = act
	s.txns[act.txn.id] = act.txn
	s.mu.Unlock()
	s.persistActivity(repo, uuid)
	// mod_dav_svn answers 201 with SVN-Txn-Name only; clients derive the txn
	// URL from the stub + name. Location is included for clients that prefer
	// to follow it.
	w.Header().Set("Location", s.absURL(r, name, "/!svn/txr/"+act.txn.id))
	w.Header().Set("SVN-Txn-Name", act.txn.id)
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) commitV2Txn(w http.ResponseWriter, r *http.Request, name, txnName string) {
	actID := strings.TrimPrefix(txnName, "txn-")
	if actID == "" || actID == txnName {
		http.Error(w, "commit-txn requires txn-name", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	_, exists := s.activities[actID]
	s.mu.Unlock()
	if !exists {
		http.Error(w, "transaction not found", http.StatusConflict)
		return
	}
	// Finalize through MERGE: hooks, authz, per-path OOD and the atomic
	// CommitTxn stay in one place.
	mergeBody := `<?xml version="1.0"?><D:merge xmlns:D="DAV:"><D:source><D:href>` +
		s.absURL(r, name, "/!svn/act/"+actID) + `</D:href></D:source></D:merge>`
	mr := r.Clone(r.Context())
	mr.Method = MethodMerge
	mr.Body = io.NopCloser(strings.NewReader(mergeBody))
	mr.ContentLength = int64(len(mergeBody))
	mr.URL.Path = strings.TrimSuffix(s.cfg.BasePath, "/") + "/" + name + "/"
	s.handleMerge(w, mr, resource{typ: resRoot, repo: name})
}

// --- skel parser ---

type skelParser struct {
	b []byte
	i int
}

func isSkelSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }

func (p *skelParser) skipSpace() {
	for p.i < len(p.b) && isSkelSpace(p.b[p.i]) {
		p.i++
	}
}

// parseSkel decodes a subversion skel into nested []any (lists) and string
// atoms. Both the classic "len:data" and the separator form "len data" that
// SVN 1.14 writes are accepted.
func parseSkel(b []byte) (any, error) {
	p := &skelParser{b: b}
	return p.parse()
}

func (p *skelParser) parse() (any, error) {
	p.skipSpace()
	if p.i >= len(p.b) {
		return nil, io.ErrUnexpectedEOF
	}
	if p.b[p.i] == '(' {
		p.i++
		var list []any
		for {
			p.skipSpace()
			if p.i >= len(p.b) {
				return nil, io.ErrUnexpectedEOF
			}
			if p.b[p.i] == ')' {
				p.i++
				return list, nil
			}
			v, err := p.parse()
			if err != nil {
				return nil, err
			}
			list = append(list, v)
		}
	}
	// Length-prefixed atom "len:data": consume exactly len bytes, which may
	// include spaces, so this must run before the generic token scan.
	if p.b[p.i] >= '0' && p.b[p.i] <= '9' {
		j := p.i
		for j < len(p.b) && p.b[j] >= '0' && p.b[j] <= '9' {
			j++
		}
		if j < len(p.b) && p.b[j] == ':' {
			if n, err := strconv.Atoi(string(p.b[p.i:j])); err == nil && j+1+n <= len(p.b) {
				s := string(p.b[j+1 : j+1+n])
				p.i = j + 1 + n
				return s, nil
			}
		}
	}
	start := p.i
	for p.i < len(p.b) && !isSkelSpace(p.b[p.i]) && p.b[p.i] != '(' && p.b[p.i] != ')' {
		p.i++
	}
	tok := string(p.b[start:p.i])
	if n, err := strconv.Atoi(tok); err == nil && n >= 0 && tok[0] >= '0' && tok[0] <= '9' {
		// Separator form "len data" written by SVN 1.14's ra_serf.
		if p.i < len(p.b) && isSkelSpace(p.b[p.i]) {
			save := p.i
			p.skipSpace()
			if p.i+n <= len(p.b) {
				s := string(p.b[p.i : p.i+n])
				p.i += n
				return s, nil
			}
			p.i = save
		}
	}
	return tok, nil
}
