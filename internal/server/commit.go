package server

import (
	"encoding/xml"
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
	s.mu.Lock()
	if _, ok := s.activities[res.uuid]; !ok {
		s.activities[res.uuid] = &activity{id: res.uuid, revProps: map[string]string{}}
	}
	s.mu.Unlock()
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
	}
	tx := a.txn
	s.mu.Unlock()

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
	sets, removes := parseProppatch(body)

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
			a.revProps[propName(p)] = p.value
		}
		s.mu.Unlock()
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
					a.revProps[propName(p)] = p.value
				}
				for _, p := range removes {
					delete(a.revProps, propName(p))
				}
			}
			s.mu.Unlock()
			// 207 multistatus acknowledging the revision props.
			var b strings.Builder
			b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
			b.WriteString(`<D:multistatus xmlns:D="DAV:" xmlns:svn="http://subversion.tigris.org/xmlns/svn/">` + "\n")
			b.WriteString(`  <D:response>` + "\n")
			b.WriteString(`    <D:href>` + s.absURL(r, res.repo, "/!svn/txn/"+tx.id+"/") + `</D:href>` + "\n")
			b.WriteString(`    <D:propstat><D:prop>` + "\n")
			for _, p := range sets {
				b.WriteString(`      <` + xmlEscape(propName(p)) + `>` + xmlEscape(p.value) + `</` + xmlEscape(propName(p)) + `>` + "\n")
			}
			b.WriteString(`    </D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat>` + "\n")
			b.WriteString(`  </D:response>` + "\n")
			b.WriteString(`</D:multistatus>` + "\n")
			w.Header().Set("Content-Type", "text/xml; charset=utf-8")
			w.WriteHeader(http.StatusMultiStatus)
			io.WriteString(w, b.String())
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
		for _, p := range sets {
			ch.props[propName(p)] = p.value
		}
		for _, p := range removes {
			delete(ch.props, propName(p))
		}
		s.mu.Unlock()
		// 207 multistatus acknowledging each prop
		var b strings.Builder
		b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
		b.WriteString(`<D:multistatus xmlns:D="` + NS_DAV + `">` + "\n")
		b.WriteString(`  <D:response>` + "\n")
		b.WriteString(`    <D:href>` + s.absURL(r, res.repo, "/!svn/txn/"+tx.id+res.path) + `</D:href>` + "\n")
		b.WriteString(`    <D:propstat><D:prop>` + "\n")
		for _, p := range sets {
			b.WriteString(`      <` + xmlEscape(propName(p)) + `>` + xmlEscape(p.value) + `</` + xmlEscape(propName(p)) + `>` + "\n")
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

type propSet struct {
	ns, local string
	value     string
}

func propName(p propSet) string {
	if p.ns == NS_SVN {
		return "svn:" + p.local
	}
	return p.local
}

// parseProppatch parses a DAV propertyupdate body into sets/removes.
func parseProppatch(body []byte) (sets []propSet, removes []propSet) {
	dec := xml.NewDecoder(strings.NewReader(string(body)))
	var curNS, curLocal string
	inSet := false
	for {
		tok, err := dec.Token()
		if err != nil {
			break
		}
		switch t := tok.(type) {
		case xml.StartElement:
			switch t.Name.Local {
			case "set":
				inSet = true
				curNS, curLocal = "", ""
			case "remove":
				inSet = false
				curNS, curLocal = "", ""
			case "prop":
				// container; children are the props themselves
			default:
				curNS, curLocal = t.Name.Space, t.Name.Local
			}
		case xml.CharData:
			if curLocal != "" {
				v := strings.TrimSpace(string(t))
				ps := propSet{ns: curNS, local: curLocal, value: v}
				if inSet {
					sets = append(sets, ps)
				} else {
					removes = append(removes, ps)
				}
				curNS, curLocal = "", ""
			}
		}
	}
	return sets, removes
}

// --- PUT / DELETE / MKCOL on a transaction working resource ---

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request, res resource) {
	if res.typ != resTxn {
		http.Error(w, "PUT only applies to a transaction working resource", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	tx, ok := s.txns[res.uuid]
	if !ok {
		s.mu.Unlock()
		http.Error(w, "transaction does not exist", http.StatusConflict)
		return
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, 512<<20))
	if err != nil {
		s.mu.Unlock()
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ch := tx.changeFor(res.path)
	ch.kind = fs.KindFile
	// Existing file at the base revision => this PUT modifies it (204);
	// otherwise it creates it (201).
	isNew := true
	if tx.baseRev > 0 {
		if repo, rok := s.openRepoOnly(w, res.repo); rok {
			if nr, err := repo.Resolve(tx.baseRev, res.path); err == nil && nr.Kind == fs.KindFile {
				isNew = false
				if strings.Contains(r.Header.Get("Content-Type"), "svndiff") {
					if b, err := repo.GetText(nr.Node); err == nil {
						// A modifying PUT carries an svndiff delta based on
						// the checked-out revision of this path.
						decoded, derr := svndiff.Apply(b, data)
						if derr != nil {
							s.mu.Unlock()
							http.Error(w, "svndiff decode: "+derr.Error(), http.StatusBadRequest)
							return
						}
						data = decoded
					}
				}
			}
		}
	}
	if isNew && strings.Contains(r.Header.Get("Content-Type"), "svndiff") {
		decoded, derr := svndiff.Apply(nil, data)
		if derr == nil {
			data = decoded
		} else {
			// A brand-new file has no base revision; the client may send either a
			// full delta against the empty base or the raw content. If decoding
			// fails, keep the raw body instead of aborting the whole commit.
			d0 := data
			if len(d0) > 64 {
				d0 = d0[:64]
			}
			os.WriteFile("/home/user/svngod/bad_delta_dump.bin", data, 0o644)
			s.cfg.Logger.Printf("PUT %s: svndiff decode ignored on new file: %v delta=%x", res.path, derr, d0)
		}
	}
	ch.data = data
	if ch.props == nil {
		ch.props = map[string]string{}
	}
	s.mu.Unlock()
	w.Header().Set("Location", s.absURL(r, res.repo, "/!svn/txn/"+tx.id+res.path))
	if isNew {
		w.WriteHeader(http.StatusCreated)
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, res resource) {
	if res.typ == resActivity {
		s.mu.Lock()
		delete(s.activities, res.uuid)
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if res.typ != resTxn {
		http.Error(w, "DELETE only applies to a transaction working resource", http.StatusBadRequest)
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
	s.mu.Unlock()
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
	s.mu.Unlock()
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

	rev, err := repo.CommitTxn(changes, revProps)
	if err != nil {
		log.Printf("CommitTxn failed: %v", err)
		for _, c := range changes {
			log.Printf("  change path=%q kind=%v action=%v textlen=%d", c.Path, c.Kind, c.Action, len(c.Data))
		}
		http.Error(w, "commit failed: "+err.Error(), http.StatusConflict)
		return
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
