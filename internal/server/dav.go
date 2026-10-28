package server

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"svngod/internal/fs"
)

// handleOptions advertises DeltaV capabilities, the repo UUID and root URL.
func (s *Server) handleOptions(w http.ResponseWriter, r *http.Request, res resource) {
	if res.repo == "" {
		s.notImpl(w, "OPTIONS")
		return
	}
	repo, ok := s.repo(w, res)
	if !ok {
		return
	}
	uuid, err := repo.UUID()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h := w.Header()
	h.Set("DAV", davCapabilities)
	h.Set("MS-Author-Via", "DAV")
	h.Set("SVN-Repository-UUID", uuid)
	// SVN-Repository-Root is HTTPv2-only; advertising it on a v1 server makes
	// ra_serf mis-detect v2 and derive the repos root incorrectly. v1 clients
	// take the repos root from the session URL instead.
	head, err := repo.Head()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.Set("SVN-Youngest-Rev", strconv.FormatUint(head, 10))
	h.Set("SVN-repos-Version", "1")
	h.Set("Allow", allowedMethods)
	h.Set("Content-Type", "text/xml; charset=utf-8")
	// ra_serf always parses the OPTIONS response body as XML
	// (options_response_handler -> expat parsing of S:options-response), so
	// an empty body makes clients fail with "Malformed XML: no element found".
	// We do NOT advertise HTTPv2 (no supported-posts etc.), so clients stay on v1.
	act := s.absURL(r, res.repo, "/!svn/act/")
	var b bytes.Buffer
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?>`)
	b.WriteString(`<D:options-response xmlns:D="DAV:">`)
	b.WriteString(`<D:activity-collection-set><D:href>`)
	b.WriteString(escapeXML(act))
	b.WriteString(`</D:href></D:activity-collection-set>`)
	b.WriteString(`</D:options-response>`)
	w.WriteHeader(http.StatusOK)
	w.Write(b.Bytes())
}

// handleGet serves a file's full text (GET on a version-controlled resource).
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, res resource) {
	repo, ok := s.repo(w, res)
	if !ok {
		return
	}
	rev, path, ok := s.resolveNode(w, repo, res)
	if !ok {
		return
	}
	nr, err := repo.Resolve(rev, path)
	if err != nil {
		if errors.Is(err, fs.ErrPathNotFound) {
			http.NotFound(w, r)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	if nr.Kind != fs.KindFile {
		http.Error(w, "GET only applies to files", http.StatusMethodNotAllowed)
		return
	}
	text, err := repo.GetText(nr.Node)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if mime, ok := nr.Props["svn:mime-type"]; ok && mime != "" {
		w.Header().Set("Content-Type", mime)
	} else {
		w.Header().Set("Content-Type", "text/plain")
	}
	w.Header().Set("ETag", fmt.Sprintf(`"%d/%s"`, nr.Node.Rev, nr.Node.IDKey()))
	w.Header().Set("Content-Length", strconv.Itoa(len(text)))
	w.Header().Set("SVN-Repository-UUID", repoUUID(repo))
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(text)
}

// errPathNotFound marks an unresolvable path (mapped to 404).

// resolveNode maps a resource to a concrete (rev, path) inside the repo.
// It returns ok=false after writing an error response.
func (s *Server) resolveNode(w http.ResponseWriter, repo *fs.Repo, res resource) (uint64, string, bool) {
	head, err := repo.Head()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return 0, "", false
	}
	rev := head
	switch res.typ {
	case resRoot, resVcc, resPublic, resMe:
		rev = head
	case resBaselineColl, resBaseline, resRev, resVersion:
		rev = res.rev
	default:
		http.Error(w, "unsupported resource", http.StatusBadRequest)
		return 0, "", false
	}
	if res.rev > head {
		http.Error(w, "revision does not exist", http.StatusBadRequest)
		return 0, "", false
	}
	path := res.path
	if path == "" {
		path = "/"
	}
	return rev, path, true
}

// handlePropfind serves DAV property discovery used by svn for checkout,
// list, info, and baseline discovery.
func (s *Server) handlePropfind(w http.ResponseWriter, r *http.Request, res resource) {
	repo, ok := s.repo(w, res)
	if !ok {
		return
	}
	// parse requested props (allprop if empty body / no propfind element)
	props, err := parsePropfind(r.Body)
	if err != nil {
		http.Error(w, "bad propfind: "+err.Error(), http.StatusBadRequest)
		return
	}
	depth := r.Header.Get("Depth")
	if depth == "" {
		depth = "0"
	}
	if depth != "0" && depth != "1" && depth != "infinity" {
		http.Error(w, "invalid Depth", http.StatusBadRequest)
		return
	}

	rev, path, ok := s.resolveNode(w, repo, res)
	if !ok {
		return
	}
	base := s.absURL(r, res.repo, "/")

	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
	b.WriteString(`<D:multistatus ` + rootAttr() + ">\n")

	switch res.typ {
	case resVcc:
		s.propVcc(&b, repo, base, res.repo, rev, path, props)
	case resRoot:
		s.propRoot(&b, repo, base, res.repo, rev, path, props)
	case resBaseline:
		s.propBaselineColl(&b, repo, base, res.repo, rev, props)
		// depth 1/infinity: list the baseline's root directory children.
		if depth == "1" || depth == "infinity" {
			if err := s.propChildren(&b, repo, base, res.repo, rev, "/", props, depth); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	default:
		if err := s.propNode(&b, repo, base, res.repo, rev, path, props, false); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// depth 1/infinity: children
		if depth == "1" || depth == "infinity" {
			if err := s.propChildren(&b, repo, base, res.repo, rev, path, props, depth); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}

	b.WriteString("</D:multistatus>\n")
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.Header().Set("DAV", davCapabilities)
	w.WriteHeader(http.StatusMultiStatus)
	io.WriteString(w, b.String())
}

// propRoot writes the version-controlled-collection root discovery response.
// svn PROPFINDs the repo root (depth 0) to find the vcc href.
func (s *Server) propRoot(b *strings.Builder, repo *fs.Repo, base, name string, rev uint64, path string, props []Name) {
	b.WriteString("  <D:response>\n")
	b.WriteString("    <D:href>" + strings.TrimSuffix(base, "/") + "</D:href>\n")
	b.WriteString("    <D:propstat>\n      <D:prop>\n")
	b.WriteString("      <D:version-controlled-configuration><D:href>" + base + "!svn/vcc/default</D:href></D:version-controlled-configuration>\n")
	b.WriteString("      <D:baseline-collection><D:href>" + s.baselineHrefPath(base, rev) + "</D:href></D:baseline-collection>\n")
	b.WriteString("      <D:checked-in><D:href>" + base + "!svn/bln/" + strconv.FormatUint(rev, 10) + "</D:href></D:checked-in>\n")
	b.WriteString("      <S:baseline-relative-path></S:baseline-relative-path>\n")
	b.WriteString("      <D:resourcetype><D:collection/></D:resourcetype>\n")
	b.WriteString("      <S:repository-uuid>" + repoUUID(repo) + "</S:repository-uuid>\n")
	b.WriteString("      </D:prop>\n      <D:status>HTTP/1.1 200 OK</D:status>\n    </D:propstat>\n")
	b.WriteString("  </D:response>\n")
}

// propVcc writes the version-controlled-configuration response.
func (s *Server) propVcc(b *strings.Builder, repo *fs.Repo, base, name string, rev uint64, path string, props []Name) {
	b.WriteString("  <D:response>\n")
	b.WriteString("    <D:href>" + base + "!svn/vcc/default</D:href>\n")
	b.WriteString("    <D:propstat>\n      <D:prop>\n")
	b.WriteString("      <D:version-controlled-configuration><D:href>" + base + "!svn/vcc/default</D:href></D:version-controlled-configuration>\n")
	b.WriteString("      <D:baseline-collection><D:href>" + s.baselineHrefPath(base, rev) + "</D:href></D:baseline-collection>\n")
	b.WriteString("      <D:checked-in><D:href>" + base + "!svn/bln/" + strconv.FormatUint(rev, 10) + "</D:href></D:checked-in>\n")
	b.WriteString("      <S:baseline-relative-path></S:baseline-relative-path>\n")
	b.WriteString("      <D:resourcetype><D:version-controlled-configuration/></D:resourcetype>\n")
	b.WriteString("      <S:repository-uuid>" + repoUUID(repo) + "</S:repository-uuid>\n")
	b.WriteString("      </D:prop>\n      <D:status>HTTP/1.1 200 OK</D:status>\n    </D:propstat>\n")
	b.WriteString("  </D:response>\n")
}

// propBaselineColl writes the baseline collection (!svn/bln/<rev>) response.
// baselineHrefPath returns a host-less canonical path form of the baseline URL,
// e.g. /svn/demo/!svn/bln/1 — canonical fspath so ra_serf skip_ancestor won't assert.

// basePathHref returns a host-less canonical path form: /svn/demo/dir etc.
func (s *Server) basePathHref(base, rel string) string {
	u, err := url.Parse(strings.TrimSuffix(base, "/"))
	if err != nil || u.Path == "" {
		return strings.TrimSuffix(base, "/") + "/" + rel
	}
	return strings.TrimSuffix(u.Path, "/") + "/" + strings.TrimPrefix(rel, "/")
}

func (s *Server) baselineHrefPath(base string, rev uint64) string {
	u, err := url.Parse(strings.TrimSuffix(base, "/"))
	if err != nil || u.Path == "" {
		return strings.TrimSuffix(base, "/") + "!svn/bln/" + strconv.FormatUint(rev, 10)
	}
	return strings.TrimSuffix(u.Path, "/") + "/!svn/bln/" + strconv.FormatUint(rev, 10)
}

func (s *Server) propBaselineColl(b *strings.Builder, repo *fs.Repo, base, name string, rev uint64, props []Name) {
	b.WriteString("  <D:response>\n")
	b.WriteString("    <D:href>" + s.baselineHrefPath(base, rev) + "</D:href>\n")
	b.WriteString("    <D:propstat>\n      <D:prop>\n")
	b.WriteString("      <D:resourcetype><D:collection/><D:baseline/></D:resourcetype>\n")
	b.WriteString("      <D:version-name>" + strconv.FormatUint(rev, 10) + "</D:version-name>\n")
	b.WriteString("      <D:baseline-collection><D:href>" + s.baselineHrefPath(base, rev) + "</D:href></D:baseline-collection>\n")
	b.WriteString("      <S:baseline-relative-path></S:baseline-relative-path>\n")
	b.WriteString("      <D:checked-in><D:href>" + base + "!svn/bln/" + strconv.FormatUint(rev, 10) + "</D:href></D:checked-in>\n")
	b.WriteString("      <S:repository-uuid>" + repoUUID(repo) + "</S:repository-uuid>\n")
	b.WriteString("      </D:prop>\n      <D:status>HTTP/1.1 200 OK</D:status>\n    </D:propstat>\n")
	b.WriteString("  </D:response>\n")
}

// propNode writes props for a concrete node (file or dir) at (rev,path).
func (s *Server) propNode(b *strings.Builder, repo *fs.Repo, base, name string, rev uint64, path string, props []Name, publicHref bool) error {
	nr, err := repo.Resolve(rev, path)
	if err != nil {
		if errors.Is(err, fs.ErrPathNotFound) {
			// resource does not exist at this rev: emit 404 propstat
			b.WriteString("  <D:response>\n")
			b.WriteString("    <D:href>" + base + privateHref(rev, path) + "</D:href>\n")
			b.WriteString("    <D:status>HTTP/1.1 404 Not Found</D:status>\n")
			b.WriteString("  </D:response>\n")
			return nil
		}
		return err
	}
	href := base + privateHref(rev, path)
	if publicHref {
		// Directory listing children use a canonical public path so the
		// client's svn_fspath__skip_ancestor sees a canonical parent.
		href = s.basePathHref(base, "!svn/bln/"+strconv.FormatUint(rev, 10)+"/"+strings.TrimPrefix(path, "/"))
	}
	b.WriteString("  <D:response>\n")
	b.WriteString("    <D:href>" + href + "</D:href>\n")
	b.WriteString("    <D:propstat>\n      <D:prop>\n")
	// resourcetype
	if nr.Kind == fs.KindDir {
		b.WriteString("      <D:resourcetype><D:collection/></D:resourcetype>\n")
	} else {
		b.WriteString("      <D:resourcetype/>\n")
	}
	if e := s.getLock(name, path); e != nil {
		writeActivelock(b, e)
	}
	b.WriteString("      <D:getetag>" + etagFor(nr) + "</D:getetag>\n")
	b.WriteString("      <D:version-controlled-configuration><D:href>" + base + "!svn/vcc/default</D:href></D:version-controlled-configuration>\n")
	b.WriteString("      <D:version-name>" + strconv.FormatUint(rev, 10) + "</D:version-name>\n")
	b.WriteString("      <D:checked-in><D:href>" + base + "!svn/bln/" + strconv.FormatUint(rev, 10) + "</D:href></D:checked-in>\n")
	b.WriteString("      <S:baseline-relative-path>" + escapePath(strings.TrimPrefix(path, "/")) + "</S:baseline-relative-path>\n")
	b.WriteString("      <D:creator-displayname>" + propVal(nr.Props, "svn:author") + "</D:creator-displayname>\n")
	writeSvnEntryProps(b, nr, rev, path)
	if nr.Kind == fs.KindFile {
		text, err := repo.GetText(nr.Node)
		if err != nil {
			return err
		}
		b.WriteString("      <D:getcontentlength>" + strconv.Itoa(len(text)) + "</D:getcontentlength>\n")
		mime := propVal(nr.Props, "svn:mime-type")
		if mime == "" {
			mime = "text/plain"
		}
		b.WriteString("      <D:getcontenttype>" + mime + "</D:getcontenttype>\n")
		b.WriteString("      <svn:size>" + strconv.Itoa(len(text)) + "</svn:size>\n")
	} else {
		children, err := repo.ListDir(rev, path)
		if err == nil {
			b.WriteString("      <D:getcontentlength>" + strconv.Itoa(len(children)) + "</D:getcontentlength>\n")
		}
	}
	b.WriteString("      </D:prop>\n      <D:status>HTTP/1.1 200 OK</D:status>\n    </D:propstat>\n")
	b.WriteString("  </D:response>\n")
	return nil
}

// propChildren writes immediate children (depth 1) or full subtree (infinity).
func (s *Server) propChildren(b *strings.Builder, repo *fs.Repo, base, name string, rev uint64, path string, props []Name, depth string) error {
	nr, err := repo.Resolve(rev, path)
	if err != nil {
		return nil // not a dir
	}
	if nr.Kind != fs.KindDir {
		return nil
	}
	children, err := repo.ListDir(rev, path)
	if err != nil {
		return err
	}
	for _, c := range children {
		cpath := path + "/" + c.Name
		if err := s.propNode(b, repo, base, name, rev, cpath, props, true); err != nil {
			return err
		}
		if depth == "infinity" {
			cnr, err := repo.Resolve(rev, cpath)
			if err == nil && cnr.Kind == fs.KindDir {
				if err := s.propChildren(b, repo, base, name, rev, cpath, props, depth); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// --- propfind request parsing ---

func parsePropfind(body io.Reader) ([]Name, error) {
	data, err := io.ReadAll(io.LimitReader(body, 1<<20))
	if err != nil {
		return nil, err
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil, nil // allprop default
	}
	var top struct {
		XMLName xml.Name
		Inner   string `xml:",innerxml"`
	}
	if err := xml.Unmarshal(data, &top); err != nil {
		return nil, err
	}
	// Look for <D:prop> children; if none, it's allprop/propname.
	var props []Name
	dec := xml.NewDecoder(strings.NewReader(top.Inner))
	for {
		tok, err := dec.Token()
		if err != nil {
			break
		}
		if se, ok := tok.(xml.StartElement); ok {
			props = append(props, Name{NS: se.Name.Space, Local: se.Name.Local})
		}
	}
	return props, nil
}

// --- misc helpers ---

func repoUUID(repo *fs.Repo) string {
	u, err := repo.UUID()
	if err != nil {
		return ""
	}
	return u
}

// privateHref renders the private URL for a node: revision-pinned href.
func privateHref(rev uint64, path string) string {
	if path == "/" || path == "" {
		return "!svn/rev/" + strconv.FormatUint(rev, 10) + "/"
	}
	return "!svn/rev/" + strconv.FormatUint(rev, 10) + "/" + strings.TrimPrefix(path, "/")
}

func etagFor(nr *fs.NodeRev) string {
	return fmt.Sprintf(`"%d/%s"`, nr.Node.Rev, nr.Node.IDKey())
}

func propVal(props map[string]string, k string) string {
	if v, ok := props[k]; ok {
		return v
	}
	return ""
}

func escapePath(p string) string {
	if p == "" {
		return ""
	}
	return xmlEscape(p)
}

func xmlEscape(s string) string {
	var b strings.Builder
	xml.EscapeText(&b, []byte(s))
	return b.String()
}

// writeSvnEntryProps emits the svn:* entry properties a client expects.
func writeSvnEntryProps(b *strings.Builder, nr *fs.NodeRev, rev uint64, path string) {
	kind := "file"
	if nr.Kind == fs.KindDir {
		kind = "dir"
	}
	b.WriteString("      <svn:entry rev=\"" + strconv.FormatUint(rev, 10) + "\" kind=\"" + kind + "\" >\n")
	b.WriteString("        <svn:node-kind>" + kind + "</svn:node-kind>\n")
	b.WriteString("        <svn:path>" + xmlEscape(path) + "</svn:path>\n")
	if nr.CopyFrom != nil {
		b.WriteString("        <svn:copy-from rev=\"" + strconv.FormatUint(nr.CopyFrom.Rev, 10) + "\">" + xmlEscape(nr.CopyFromPath) + "</svn:copy-from>\n")
	}
	for k, v := range nr.Props {
		if strings.HasPrefix(k, "svn:") && k != "svn:date" {
			b.WriteString("        <svn:prop name=\"" + xmlEscape(k) + "\">" + xmlEscape(v) + "</svn:prop>\n")
		}
	}
	b.WriteString("      </svn:entry>\n")
}

// The remaining methods (REPORT, MKACTIVITY, commit flow, lock) are built in
// later increments; these stubs keep the router complete.


// handleCopyMove implements COPY/MOVE into a transaction working resource.
// Source is either a version baseline (!svn/bc/<rev>/<path>) or an already
// checked-out resource inside the same transaction. Destination header names
// the target working resource.
func (s *Server) handleCopyMove(w http.ResponseWriter, r *http.Request, res resource) {
	dst := r.Header.Get("Destination")
	if dst == "" {
		http.Error(w, "COPY/MOVE requires a Destination header", http.StatusBadRequest)
		return
	}
	u, err := url.Parse(dst)
	if err != nil {
		http.Error(w, "bad Destination header", http.StatusBadRequest)
		return
	}
	dres := parseResource(s.cfg.BasePath, u.Path)
	if dres.typ != resTxn {
		http.Error(w, "COPY destination must be a transaction working resource", http.StatusBadRequest)
		return
	}

	// srcPlans accumulates the materializations needed in the destination txn.
	var srcPlans []srcPlan
	switch res.typ {
	case resBaseline, resBaselineColl:
		repo, ok := s.repo(w, res)
		if !ok {
			return
		}
		nr, err := repo.Resolve(res.rev, res.path)
		if err != nil {
			http.Error(w, "copy source: "+err.Error(), http.StatusNotFound)
			return
		}
		// A single ActionCopy carries copyfrom; applyChange materializes the
		// whole subtree from history (including children + contents).
		srcPlans = []srcPlan{{dst: dres.path, kind: nr.Kind, cfPath: res.path, cfRev: res.rev, action: fs.ActionCopy}}
	case resTxn:
		s.mu.Lock()
		stx, ok := s.txns[res.uuid]
		if !ok {
			s.mu.Unlock()
			http.Error(w, "source transaction not found", http.StatusNotFound)
			return
		}
		srcCh, ok := stx.changes[res.path]
		if !ok {
			s.mu.Unlock()
			http.Error(w, "copy source not in transaction", http.StatusNotFound)
			return
		}
		srcPlans = []srcPlan{{src: res.path, dst: dres.path, kind: srcCh.kind, data: srcCh.data, props: copyMap(srcCh.props)}}
		s.mu.Unlock()
	default:
		http.Error(w, "COPY source not supported", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	tx, ok := s.txns[dres.uuid]
	if !ok {
		s.mu.Unlock()
		http.Error(w, "destination transaction not found", http.StatusNotFound)
		return
	}
	for _, p := range srcPlans {
		ch := tx.changeFor(p.dst)
		ch.action = p.action
		ch.kind = p.kind
		ch.data = p.data
		ch.props = p.props
		ch.copyFromPath = p.cfPath
		ch.copyFromRev = p.cfRev
	}
	// MOVE additionally removes the source.
	if r.Method == "MOVE" && res.typ == resTxn && res.uuid == dres.uuid {
		repo, rok := s.repo(w, res)
		if rok {
			if _, err := repo.Resolve(tx.baseRev, res.path); err == nil {
				// source existed in base → mark it deleted
				src := tx.changeFor(res.path)
				src.action = fs.ActionDelete
			} else {
				// source was created inside this txn → drop it entirely
				delete(tx.changes, res.path)
			}
		}
	}
	s.mu.Unlock()
	w.Header().Set("Location", dst)
	w.WriteHeader(http.StatusCreated)
}

// srcPlan is one materialized destination node for a COPY.
type srcPlan struct {
	src    string // source svn path (for error reporting)
	dst    string // destination svn path
	action fs.ChangeAction
	kind   byte
	data   []byte
	props  map[string]string
	cfPath string
	cfRev  uint64
}


func copyMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// escapeXML escapes a string for embedding in XML text content.
func escapeXML(s string) string {
	var b bytes.Buffer
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}
