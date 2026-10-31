package server

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"svngod/internal/fs"
)

// maxPropfindNodes bounds depth=infinity recursion so a hostile or accidental
// recursive listing of a huge tree cannot exhaust server memory/CPU.
const maxPropfindNodes = 1 << 20

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
	writeDAVCapabilities(h)
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
	// svngocli negotiates the bulk commit channel through this capability.
	h.Set("X-Svngod-Bundle", "1")
	if httpV2Enabled() {
		// HTTPv2 negotiation, byte-for-byte as mod_dav_svn 1.14 sends it:
		// repository-relative URLs without a trailing slash (ra_serf asserts
		// that stub values are canonical svn paths), and the supported POST
		// transaction types. rev/rev-root and txn/txn-root are distinct stubs:
		// the client joins a revision or txn name onto them.
		pfx := strings.TrimSuffix(s.cfg.BasePath, "/") + "/" + res.repo
		h.Set("SVN-Repository-Root", pfx)
		h.Set("SVN-Me-Resource", pfx+"/!svn/me")
		h.Set("SVN-Rev-Root-Stub", pfx+"/!svn/rvr")
		h.Set("SVN-Rev-Stub", pfx+"/!svn/rev")
		h.Set("SVN-Txn-Root-Stub", pfx+"/!svn/txr")
		h.Set("SVN-Txn-Stub", pfx+"/!svn/txn")
		h.Add("SVN-Supported-Posts", "create-txn")
		h.Add("SVN-Supported-Posts", "create-txn-with-props")
	}
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
	size, err := repo.TextSizeOf(nr)
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
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.Header().Set("SVN-Repository-UUID", repoUUID(repo))
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	// Range support: large external texts are read with a ranged object
	// request; inline texts are sliced from the reconstructed bytes.
	if rng := r.Header.Get("Range"); rng != "" {
		if start, end, ok := parseRange(rng, int(size)); ok {
			part, rerr := repo.ReadTextRange(nr, int64(start), int64(end-start+1))
			if rerr != nil {
				http.Error(w, rerr.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
			w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
			w.WriteHeader(http.StatusPartialContent)
			w.Write(part)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	if _, serr := repo.StreamText(nr.Node, w); serr != nil {
		log.Printf("GET %s: stream: %v", path, serr)
	}
}

// parseRange parses a single "bytes=start-end" range (suffix ranges bytes=-N
// are accepted by treating -N as "last N bytes"). Returns ok=false if the
// header is absent, malformed, or unsatifiable (multi-range not supported).
func parseRange(h string, size int) (start, end int, ok bool) {
	p := strings.TrimPrefix(strings.TrimSpace(h), "bytes=")
	if p == h {
		return 0, 0, false
	}
	dash := strings.IndexByte(p, '-')
	if dash < 0 {
		return 0, 0, false
	}
	sStr, eStr := p[:dash], p[dash+1:]
	if eStr == "" {
		// bytes=start- : from start to EOF
		st, err := strconv.Atoi(sStr)
		if err != nil || st < 0 || st >= size {
			return 0, 0, false
		}
		return st, size - 1, true
	}
	if sStr == "" {
		// bytes=-N : last N bytes
		n, err := strconv.Atoi(eStr)
		if err != nil || n <= 0 || size == 0 {
			return 0, 0, false
		}
		if n > size {
			n = size
		}
		return size - n, size - 1, true
	}
	st, err1 := strconv.Atoi(sStr)
	en, err2 := strconv.Atoi(eStr)
	if err1 != nil || err2 != nil || st < 0 || en < st || st >= size {
		return 0, 0, false
	}
	if en >= size {
		en = size - 1
	}
	return st, en, true
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
		log.Printf("resolveNode %s typ=%v -> unsupported", res.path, res.typ)
		http.Error(w, "unsupported resource", http.StatusBadRequest)
		return 0, "", false
	}
	if res.rev > head {
		log.Printf("resolveNode %s res.rev=%d head=%d -> not exist", res.path, res.rev, head)
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
	pbody, _ := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	dprops, propnames := parsePropfindRequest(pbody)
	props := make([]Name, len(dprops))
	for i := range dprops {
		props[i] = Name{NS: dprops[i].ns, Local: dprops[i].local}
	}
	// The svn client sets a Label header to the WC baseline revision on the
	// PROPFINDs it issues during a commit (e.g. Label: 89). Treat a numeric
	// Label as the revision to resolve, exactly like a baseline URL.
	labelRev := uint64(0)
	if lbl := r.Header.Get("Label"); lbl != "" {
		if n, err := strconv.ParseUint(strings.TrimSpace(lbl), 10, 64); err == nil && n > 0 {
			labelRev = n
		}
	}
	depth := r.Header.Get("Depth")
	if depth == "" {
		depth = "0"
	}
	if depth != "0" && depth != "1" && depth != "infinity" {
		log.Printf("PROPFIND %s invalid Depth=%q -> 400", r.URL.Path, depth)
		http.Error(w, "invalid Depth", http.StatusBadRequest)
		return
	}

	rev, path, ok := s.resolveNode(w, repo, res)
	if !ok {
		return
	}
	if labelRev > 0 && labelRev <= rev {
		rev = labelRev
	}
	base := s.absURL(r, res.repo, "/")

	var b strings.Builder
	budget := 0
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
	b.WriteString(`<D:multistatus ` + rootAttr() + ">\n")

	switch res.typ {
	case resVcc:
		s.propVcc(&b, repo, base, res.repo, rev, path, props)
	case resRoot:
		s.propRoot(&b, repo, base, res.repo, rev, path, props)
	case resBaseline:
		// !svn/bln/<rev> is the baseline collection; !svn/bln/<rev>/<path>
		// addresses a concrete node inside it.
		if res.path != "" && res.path != "/" {
			children := depth == "1" || depth == "infinity"
			// When listing, parent href must use the same !svn/bln prefix as its
			// children, else the client's svn_fspath__skip_ancestor assertion fails.
			if err := s.propNode(&b, repo, base, res.repo, rev, path, nil, props, propnames, children); err != nil {
				if errors.Is(err, fs.ErrPathNotFound) {
					http.Error(w, "Not Found", http.StatusNotFound)
					return
				}
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if children {
				if err := s.propChildren(&b, repo, base, res.repo, rev, path, props, depth, &budget, IdentityFrom(r.Context()).Username); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
			}
		} else {
			s.propBaselineColl(&b, repo, base, res.repo, rev, props)
			// depth 1/infinity: list the baseline's root directory children.
			if depth == "1" || depth == "infinity" {
				if err := s.propChildren(&b, repo, base, res.repo, rev, "/", props, depth, &budget, IdentityFrom(r.Context()).Username); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
			}
		}
	default:
		if err := s.propNode(&b, repo, base, res.repo, rev, path, nil, props, propnames, depth == "1" || depth == "infinity"); err != nil {
			if errors.Is(err, fs.ErrPathNotFound) {
				http.Error(w, "Not Found", http.StatusNotFound)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// depth 1/infinity: children
		if depth == "1" || depth == "infinity" {
			if err := s.propChildren(&b, repo, base, res.repo, rev, path, props, depth, &budget, IdentityFrom(r.Context()).Username); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}

	b.WriteString("</D:multistatus>\n")
	if e := s.getLock(res.repo, path); e != nil {
		w.Header().Set("X-SVN-Lock-Owner", e.owner)
		w.Header().Set("X-SVN-Creation-Date", e.created.UTC().Format("2006-01-02T15:04:05.000000Z"))
	}
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	writeDAVCapabilities(w.Header())
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
		return strings.TrimSuffix(base, "/") + "/" + escapePathSegments(rel)
	}
	return strings.TrimSuffix(u.Path, "/") + "/" + strings.TrimPrefix(escapePathSegments(rel), "/")
}

// escapePathSegments percent-escapes each path segment (URL-decoding-safe) so
// non-ASCII (e.g. Chinese) file/dir names and spaces survive in href URLs that
// the svn client round-trips through ra_serf.
func escapePathSegments(p string) string {
	segs := strings.Split(p, "/")
	for i, s := range segs {
		if s != "" {
			segs[i] = url.PathEscape(s)
		}
	}
	return strings.Join(segs, "/")
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
func (s *Server) propNode(b *strings.Builder, repo *fs.Repo, base, name string, rev uint64, path string, nr *fs.NodeRev, props []Name, propnames, publicHref bool) error {
	if nr == nil {
		rn, err := repo.Resolve(rev, path)
		if err != nil {
			if errors.Is(err, fs.ErrPathNotFound) {
				// Resource does not exist at this rev: return the sentinel so the
				// caller emits a plain HTTP 404. A multistatus-404 propstat here
				// makes the client treat it as "versioned but missing vcc" and
				// fail svn import / subdir ops with E175003.
				return fs.ErrPathNotFound
			}
			return err
		}
		nr = rn
	}
	if propnames {
		// svn proplist: emit property names only (empty elements), excluding
		// the internal svn:entry so the client shows user props (svn:mime-type).
		b.WriteString("  <D:response>\n")
		b.WriteString("    <D:href>" + base + privateHref(rev, path) + "</D:href>\n")
		b.WriteString("    <D:propstat>\n      <D:prop>\n")
		b.WriteString("      <D:resourcetype/>\n")
		b.WriteString("      <D:getetag/>\n")
		b.WriteString("      <D:version-name/>\n")
		b.WriteString("      <D:creator-displayname/>\n")
		keys := make([]string, 0, len(nr.Props))
		for k := range nr.Props {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if (strings.HasPrefix(k, "svn:") && k != "svn:date" && k != "svn:author") || !strings.HasPrefix(k, "svn:") {
				b.WriteString("      " + davPropNameTag(k) + "\n")
			}
		}
		b.WriteString("      </D:prop>\n      <D:status>HTTP/1.1 200 OK</D:status>\n    </D:propstat>\n")
		b.WriteString("  </D:response>\n")
		return nil
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
	b.WriteString("      <D:checked-in><D:href>" + base + "!svn/bln/" + strconv.FormatUint(rev, 10) + "/" + escapePath(strings.TrimPrefix(path, "/")) + "</D:href></D:checked-in>\n")
	b.WriteString("      <S:baseline-relative-path>" + escapePath(strings.TrimPrefix(path, "/")) + "</S:baseline-relative-path>\n")
	// Last-changed metadata comes from the revision properties of the node-rev,
	// not from node properties (svn:author is a revprop). Without this the
	// author is blank and svn ls -v shows Jan 01 1970.
	var lastAuthor, lastDate string
	if rp, err := repo.GetRevProps(nr.Node.Rev); err == nil && rp != nil {
		lastAuthor = rp.Props["svn:author"]
		lastDate = rp.Props["svn:date"]
	}
	b.WriteString("      <D:creator-displayname>" + xmlEscape(lastAuthor) + "</D:creator-displayname>\n")
	if lastDate != "" {
		b.WriteString("      <D:creationdate>" + xmlEscape(lastDate) + "</D:creationdate>\n")
	}
	// svn:entry is a DAV-internal property; emitting it in a propfind makes
	// `svn proplist` list it as a user property. update-report already delivers
	// committed-rev/date/author via S:set-prop, so it is not needed here.
	// writeSvnEntryProps(b, nr, rev, path)
	keys := make([]string, 0, len(nr.Props))
	for k := range nr.Props {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if k == "svn:date" || k == "svn:author" {
			continue
		}
		b.WriteString("      " + davPropElement(k, nr.Props[k]) + "\n")
	}
	if nr.Kind == fs.KindFile {
		// Children listed by propChildren carry only Node/Kind/Props; fetch the
		// full node-rev (cached) so the size comes from TotalSize/TextSize
		// instead of materializing a possibly huge text just for PROPFIND.
		fnr := nr
		if real, err := repo.GetNodeRev(nr.Node); err == nil {
			fnr = real
		}
		if size, serr := repo.TextSizeOf(fnr); serr == nil {
			b.WriteString("      <D:getcontentlength>" + strconv.FormatInt(size, 10) + "</D:getcontentlength>\n")
		}
		mime := propVal(nr.Props, "svn:mime-type")
		if mime == "" {
			mime = "text/plain"
		}
		b.WriteString("      <D:getcontenttype>" + mime + "</D:getcontenttype>\n")
		// svn:size is delivered via update-report (S:size), not propfind:
		// exposing it here makes svn proplist list it as a user property.
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
func (s *Server) propChildren(b *strings.Builder, repo *fs.Repo, base, name string, rev uint64, path string, props []Name, depth string, budget *int, user string) error {
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
		// Guard depth=infinity recursion: a huge tree would blow up memory and
		// CPU (recursively listing every node). Refuse past a hard node budget.
		*budget++
		if *budget > maxPropfindNodes {
			return errors.New("PROPFIND depth=infinity exceeds node budget")
		}
		// Paths may carry a trailing slash (client requests /d/); join without
		// introducing "//" so the emitted hrefs stay canonical for the client's
		// svn_fspath__skip_ancestor assertion.
		cpath := strings.TrimRight(path, "/") + "/" + c.Name
		// Never disclose inaccessible subtrees in a directory listing.
		if az := s.cfg.authzFor(name); az != nil && !az.Authorize(user, name, cpath, false) {
			continue
		}
		// ListDir already returned each child's NodeRev fields; pass the
		// pre-parsed rev so propNode does not re-Resolve (which would expand the
		// whole parent dir per child => O(n^2) on a big directory).
		cnr := &fs.NodeRev{Node: c.ID, Kind: c.Kind, Props: c.Props}
		if err := s.propNode(b, repo, base, name, rev, cpath, cnr, props, false, true); err != nil {
			return err
		}
		if depth == "infinity" {
			cnr, err := repo.Resolve(rev, cpath)
			if err == nil && cnr.Kind == fs.KindDir {
				if err := s.propChildren(b, repo, base, name, rev, cpath, props, depth, budget, user); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// --- propfind request parsing ---

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
	b.WriteString("      <svn:entry rev=\"" + strconv.FormatUint(rev, 10) + "\" kind=\"" + kind + "\">\n")
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
	if repo, ok := s.repo(w, res); ok {
		s.loadTxnChanges(repo, dres.uuid)
		if res.typ == resTxn {
			s.loadTxnChanges(repo, res.uuid)
		}
	} else {
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
		// openRepoOnly (not s.repo): we already hold s.mu here, and a shared
		// repo cache would re-lock s.mu (non-reentrant) and deadlock.
		repo, rok := s.openRepoOnly(w, res.repo)
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
	actID := strings.TrimPrefix(tx.id, "txn-")
	s.mu.Unlock()
	if repoHandle, rok := s.repo(w, res); rok {
		s.persistChange(repoHandle, actID, dres.path)
		if r.Method == "MOVE" && res.typ == resTxn && res.uuid == dres.uuid {
			s.persistChange(repoHandle, actID, res.path)
		}
	}
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
