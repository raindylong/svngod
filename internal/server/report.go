package server

import (
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"svngod/internal/fs"
	"svngod/internal/svndiff"
)

// handleReport dispatches REPORT requests (sent to /!svn/me). The body's root
// element selects update-report / log-report / get-locations, etc.
func (s *Server) handleReport(w http.ResponseWriter, r *http.Request, res resource) {
	// svn v1 clients issue REPORT (update-report / log-report / get-locations)
	// against the vcc or baseline URL, while v2 uses /!svn/me. The report type
	// is determined by the request body, so we accept REPORT on any resource
	// that resolves to a repository.
	repo, ok := s.repo(w, res)
	if !ok {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var root xmlEl
	if err := xml.Unmarshal(body, &root); err != nil {
		log.Printf("REPORT bad XML body=%q err=%v", string(body), err)
		http.Error(w, "bad REPORT XML: "+err.Error(), http.StatusBadRequest)
		return
	}
	base := s.absURL(r, res.repo, "/")
	switch root.XMLName.Local {
	case "update-report":
		s.reportUpdate(w, repo, base, res.repo, body)
	case "log-report":
		s.reportLog(w, repo, base, body)
	case "list-report":
		s.reportList(w, repo, base, res.repo, body)
	case "file-revs-report", "get-file-revs-report", "get-file-revs":
		s.reportFileRevs(w, repo, base, res.path, body)
	case "get-locations", "get-locations-segments":
		s.reportGetLocations(w, repo, base, res.path, body)
	default:
		http.Error(w, "unsupported REPORT: "+root.XMLName.Local, http.StatusBadRequest)
	}
}

// xmlEl is a generic XML node used to inspect REPORT request bodies.
type xmlEl struct {
	XMLName  xml.Name
	Attrs    []xml.Attr `xml:",any,attr"`
	Content  string     `xml:",chardata"`
	Children []xmlEl    `xml:",any"`
}

func (e xmlEl) attr(name string) string {
	for _, a := range e.Attrs {
		if a.Name.Local == name {
			return a.Value
		}
	}
	return ""
}

func (e xmlEl) child(local string) (xmlEl, bool) {
	for _, c := range e.Children {
		if c.XMLName.Local == local {
			return c, true
		}
	}
	return xmlEl{}, false
}

// --- update-report (svn checkout / update) ---

func (s *Server) reportUpdate(w http.ResponseWriter, repo *fs.Repo, base, name string, body []byte) {
	var root xmlEl
	if err := xml.Unmarshal(body, &root); err != nil {
		http.Error(w, "bad update-report", http.StatusBadRequest)
		return
	}
	// target revision (default HEAD)
	head, err := repo.Head()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rev := head
	if tr, ok := root.child("target-revision"); ok {
		if n, err := strconv.ParseUint(strings.TrimSpace(tr.Content), 10, 64); err == nil {
			rev = n
		}
	}
	if rev > head {
		http.Error(w, "target revision does not exist", http.StatusBadRequest)
		return
	}
	// Client's WC baseline revision comes from <S:entry rev="N"> (svn diff), or
	// <S:revision>; used so the txdelta is sent as a delta from the baseline
	// revision's text rather than an empty base.
	var baseRev uint64
	// start-empty="true" on <S:entry> means the client's WC has no such node
	// (fresh checkout / sparse). In that case the server must send add-file /
	// add-directory, never open-file, even when the baseline has the node.
	startEmpty := false
	if e, ok := root.child("entry"); ok {
		if v := e.attr("start-empty"); v == "true" || v == "yes" {
			startEmpty = true
		}
		if v := e.attr("rev"); v != "" {
			if n, err := strconv.ParseUint(v, 10, 64); err == nil {
				baseRev = n
			}
		}
	}
	if baseRev == 0 {
		if br, ok := root.child("revision"); ok {
			v := br.attr("rev")
			if v == "" {
				v = strings.TrimSpace(br.Content)
			}
			if n, err := strconv.ParseUint(v, 10, 64); err == nil {
				baseRev = n
			}
		}
	}
	if baseRev > head {
		baseRev = 0
	}
	// src path (the checkout root inside the repo)
	rootPath := "/"
	if sp, ok := root.child("src-path"); ok {
		if p, ok := extractReposPath(sp.Content, base); ok {
			rootPath = p
		}
	}

	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
	b.WriteString(`<S:update-report xmlns:S="svn:" xmlns:D="` + NS_DAV + `" xmlns:V="http://subversion.tigris.org/xmlns/dav/" inline-props="true">` + "\n")
	b.WriteString("  <S:target-revision rev=\"" + strconv.FormatUint(rev, 10) + "\"/>\n")
	rp, _ := repo.GetRevProps(rev)
	uuid, _ := repo.UUID()
	// Pre-index the baseline revision's subtree once, so updateWalkDir can
	// resolve each file's baseline text by map lookup instead of a per-file
	// Resolve(baseRev, path) that re-walks the tree for every node.
	baseIdx := map[string]fs.NodeID{}
	if baseRev > 0 {
		if err := indexTree(repo, baseRev, rootPath, baseIdx); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if err := s.updateWalkDir(&b, repo, base, rev, baseRev, rootPath, "open-directory", true, rp, uuid, baseIdx, startEmpty); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	b.WriteString("</S:update-report>\n")
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, b.String())
}

// indexTree walks rev's subtree rooted at path and records cpath -> NodeID for
// every descendant, enabling O(1) baseline lookups during update-report.
func indexTree(repo *fs.Repo, rev uint64, path string, out map[string]fs.NodeID) error {
	children, err := repo.ListDir(rev, path)
	if err != nil {
		return err
	}
	for _, c := range children {
		cpath := strings.TrimSuffix(path, "/") + "/" + c.Name
		out[cpath] = c.ID
		if c.Kind == fs.KindDir {
			if err := indexTree(repo, rev, cpath, out); err != nil {
				return err
			}
		}
	}
	return nil
}

// writeEntryProps emits svn:entry properties for a node. svn's update editor
// derives the working-copy changed_rev/date/author/uuid from these; without
// svn:entry:committed-rev the client aborts with an invalid-revision assert.
func writeEntryProps(b *strings.Builder, rp *fs.RevProps, uuid string, rev uint64) {
	if rp != nil {
		if date := rp.Props["svn:date"]; date != "" {
			b.WriteString(`<S:set-prop name="svn:entry:committed-date">` + xmlEscape(date) + `</S:set-prop>` + "\n")
		}
		if author := rp.Props["svn:author"]; author != "" {
			b.WriteString(`<S:set-prop name="svn:entry:last-author">` + xmlEscape(author) + `</S:set-prop>` + "\n")
		}
	}
	b.WriteString(`<S:set-prop name="svn:entry:committed-rev">` + strconv.FormatUint(rev, 10) + `</S:set-prop>` + "\n")
	if uuid != "" {
		b.WriteString(`<S:set-prop name="svn:entry:uuid">` + xmlEscape(uuid) + `</S:set-prop>` + "\n")
	}
}

// updateWalkDir emits a directory node and its children (add-directory /
// add-file with fulltext txdelta), walking the whole subtree at rev. baseRev
// is the client's WC baseline revision (from <S:revision>); when > 0 the file
// txdelta is sent as a delta from the baseline revision's text (svn diff).
func (s *Server) updateWalkDir(b *strings.Builder, repo *fs.Repo, base string, rev uint64, baseRev uint64, path, openEl string, wrap bool, rp *fs.RevProps, uuid string, baseIdx map[string]fs.NodeID, startEmpty bool) error {
	// Element rev (open-directory / open-file) mirrors the client's WC baseline
	// revision; on a fresh checkout (baseRev==0) we emit HEAD. The checked-in
	// href still points at the target revision.
	outRev := rev
	if baseRev > 0 {
		outRev = baseRev
	}
	if wrap {
		b.WriteString("  <S:" + openEl + " rev=\"" + strconv.FormatUint(outRev, 10) + "\">\n")
	}
	writeEntryProps(b, rp, uuid, rev)
	children, err := repo.ListDir(rev, path)
	if err != nil {
		return err
	}
	// stable ordering
	sort.Slice(children, func(i, j int) bool { return children[i].Name < children[j].Name })
	for _, c := range children {
		cpath := strings.TrimSuffix(path, "/") + "/" + c.Name
		if c.Kind == fs.KindDir {
			if _, inBase := baseIdx[cpath]; inBase && !startEmpty {
				// existing dir in client's WC -> open-directory (update semantics)
				ver := base + "!svn/ver/" + strconv.FormatUint(rev, 10) + "/" + strings.TrimPrefix(cpath, "/")
				b.WriteString("    <S:open-directory name=\"" + xmlEscape(c.Name) + "\" rev=\"" + strconv.FormatUint(outRev, 10) + "\">\n")
				b.WriteString("      <D:checked-in><D:href>" + xmlEscape(ver) + "</D:href></D:checked-in>\n")
				writeEntryProps(b, rp, uuid, rev)
				writeNodeProps(b, c.Props)
				if err := s.updateWalkDir(b, repo, base, rev, baseRev, cpath, "open-directory", false, rp, uuid, baseIdx, startEmpty); err != nil {
					return err
				}
				b.WriteString("    </S:open-directory>\n")
			} else {
				bc := base + "!svn/bc/" + strconv.FormatUint(rev, 10) + "/" + strings.TrimPrefix(cpath, "/")
				b.WriteString("    <S:add-directory name=\"" + xmlEscape(c.Name) + "\" bc-url=\"" + xmlEscape(bc) + "\">\n")
				b.WriteString("      <D:checked-in><D:href>" + xmlEscape(bc) + "</D:href></D:checked-in>\n")
				writeEntryProps(b, rp, uuid, rev)
				writeNodeProps(b, c.Props)
				if err := s.updateWalkDir(b, repo, base, rev, baseRev, cpath, "add-directory", false, rp, uuid, baseIdx, startEmpty); err != nil {
					return err
				}
				b.WriteString("    </S:add-directory>\n")
			}
		} else {
			_, inBase := baseIdx[cpath]
			inBase = inBase && !startEmpty
			// use the child's immutable node id from ListDir directly, avoiding a
			// per-file Resolve (which re-walks the root->path chain and re-reads
			// every block on the way).
			nr, err := repo.GetNodeRev(c.ID)
			if err != nil {
				return err
			}
			text, err := repo.GetText(nr.Node)
			if err != nil {
				return err
			}
			writeNodeProps(b, nr.Props)
			var baseText []byte
			if baseRev > 0 && baseRev != rev {
				if bid, ok := baseIdx[cpath]; ok {
					if bt, gerr := repo.GetText(bid); gerr == nil {
						baseText = bt
					}
				}
			}
			delta, err := svndiff.Encode(baseText, text)
			if err != nil {
				return err
			}
			sum := md5.Sum(text)
			md5hex := hex.EncodeToString(sum[:])
			if inBase {
				// existing file in client's WC -> open-file (update semantics).
				// add-file would make the client's add_file() editor skip the
				// already-versioned file as an obstructing working copy.
				ver := base + "!svn/ver/" + strconv.FormatUint(rev, 10) + "/" + strings.TrimPrefix(cpath, "/")
				ss := sha1.Sum(text)
				// base-checksum validates the client's WC baseline: for an
				// incremental update it is the baseline text's MD5; when
				// baseRev==rev (no-op) the WC baseline equals the target text.
				bs := md5.Sum(text)
				if len(baseText) > 0 {
					bs = md5.Sum(baseText)
				}
				b.WriteString("    <S:open-file name=\"" + xmlEscape(c.Name) + "\" rev=\"" + strconv.FormatUint(outRev, 10) + "\">\n")
				b.WriteString("      <D:checked-in><D:href>" + xmlEscape(ver) + "</D:href></D:checked-in>\n")
				writeEntryProps(b, rp, uuid, rev)
				// fetch-file: client GETs the full text at the checked-in href
				// (like mod_dav_svn). base-checksum validates the WC baseline.
				b.WriteString("      <S:fetch-file base-checksum=\"" + hex.EncodeToString(bs[:]) + "\" sha1-checksum=\"" + hex.EncodeToString(ss[:]) + "\"/>\n")
				b.WriteString("      <S:prop><V:md5-checksum>" + md5hex + "</V:md5-checksum></S:prop>\n")
				b.WriteString("    </S:open-file>\n")
			} else {
				bc := base + "!svn/bc/" + strconv.FormatUint(rev, 10) + "/" + strings.TrimPrefix(cpath, "/")
				b.WriteString("    <S:add-file name=\"" + xmlEscape(c.Name) + "\" sha1-checksum=\"" + sha1Hex(text) + "\">\n")
				b.WriteString("      <D:checked-in><D:href>" + xmlEscape(bc) + "</D:href></D:checked-in>\n")
				writeEntryProps(b, rp, uuid, rev)
				b.WriteString("      <S:txdelta>" + base64.StdEncoding.EncodeToString(delta) + "</S:txdelta>\n")
				b.WriteString("      <S:prop><V:md5-checksum>" + md5hex + "</V:md5-checksum></S:prop>\n")
				b.WriteString("    </S:add-file>\n")
			}
		}
	}
	if wrap {
		b.WriteString("  </S:" + openEl + ">\n")
	}
	return nil
}

// sha1Hex returns the lowercase hex SHA-1 of data.
func sha1Hex(b []byte) string {
	s := sha1.Sum(b)
	return hex.EncodeToString(s[:])
}

// writeNodeProps emits <S:set-prop> for each node property (svn namespace and
// custom props). svn:date is a revision property and is skipped here.
func writeNodeProps(b *strings.Builder, props map[string]string) {
	if len(props) == 0 {
		return
	}
	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if k == "svn:date" {
			continue
		}
		b.WriteString("      <S:set-prop name=\"" + xmlEscape(k) + "\">" + xmlEscape(props[k]) + "</S:set-prop>\n")
	}
}

// extractReposPath converts a src-path URL back to a repos-relative svn path.
func extractReposPath(url, base string) (string, bool) {
	// base is the absolute repo-root URL, e.g. "http://host/svn/demo/".
	// src-path is usually a repo-relative path "/svn/demo/s10" (svn CLI),
	// sometimes an absolute URL. Extract the repo-internal path.
	// 1) repo path prefix from base (path component after scheme://host)
	if i := strings.Index(base, "://"); i >= 0 {
		rest := base[i+3:]
		if j := strings.Index(rest, "/"); j >= 0 {
			basePath := rest[j:] // e.g. "/svn/demo/"
			if strings.HasPrefix(url, basePath) {
				p := strings.TrimPrefix(url[len(basePath):], "/")
				if p == "" {
					return "/", true
				}
				return "/" + p, true
			}
		}
	}
	// 2) absolute URL containing the whole base
	idx := strings.Index(url, base)
	if idx < 0 {
		return "/", false
	}
	p := strings.TrimPrefix(url[idx+len(base):], "/")
	if p == "" {
		return "/", true
	}
	return "/" + p, true
}

// --- log-report (svn log) ---

func (s *Server) reportLog(w http.ResponseWriter, repo *fs.Repo, base string, body []byte) {
	var root xmlEl
	if err := xml.Unmarshal(body, &root); err != nil {
		http.Error(w, "bad log-report", http.StatusBadRequest)
		return
	}
	head, err := repo.Head()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// start/end revisions (descending from start to end)
	start, end := head, uint64(1)
	if v, ok := root.child("start-revision"); ok {
		if n, err := strconv.ParseUint(strings.TrimSpace(v.Content), 10, 64); err == nil && n <= head {
			start = n
		}
	}
	if v, ok := root.child("end-revision"); ok {
		if n, err := strconv.ParseUint(strings.TrimSpace(v.Content), 10, 64); err == nil && n >= 1 {
			end = n
		}
	}
	limit := uint64(0)
	if v, ok := root.child("limit"); ok {
		if n, err := strconv.ParseUint(strings.TrimSpace(v.Content), 10, 64); err == nil {
			limit = n
		}
	}
	_, discoverChanged := root.child("discover-changed-paths")
	// requested revprops: which svn:* props the client wants
	requested := map[string]bool{}
	anyRevprop := false
	for _, c := range root.Children {
		if c.XMLName.Local == "revprop" {
			anyRevprop = true
			// The client names the requested revision property in the element
			// content (e.g. <S:revprop>svn:author</S:revprop>); tolerate an
			// attribute form as well.
			n := c.attr("name")
			if n == "" {
				n = strings.TrimSpace(c.Content)
			}
			if n != "" {
				requested[n] = true
			}
		}
	}
	// If the client asked for no revprops explicitly, skip them.
	noRevProps := false
	if _, ok := root.child("no-revprops"); ok {
		noRevProps = true
	}

	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
	b.WriteString(`<S:log-report xmlns:S="svn:" xmlns:D="` + NS_DAV + `">` + "\n")
	count := uint64(0)
	for rv := start; rv >= end; rv-- {
		if limit > 0 && count >= limit {
			break
		}
		count++
		rp, err := repo.GetRevProps(rv)
		if err != nil {
			continue
		}
		b.WriteString("  <S:log-item>\n")
		if discoverChanged {
			s.logChangedPaths(&b, repo, rv)
		}
		b.WriteString("    <D:version-name>" + strconv.FormatUint(rv, 10) + "</D:version-name>\n")
		if !noRevProps && len(rp.Props) > 0 {
			keys := make([]string, 0, len(rp.Props))
			for k := range rp.Props {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				v := rp.Props[k]
				// if the client named specific revprops, honour that set
				if anyRevprop && !requested[k] {
					continue
				}
				switch k {
				case "svn:author":
					b.WriteString("    <D:creator-displayname>" + xmlEscape(v) + "</D:creator-displayname>\n")
				case "svn:date":
					b.WriteString("    <S:date>" + xmlEscape(v) + "</S:date>\n")
				case "svn:log":
					b.WriteString("    <D:comment>" + xmlEscape(v) + "</D:comment>\n")
				default:
					b.WriteString("    <S:revprop name=\"" + xmlEscape(k) + "\">" + xmlEscape(v) + "</S:revprop>\n")
				}
			}
		}
		b.WriteString("  </S:log-item>\n")
		if rv == 0 || rv == 1 {
			break
		}
	}
	b.WriteString("</S:log-report>\n")
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, b.String())
}

// logChangedPaths diffs rev vs rev-1 and emits added/modified/deleted paths.
func (s *Server) logChangedPaths(b *strings.Builder, repo *fs.Repo, rev uint64) {
	changes, err := treeDiff(repo, rev)
	if err != nil {
		return
	}
	paths := make([]string, 0, len(changes))
	for p := range changes {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		ch := changes[p]
		switch ch.action {
		case chAdded:
			if nr, err := repo.Resolve(rev, p); err == nil && nr.CopyFrom != nil {
				b.WriteString("    <S:added-path copyfrom-path=\"" + xmlEscape(nr.CopyFromPath) + "\" copyfrom-rev=\"" +
					strconv.FormatUint(nr.CopyFrom.Rev, 10) + "\">" + xmlEscape(p) + "</S:added-path>\n")
				continue
			}
			b.WriteString("    <S:added-path>" + xmlEscape(p) + "</S:added-path>\n")
		case chModified:
			b.WriteString("    <S:modified-path>" + xmlEscape(p) + "</S:modified-path>\n")
		case chDeleted:
			b.WriteString("    <S:deleted-path>" + xmlEscape(p) + "</S:deleted-path>\n")
		case chReplaced:
			b.WriteString("    <S:replaced-path>" + xmlEscape(p) + "</S:replaced-path>\n")
		}
	}
}

type changeKind int

const (
	chAdded changeKind = iota
	chModified
	chDeleted
	chReplaced
)

type change struct{ action changeKind }

// treeDiff returns the set of paths changed between rev-1 and rev.
func treeDiff(repo *fs.Repo, rev uint64) (map[string]change, error) {
	if rev == 0 {
		return map[string]change{}, nil
	}
	out := map[string]change{}
	var walk func(prevRev, rev uint64, path string) error
	walk = func(prevRev, rev uint64, path string) error {
		cur, err := repo.Resolve(rev, path)
		prevErr := err
		prev, prevErr2 := repo.Resolve(prevRev, path)
		switch {
		case err == nil && prevErr2 != nil:
			out[path] = change{chAdded}
			if cur.Kind == fs.KindDir {
				children, _ := repo.ListDir(rev, path)
				for _, c := range children {
					if err := walk(prevRev, rev, path+"/"+c.Name); err != nil {
						return err
					}
				}
			}
			return nil
		case err != nil && prevErr2 == nil:
			out[path] = change{chDeleted}
			return nil
		case err != nil && prevErr2 != nil:
			return nil
		}
		// both exist: compare kind + (for files) text + props
		if cur.Kind != prev.Kind {
			out[path] = change{chReplaced}
		} else if cur.Kind == fs.KindDir {
			curCh, _ := repo.ListDir(rev, path)
			prevCh, _ := repo.ListDir(prevRev, path)
			names := map[string]bool{}
			for _, c := range curCh {
				names[c.Name] = true
			}
			for _, c := range prevCh {
				if !names[c.Name] {
					if err := walk(prevRev, rev, path+"/"+c.Name); err != nil {
						return err
					}
				}
			}
			// record directory modification if its own props changed
			if !sameProps(cur.Props, prev.Props) {
				out[path] = change{chModified}
			}
			for _, c := range curCh {
				if err := walk(prevRev, rev, path+"/"+c.Name); err != nil {
					return err
				}
			}
		} else {
			ct, err1 := repo.GetText(cur.Node)
			pt, err2 := repo.GetText(prev.Node)
			if err1 != nil || err2 != nil {
				return fmt.Errorf("treeDiff text: %v %v", err1, err2)
			}
			if !bytesEqual(ct, pt) || !sameProps(cur.Props, prev.Props) {
				out[path] = change{chModified}
			}
		}
		_ = prevErr
		return nil
	}
	if err := walk(rev-1, rev, "/"); err != nil {
		return nil, err
	}
	delete(out, "/")
	return out, nil
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sameProps(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

var _ = errors.New


// reportList writes a <S:list-report> response (svn ls / svn list).
// Request body carries <S:path> and optional <S:prop> entries; response is
// <S:list-report> with a <S:entry kind=... name=...>per child whose text is
// the relative path.
func (s *Server) reportList(w http.ResponseWriter, repo *fs.Repo, base, name string, body []byte) {
	var req struct {
		XMLName xml.Name
		Path    string `xml:"path"`
	}
	xml.Unmarshal(body, &req)
	path := req.Path
	if path == "" {
		path = "/"
	}
	rev, _ := repo.Head()
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
	b.WriteString(`<S:list-report xmlns:S="` + NS_SVN + `" xmlns:D="` + NS_DAV + `">` + "\n")
	b.WriteString(`  <S:path>` + xmlEscape(path) + `</S:path>` + "\n")
	children, err := repo.ListDir(rev, path)
	if err == nil {
		for _, c := range children {
			kind := "file"
			if c.Kind == fs.KindDir {
				kind = "dir"
			}
			b.WriteString(`  <S:entry kind="` + kind + `" name="` + xmlEscape(c.Name) + `">` + xmlEscape(c.Name) + `</S:entry>` + "\n")
		}
	}
	b.WriteString(`</S:list-report>` + "\n")
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, b.String())
}

// --- get-file-revs (svn blame) ---

func (s *Server) reportFileRevs(w http.ResponseWriter, repo *fs.Repo, base, urlPath string, body []byte) {
	var root xmlEl
	if err := xml.Unmarshal(body, &root); err != nil {
		http.Error(w, "bad get-file-revs", http.StatusBadRequest)
		return
	}
	// The client leaves <S:path> empty and addresses the file through the
	// REPORT URL (e.g. !svn/bln/2/hello.txt); use that path here.
	path := urlPath
	if c, ok := root.child("path"); ok && strings.TrimSpace(c.Content) != "" {
		path = "/" + strings.Trim(strings.TrimSpace(c.Content), "/")
	}
	head, _ := repo.Head()
	start, end := head, uint64(1)
	if v, ok := root.child("start-revision"); ok {
		if n, err := strconv.ParseUint(strings.TrimSpace(v.Content), 10, 64); err == nil && n <= head {
			start = n
		}
	}
	if v, ok := root.child("end-revision"); ok {
		if n, err := strconv.ParseUint(strings.TrimSpace(v.Content), 10, 64); err == nil && n >= 1 {
			end = n
		}
	}
	lo, hi := start, end
	if lo > hi {
		lo, hi = hi, lo
	}
	// Collect ascending (oldest first) revisions where the file exists.
	type fr struct {
		rev  uint64
		text []byte
	}
	var revs []fr
	for r := lo; r <= hi; r++ {
		nr, err := repo.Resolve(r, path)
		if err != nil || nr.Kind != fs.KindFile {
			continue
		}
		text, _ := repo.GetText(nr.Node)
		revs = append(revs, fr{rev: r, text: text})
	}
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
	b.WriteString(`<S:file-revs-report xmlns:S="svn:" xmlns:D="` + NS_DAV + `">` + "\n")
	// Emit oldest-first (forwards blame); each delta rebuilds this revision
	// from the previous (older) one: base=older text -> this text.
	for i := 0; i < len(revs); i++ {
		cur := revs[i]
		var delta []byte
		if i > 0 {
			delta, _ = svndiff.Encode(revs[i-1].text, cur.text)
		} else {
			delta, _ = svndiff.Encode(nil, cur.text)
		}
		rp, _ := repo.GetRevProps(cur.rev)
		auth := ""
		if rp != nil {
			auth = rp.Props["svn:author"]
		}
		b.WriteString("  <S:file-rev path=\"" + xmlEscape(path) + "\" rev=\"" + strconv.FormatUint(cur.rev, 10) + "\">\n")
		if auth != "" {
			b.WriteString("    <S:rev-prop name=\"svn:author\">" + xmlEscape(auth) + "</S:rev-prop>\n")
		}
		if len(delta) > 0 {
			b.WriteString("    <S:txdelta>" + base64.StdEncoding.EncodeToString(delta) + "</S:txdelta>\n")
		}
		b.WriteString("  </S:file-rev>\n")
	}
	b.WriteString("</S:file-revs-report>\n")
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	fmt.Fprint(w, b.String())
}

// reportGetLocations answers a get-locations REPORT: the repos path lineage of
// resPath between location-revision and peg-revision. svn diff uses it to
// locate a path at an older revision (rename tracking). Without copies the
// path is constant; a copy point switches the lineage to its copy source.
func (s *Server) reportGetLocations(w http.ResponseWriter, repo *fs.Repo, base string, resPath string, body []byte) {
	var root xmlEl
	if err := xml.Unmarshal(body, &root); err != nil {
		http.Error(w, "bad get-locations", http.StatusBadRequest)
		return
	}
	path := resPath
	if sp, ok := root.child("path"); ok && strings.TrimSpace(sp.Content) != "" {
		path = strings.TrimSpace(sp.Content)
	}
	head, err := repo.Head()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	peg := head
	if pr, ok := root.child("peg-revision"); ok {
		if n, err := strconv.ParseUint(strings.TrimSpace(pr.Content), 10, 64); err == nil {
			peg = n
		}
	}
	loc := peg
	if lr, ok := root.child("location-revision"); ok {
		if n, err := strconv.ParseUint(strings.TrimSpace(lr.Content), 10, 64); err == nil {
			loc = n
		}
	}
	if loc > peg {
		loc, peg = peg, loc
	}
	// Build the lineage: the path as it existed at each revision from peg down
	// to loc, following copyFrom sources at copy points.
	lineage := map[uint64]string{}
	cur := path
	for rev := peg; rev >= loc && rev >= 1; rev-- {
		lineage[rev] = cur
		if nr, err := repo.Resolve(rev, cur); err == nil && nr.CopyFrom != nil {
			if nr.CopyFrom.Rev < rev && nr.CopyFromPath != "" {
				cur = nr.CopyFromPath
			}
		} else if err != nil {
			break
		}
	}
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
	b.WriteString(`<S:get-locations-report xmlns:S="svn:" xmlns:D="` + NS_DAV + `">` + "\n")
	for rev := loc; rev <= peg; rev++ {
		p, ok := lineage[rev]
		if !ok {
			p = path
		}
		b.WriteString(`  <S:location path="` + xmlEscape(p) + `" rev="` + strconv.FormatUint(rev, 10) + `"/>` + "\n")
	}
	b.WriteString("</S:get-locations-report>\n")
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	fmt.Fprint(w, b.String())
}
