package server

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
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
	case "get-locations", "get-locations-segments":
		s.notImpl(w, "REPORT "+root.XMLName.Local)
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
	// src path (the checkout root inside the repo)
	rootPath := "/"
	if sp, ok := root.child("src-path"); ok {
		if p, ok := extractReposPath(sp.Content, base); ok {
			rootPath = p
		}
	}

	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
	b.WriteString(`<S:update-report xmlns:S="svn:" xmlns:D="` + NS_DAV + `">` + "\n")
	b.WriteString("  <S:target-revision rev=\"" + strconv.FormatUint(rev, 10) + "\"/>\n")
	if err := s.updateWalkDir(&b, repo, base, rev, rootPath, "open-directory", true); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	b.WriteString("</S:update-report>\n")
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, b.String())
}

// writeEntryProps emits svn:entry properties for a node. svn's update editor
// derives the working-copy changed_rev/date/author/uuid from these; without
// svn:entry:committed-rev the client aborts with an invalid-revision assert.
func writeEntryProps(b *strings.Builder, repo *fs.Repo, rev uint64) {
	rp, err := repo.GetRevProps(rev)
	uuid, _ := repo.UUID()
	if err == nil {
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
// add-file with fulltext txdelta), walking the whole subtree at rev.
func (s *Server) updateWalkDir(b *strings.Builder, repo *fs.Repo, base string, rev uint64, path, openEl string, wrap bool) error {
	if wrap {
		b.WriteString("  <S:" + openEl + " rev=\"" + strconv.FormatUint(rev, 10) + "\">\n")
	}
	writeEntryProps(b, repo, rev)
	children, err := repo.ListDir(rev, path)
	if err != nil {
		return err
	}
	// stable ordering
	sort.Slice(children, func(i, j int) bool { return children[i].Name < children[j].Name })
	for _, c := range children {
		cpath := path + "/" + c.Name
		if c.Kind == fs.KindDir {
			bc := base + "!svn/bc/" + strconv.FormatUint(rev, 10) + "/" + strings.TrimPrefix(cpath, "/")
			b.WriteString("    <S:add-directory name=\"" + xmlEscape(c.Name) + "\" bc-url=\"" + xmlEscape(bc) + "\">\n")
			b.WriteString("      <D:checked-in><D:href>" + xmlEscape(bc) + "</D:href></D:checked-in>\n")
			writeEntryProps(b, repo, rev)
			writeNodeProps(b, c.Props)
			if err := s.updateWalkDir(b, repo, base, rev, cpath, "add-directory", false); err != nil {
				return err
			}
			b.WriteString("    </S:add-directory>\n")
		} else {
			b.WriteString("    <S:add-file name=\"" + xmlEscape(c.Name) + "\">\n")
			bc := base + "!svn/bc/" + strconv.FormatUint(rev, 10) + "/" + strings.TrimPrefix(cpath, "/")
			b.WriteString("      <D:checked-in><D:href>" + xmlEscape(bc) + "</D:href></D:checked-in>\n")
			writeEntryProps(b, repo, rev)
			nr, err := repo.Resolve(rev, cpath)
			if err != nil {
				return err
			}
			text, err := repo.GetText(nr.Node)
			if err != nil {
				return err
			}
			writeNodeProps(b, nr.Props)
			delta, err := svndiff.Encode(nil, text)
			if err != nil {
				return err
			}
			b.WriteString("      <S:txdelta>" + base64.StdEncoding.EncodeToString(delta) + "</S:txdelta>\n")
			sum := md5.Sum(text)
			b.WriteString("      <S:prop><D:md5-checksum>" + hex.EncodeToString(sum[:]) + "</D:md5-checksum></S:prop>\n")
			b.WriteString("    </S:add-file>\n")
		}
	}
	if wrap {
		b.WriteString("  </S:" + openEl + ">\n")
	}
	return nil
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
	// url like "http://host/svn/demo/trunk" ; base like "http://host/svn/demo/"
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
