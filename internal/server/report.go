package server

import (
	"bytes"
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
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

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
	// Respect the Label request header: a numeric Label is the WC baseline
	// revision the client resolves against during a commit; non-numeric (a
	// named version label) cannot be honored since svngod has no label space.
	if lbl := r.Header.Get("Label"); lbl != "" {
		if _, err := strconv.ParseUint(strings.TrimSpace(lbl), 10, 64); err != nil {
			http.Error(w, "Label (version label) not supported", http.StatusBadRequest)
			return
		}
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
		s.reportLog(w, repo, res.path, body)
	case "list-report":
		s.reportList(w, r, repo, base, res.repo, res.path, body)
	case "file-revs-report", "get-file-revs-report", "get-file-revs":
		s.reportFileRevs(w, repo, base, res.path, body)
	case "get-location-segments":
		s.reportGetLocationSegments(w, repo, res.path, body)
	case "get-locations":
		s.reportGetLocations(w, repo, base, res.path, body)
	case "get-locks-report", "get-locks":
		s.reportGetLocks(w, repo, res.repo, res.path, body)
	case "mergeinfo-report", "get-mergeinfo-report", "get-mergeinfo":
		s.reportGetMergeinfo(w, repo, res.path, body)
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
	head, err := repo.Head()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rev := head
	// <S:text-deltas>yes</S:text-deltas> asks for file texts inline
	// (<S:txdelta>) instead of letting the client fetch them with GET. Clients
	// omit it for checkouts; embedding the text made a checkout REPORT carry
	// the whole tree as base64.
	sendAll := false
	if td, ok := root.child("text-deltas"); ok {
		sendAll = strings.EqualFold(strings.TrimSpace(td.Content), "yes")
	}
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
	// The WC baseline is described by the <S:entry> tree: every entry names a
	// path and the revision that path is at. A single revision for the whole
	// report is wrong for mixed-revision working copies and made svn update
	// fail with checksum mismatches or "obstructing working copy".
	wb := newWCBaseline(repo, rootPath, &root)

	// Requested depth controls how far the report descends. "unknown" means
	// "keep the WC's current sparse shape", so each entry's own depth limits
	// recursion; an explicit --set-depth overrides it.
	reqDepth := ""
	if e, ok := root.child("depth"); ok {
		reqDepth = strings.TrimSpace(e.Content)
	}
	explicitDepth := reqDepth != "" && reqDepth != "unknown"
	limit, filesOnly := -1, false
	if explicitDepth {
		limit, filesOnly = depthLimit(reqDepth)
	}

	// Two-tree form: svn merge / svn diff between different URLs names a
	// dst-path that differs from src-path. The response must diff those two
	// trees, not the target path against the working copy.
	dstPath := ""
	if dp, ok := root.child("dst-path"); ok {
		if p, ok := extractReposPath(strings.TrimSpace(dp.Content), base); ok {
			dstPath = p
		}
	}
	// svn switch reports an anchor plus a target: the WC root is only the
	// anchor and must not carry properties (the client asserts on that). The
	// content comes from dst-path (the new URL), while the baseline is the
	// entry for the old path (rootPath + update-target).
	updateTarget := ""
	if ut, ok := root.child("update-target"); ok {
		updateTarget = strings.Trim(strings.TrimSpace(ut.Content), "/")
	}
	if updateTarget != "" {
		newPath := dstPath
		if newPath == "" {
			newPath = joinWCPath(rootPath, updateTarget)
		}
		oldPath := joinWCPath(rootPath, updateTarget)
		anchorRev := rev
		if r := wb.rootRev(); r > 0 {
			anchorRev = r
		}
		var b strings.Builder
		b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
		b.WriteString(`<S:update-report xmlns:S="svn:" xmlns:D="` + NS_DAV + `" xmlns:V="http://subversion.tigris.org/xmlns/dav/" inline-props="true">` + "\n")
		b.WriteString("  <S:target-revision rev=\"" + strconv.FormatUint(rev, 10) + "\"/>\n")
		rp, _ := repo.GetRevProps(rev)
		uuid, _ := repo.UUID()
		b.WriteString("  <S:open-directory rev=\"" + strconv.FormatUint(anchorRev, 10) + "\">\n")
		if err := s.updateWalkDir(&b, repo, base, rev, newPath, "open-directory", true, rp, uuid, wb, 0, limit, filesOnly, explicitDepth, 0, updateTarget, oldPath, sendAll); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		b.WriteString("  </S:open-directory>\n")
		b.WriteString("</S:update-report>\n")
		w.Header().Set("Content-Type", "text/xml; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, b.String())
		return
	}
	if dstPath != "" && normalizeWCPath(dstPath) != normalizeWCPath(rootPath) {
		s.reportDiff(w, repo, base, rev, rootPath, dstPath, wb.rootRev(), limit, filesOnly)
		return
	}

	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
	b.WriteString(`<S:update-report xmlns:S="svn:" xmlns:D="` + NS_DAV + `" xmlns:V="http://subversion.tigris.org/xmlns/dav/" inline-props="true">` + "\n")
	b.WriteString("  <S:target-revision rev=\"" + strconv.FormatUint(rev, 10) + "\"/>\n")
	rp, _ := repo.GetRevProps(rev)
	uuid, _ := repo.UUID()
	if err := s.updateWalkDir(&b, repo, base, rev, rootPath, "open-directory", true, rp, uuid, wb, 0, limit, filesOnly, explicitDepth, 0, "", rootPath, sendAll); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	b.WriteString("</S:update-report>\n")
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, b.String())
}

// wcEntry is one <S:entry> from an update-report: a path the client has, the
// revision it is at, and how deep it is populated.
type wcEntry struct {
	path  string
	rev   uint64
	depth string
	empty bool // start-empty="true": the client has no such node
}

// wcBaseline answers "what revision is this path at in the client's working
// copy?" by finding the most specific <S:entry> covering the path.
type wcBaseline struct {
	repo    *fs.Repo
	root    string
	entries map[string]wcEntry
}

func normalizeWCPath(p string) string {
	if p == "" {
		return "/"
	}
	return "/" + strings.Trim(p, "/")
}

func joinWCPath(parent, name string) string {
	if parent == "/" || parent == "" {
		return "/" + strings.Trim(name, "/")
	}
	return strings.TrimSuffix(parent, "/") + "/" + strings.Trim(name, "/")
}

func pathUnder(path, prefix string) bool {
	if prefix == "/" || prefix == "" {
		return true
	}
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}

func newWCBaseline(repo *fs.Repo, rootPath string, root *xmlEl) *wcBaseline {
	wb := &wcBaseline{repo: repo, root: normalizeWCPath(rootPath), entries: map[string]wcEntry{}}
	rootPath = wb.root
	var walk func(parent string, e xmlEl)
	walk = func(parent string, e xmlEl) {
		name := strings.TrimSpace(e.Content)
		p := parent
		if name != "" {
			p = joinWCPath(parent, name)
		}
		ent := wcEntry{path: p, depth: e.attr("depth")}
		v := e.attr("start-empty")
		ent.empty = v == "true" || v == "yes"
		if rv := e.attr("rev"); rv != "" {
			if n, err := strconv.ParseUint(strings.TrimSpace(rv), 10, 64); err == nil {
				ent.rev = n
			}
		}
		wb.entries[p] = ent
		for _, c := range e.Children {
			if c.XMLName.Local == "entry" {
				walk(p, c)
			}
		}
	}
	for _, c := range root.Children {
		if c.XMLName.Local == "entry" {
			walk(rootPath, c)
		}
	}
	return wb
}

// revFor returns the revision the path sits at in the WC, and whether the
// client actually has it (depth and start-empty considered).
func (w *wcBaseline) revFor(path string) (uint64, bool) {
	path = normalizeWCPath(path)
	var best wcEntry
	found := false
	for p, e := range w.entries {
		if !pathUnder(path, p) {
			continue
		}
		if !found || len(p) > len(best.path) {
			best, found = e, true
		}
	}
	if !found || best.rev == 0 {
		return 0, false
	}
	// An entry that names this exact path means the client has the node;
	// start-empty only says its children are not populated (that is how a
	// depth-immediates WC reports itself when the client deepens it). For
	// inherited children, start-empty means "not present": that is what a
	// fresh checkout sends for its root entry.
	if path == best.path {
		return best.rev, true
	}
	if best.empty {
		return 0, false
	}
	if best.depth == "empty" {
		return 0, false // depth-empty: only the entry itself is present
	}
	return best.rev, true
}

// rootRev is the revision the report's target path sits at in the WC (0 for a
// fresh checkout). The root entry is keyed by the report's src-path, which is
// not necessarily the repository root (e.g. a WC checked out at /trunk).
func (w *wcBaseline) rootRev() uint64 {
	if e, ok := w.entries[w.root]; ok {
		return e.rev
	}
	return 0
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

// pathCreatedRev is the revision in which a node appeared at its current path:
// the copy root's revision for copied nodes (their node-rev id points at the
// source and would produce an rvr URL that does not contain the new path),
// else the node revision.
func pathCreatedRev(repo *fs.Repo, nr *fs.NodeRev) uint64 {
	if nr == nil {
		return 0
	}
	if cr, _, _, ok := copyOrigin(repo, nr); ok {
		return cr
	}
	return nr.Node.Rev
}

// rvrHref builds a revision-resource URL for a path at its created revision.
func rvrHref(base, path string, rev uint64) string {
	return base + "!svn/rvr/" + strconv.FormatUint(rev, 10) + "/" + strings.TrimPrefix(escapePathSegments(path), "/")
}

// depthLimit maps an svn depth name to a recursion bound: -1 = unlimited,
// 0 = the node itself only, 1 = one level. filesOnly reports that only file
// children may be listed at that level (svn_depth_files).
func depthLimit(d string) (int, bool) {
	switch d {
	case "empty":
		return 0, false
	case "files":
		return 1, true
	case "immediates":
		return 1, false
	}
	return -1, false
}

func minLimit(a, b int) int {
	if a < 0 {
		return b
	}
	if b < 0 {
		return a
	}
	if a < b {
		return a
	}
	return b
}

// entryDepthLevel returns the recursion bound implied by a WC entry's depth.
func (w *wcBaseline) entryDepthLevel(path string) (int, bool) {
	if e, ok := w.entries[normalizeWCPath(path)]; ok {
		if e.depth == "" {
			return -1, true
		}
		l, _ := depthLimit(e.depth)
		return l, true
	}
	return 0, false
}

// updateWalkDir emits one directory node and its children. Every presence /
// baseline decision comes from the client's per-path <S:entry> revisions
// (wcBaseline), so mixed-revision WCs update correctly. Paths present in the
// client's baseline but absent from the target are reported as delete-entry.
func (s *Server) updateWalkDir(b *strings.Builder, repo *fs.Repo, base string, rev uint64, path, openEl string, wrap bool, rp *fs.RevProps, uuid string, wb *wcBaseline, level, limit int, filesOnly, explicitDepth bool, copyFloor uint64, wrapName, wcPath string, sendAll bool) error {
	outRev := rev
	if br, ok := wb.revFor(wcPath); ok {
		outRev = br
	}
	// Nodes inside a copied directory share the source's node-rev ids; their
	// rvr URL must still name the path's location, so the copy revision acts
	// as a floor for created revisions.
	if dnr, derr := repo.Resolve(rev, path); derr == nil {
		if cr, _, _, ok := copyOrigin(repo, dnr); ok && cr > copyFloor {
			copyFloor = cr
		}
	}
	if wrap {
		nameAttr := ""
		if wrapName != "" {
			nameAttr = " name=\"" + xmlEscape(wrapName) + "\""
		}
		b.WriteString("  <S:" + openEl + nameAttr + " rev=\"" + strconv.FormatUint(outRev, 10) + "\">\n")
		nodeRev := rev
		if nr, err := repo.Resolve(rev, path); err == nil {
			nodeRev = pathCreatedRev(repo, nr)
			if nodeRev == 0 {
				nodeRev = rev
			}
		}
		href := rvrHref(base, path, nodeRev)
		b.WriteString("      <D:checked-in><D:href>" + xmlEscape(href) + "</D:href></D:checked-in>\n")
		writeEntryProps(b, rp, uuid, rev)
	}
	children, err := repo.ListDir(rev, path)
	if err != nil {
		return err
	}
	sort.Slice(children, func(i, j int) bool { return children[i].Name < children[j].Name })
	childLevel := level + 1
	for _, c := range children {
		// Depth bound: -1 unlimited, otherwise children exist up to `limit`.
		if limit >= 0 && childLevel > limit {
			continue
		}
		if filesOnly && limit == childLevel && c.Kind == fs.KindDir {
			continue
		}
		cpath := joinWCPath(path, c.Name)
		cwcPath := joinWCPath(wcPath, c.Name)
		baseRev, inBase := wb.revFor(cwcPath)
		if c.Kind == fs.KindDir {
			// A revision inherited from an ancestor entry does not prove the
			// client has this directory: it may have been added after that
			// revision (e.g. a server-side copy). It must exist in the
			// baseline tree as well.
			if inBase {
				if _, err := repo.Resolve(baseRev, cwcPath); err != nil {
					inBase = false
				}
			}
			if inBase {
				// Existing dir in the client's WC -> open-directory.
				dirRev := c.Rev
				if dnr, derr := repo.GetNodeRev(c.ID); derr == nil {
					if cr := pathCreatedRev(repo, dnr); cr > 0 {
						dirRev = cr
					}
				}
				if dirRev < copyFloor {
					dirRev = copyFloor
				}
				ver := rvrHref(base, cpath, dirRev)
				cl := limit
				if !explicitDepth {
					if l, ok := wb.entryDepthLevel(cpath); ok {
						cl = minLimit(limit, l)
					}
				}
				emitDir := func(body string) {
					b.WriteString("    <S:open-directory name=\"" + xmlEscape(c.Name) + "\" rev=\"" + strconv.FormatUint(outRev, 10) + "\">\n")
					b.WriteString("      <D:checked-in><D:href>" + xmlEscape(ver) + "</D:href></D:checked-in>\n")
					writeEntryProps(b, rp, uuid, rev)
					writeNodeProps(b, c.Props)
					b.WriteString(body)
					b.WriteString("    </S:open-directory>\n")
				}
				if cl >= 0 && childLevel+1 > cl {
					// Depth stops here: emit the directory, no children.
					emitDir("")
					continue
				}
				// mod_dav_svn only reports a subtree that actually changed: a
				// no-op update carries just the root directory. Render the
				// children first and drop the whole element when nothing came
				// out and the directory's own node and props are unchanged.
				var sub strings.Builder
				if err := s.updateWalkDir(&sub, repo, base, rev, cpath, "open-directory", false, rp, uuid, wb, childLevel, cl, false, explicitDepth, copyFloor, "", cwcPath, sendAll); err != nil {
					return err
				}
				needsEmit := sub.Len() > 0 || dirRev > baseRev
				if !needsEmit {
					if bnr, berr := repo.Resolve(baseRev, cwcPath); berr == nil {
						needsEmit = !sameProps(bnr.Props, c.Props)
					} else {
						needsEmit = true
					}
				}
				if !needsEmit {
					continue
				}
				emitDir(sub.String())
			} else {
				bc := base + "!svn/bc/" + strconv.FormatUint(rev, 10) + "/" + strings.TrimPrefix(escapePathSegments(cpath), "/")
				dirRev := c.Rev
				if dnr, derr := repo.GetNodeRev(c.ID); derr == nil {
					if cr := pathCreatedRev(repo, dnr); cr > 0 {
						dirRev = cr
					}
				}
				if dirRev < copyFloor {
					dirRev = copyFloor
				}
				ver := rvrHref(base, cpath, dirRev)
				b.WriteString("    <S:add-directory name=\"" + xmlEscape(c.Name) + "\" bc-url=\"" + xmlEscape(bc) + "\">\n")
				b.WriteString("      <D:checked-in><D:href>" + xmlEscape(ver) + "</D:href></D:checked-in>\n")
				writeEntryProps(b, rp, uuid, rev)
				writeNodeProps(b, c.Props)
				cl := limit
				if !explicitDepth {
					if l, ok := wb.entryDepthLevel(cpath); ok {
						cl = minLimit(limit, l)
					}
				}
				if cl >= 0 && childLevel+1 > cl {
					b.WriteString("    </S:add-directory>\n")
					continue
				}
				if err := s.updateWalkDir(b, repo, base, rev, cpath, "add-directory", false, rp, uuid, wb, childLevel, cl, false, explicitDepth, copyFloor, "", cwcPath, sendAll); err != nil {
					return err
				}
				b.WriteString("    </S:add-directory>\n")
			}
			continue
		}
		nr, err := repo.GetNodeRev(c.ID)
		if err != nil {
			return err
		}
		// Resolve the client's baseline node first. Unchanged files are
		// skipped without ever reading their text: a no-op update used to
		// load every file (and its delta chain) just to compare revisions.
		var baseNode *fs.NodeRev
		if inBase {
			bn, berr := repo.Resolve(baseRev, cwcPath)
			if berr == nil && bn.Kind == fs.KindFile {
				baseNode = bn
			}
		}
		if inBase && baseNode != nil {
			propsChanged := !sameProps(baseNode.Props, nr.Props)
			samePath := normalizeWCPath(path) == normalizeWCPath(wcPath)
			if samePath && c.ID.Rev <= baseRev && !propsChanged {
				continue // unchanged since the client's baseline
			}
			if samePath && c.ID.Rev <= baseRev && propsChanged {
				// Property-only change: open the file solely to set props.
				fileRev := pathCreatedRev(repo, nr)
				if fileRev < copyFloor {
					fileRev = copyFloor
				}
				if fileRev == 0 {
					fileRev = rev
				}
				ver := rvrHref(base, cpath, fileRev)
				b.WriteString("    <S:open-file name=\"" + xmlEscape(c.Name) + "\" rev=\"" + strconv.FormatUint(outRev, 10) + "\">\n")
				b.WriteString("      <D:checked-in><D:href>" + xmlEscape(ver) + "</D:href></D:checked-in>\n")
				writeEntryProps(b, rp, uuid, rev)
				writeNodeProps(b, nr.Props)
				b.WriteString("    </S:open-file>\n")
				continue
			}
			// A switch compares different paths: revisions do not tell us
			// whether the content changed, so compare the text itself. A plain
			// update trusts the node revisions and needs no text at all when
			// the stored checksums are known.
			var text []byte
			if !samePath {
				t, gerr := repo.GetText(nr.Node)
				if gerr != nil {
					return gerr
				}
				bt, gerr := repo.GetText(baseNode.Node)
				if gerr == nil && bytes.Equal(bt, t) && !propsChanged {
					continue
				}
				text = t
			}
			fileRev := pathCreatedRev(repo, nr)
			if fileRev < copyFloor {
				fileRev = copyFloor
			}
			if fileRev == 0 {
				fileRev = rev
			}
			ver := rvrHref(base, cpath, fileRev)
			ss := nr.TextSHA1
			if ss == ([20]byte{}) {
				if text == nil {
					t, gerr := repo.GetText(nr.Node)
					if gerr != nil {
						return gerr
					}
					text = t
				}
				ss = sha1.Sum(text)
			}
			bs := baseNode.TextMD5
			b.WriteString("    <S:open-file name=\"" + xmlEscape(c.Name) + "\" rev=\"" + strconv.FormatUint(outRev, 10) + "\">\n")
			b.WriteString("      <D:checked-in><D:href>" + xmlEscape(ver) + "</D:href></D:checked-in>\n")
			writeEntryProps(b, rp, uuid, rev)
			writeNodeProps(b, nr.Props)
			b.WriteString("      <S:fetch-file base-checksum=\"" + hex.EncodeToString(bs[:]) + "\" sha1-checksum=\"" + hex.EncodeToString(ss[:]) + "\"/>\n")
			b.WriteString("    </S:open-file>\n")
			continue
		}
		// New file in this working copy. The text is only needed when the
		// client asked for inline deltas or when no stored SHA-1 exists.
		var text []byte
		if sendAll || nr.TextSHA1 == ([20]byte{}) {
			t, terr := repo.GetText(nr.Node)
			if terr != nil {
				return terr
			}
			text = t
		}
		fileRev := pathCreatedRev(repo, nr)
		if fileRev < copyFloor {
			fileRev = copyFloor
		}
		if fileRev == 0 {
			fileRev = rev
		}
		ver := rvrHref(base, cpath, fileRev)
		ss := nr.TextSHA1
		if ss == ([20]byte{}) {
			ss = sha1.Sum(text)
		}
		b.WriteString("    <S:add-file name=\"" + xmlEscape(c.Name) + "\" sha1-checksum=\"" + hex.EncodeToString(ss[:]) + "\">\n")
		b.WriteString("      <D:checked-in><D:href>" + xmlEscape(ver) + "</D:href></D:checked-in>\n")
		writeEntryProps(b, rp, uuid, rev)
		writeNodeProps(b, nr.Props)
		if sendAll {
			delta, derr := svndiff.Encode(nil, text)
			if derr != nil {
				return derr
			}
			sum := md5.Sum(text)
			b.WriteString("      <S:txdelta>" + base64.StdEncoding.EncodeToString(delta) + "</S:txdelta>\n")
			b.WriteString("      <S:prop><V:md5-checksum>" + hex.EncodeToString(sum[:]) + "</V:md5-checksum></S:prop>\n")
		}
		b.WriteString("    </S:add-file>\n")
	}
	// Deletions: anything the client has at its baseline but the target lacks.
	if baseRev, inBase := wb.revFor(wcPath); inBase && baseRev > 0 {
		if baseChildren, err := repo.ListDir(baseRev, wcPath); err == nil {
			inTarget := make(map[string]bool, len(children))
			for _, c := range children {
				inTarget[c.Name] = true
			}
			var deleted []string
			for _, bc := range baseChildren {
				if inTarget[bc.Name] {
					continue
				}
				if _, present := wb.revFor(joinWCPath(wcPath, bc.Name)); !present {
					continue
				}
				deleted = append(deleted, bc.Name)
			}
			sort.Strings(deleted)
			for _, name := range deleted {
				b.WriteString("    <S:delete-entry name=\"" + xmlEscape(name) + "\"/>\n")
			}
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
		// mod_dav_svn reports user properties under their DAV name
		// ("custom:NAME"), and the client stores that name in the WC; sending
		// the bare name would leave WC props inconsistent with the server.
		name := k
		if !strings.HasPrefix(k, "svn:") {
			name = "custom:" + k
		}
		b.WriteString("      <S:set-prop name=\"" + xmlEscape(name) + "\">" + xmlEscape(props[k]) + "</S:set-prop>\n")
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
				return "/" + unescapePath(p), true
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
	return "/" + unescapePath(p), true
}

// unescapePath decodes URL-escaped path segments (Chinese / space file names
// arrive in src-path percent-encoded).
func unescapePath(p string) string {
	if dec, err := url.PathUnescape(p); err == nil {
		return dec
	}
	return p
}

// --- log-report (svn log) ---

func (s *Server) reportLog(w http.ResponseWriter, repo *fs.Repo, reportPath string, body []byte) {
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
	// start/end revisions. Clients send them in the requested direction
	// (svn log -r 1:5 sends start=1,end=5); honour that order instead of
	// assuming a descending range, which made ascending requests empty.
	start, end := head, uint64(1)
	if v, ok := root.child("start-revision"); ok {
		if n, err := strconv.ParseUint(strings.TrimSpace(v.Content), 10, 64); err == nil && n <= head {
			start = n
		}
	}
	if v, ok := root.child("end-revision"); ok {
		if n, err := strconv.ParseUint(strings.TrimSpace(v.Content), 10, 64); err == nil && n <= head {
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

	// <S:path> entries are relative to the report URL; a revision is only
	// interesting when one of the requested paths changed in it. Ignoring the
	// filter made `svn log URL/subdir` (and mergeinfo) return every revision.
	basePath := strings.TrimSuffix(reportPath, "/")
	if basePath == "/" {
		basePath = ""
	}
	var filters []string
	for _, c := range root.Children {
		if c.XMLName.Local != "path" {
			continue
		}
		rel := strings.Trim(strings.TrimSpace(c.Content), "/")
		p := basePath
		if rel != "" {
			p = strings.TrimSuffix(basePath, "/") + "/" + rel
		}
		filters = append(filters, p)
	}
	if len(filters) == 0 {
		filters = []string{basePath}
	}

	// requested revprops: which svn:* props the client wants
	requested := map[string]bool{}
	anyRevprop := false
	for _, c := range root.Children {
		if c.XMLName.Local == "revprop" {
			anyRevprop = true
			n := c.attr("name")
			if n == "" {
				n = strings.TrimSpace(c.Content)
			}
			if n != "" {
				requested[n] = true
			}
		}
	}
	noRevProps := false
	if _, ok := root.child("no-revprops"); ok {
		noRevProps = true
	}

	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
	b.WriteString(`<S:log-report xmlns:S="svn:" xmlns:D="` + NS_DAV + `">` + "\n")
	ascending := start < end
	count := uint64(0)
	emit := func(rv uint64) bool {
		if limit > 0 && count >= limit {
			return false
		}
		var changes map[string]change
		if rv > 0 {
			changes, _ = treeDiff(repo, rv)
		}
		// A path filter applies to every log request, whether or not the
		// client asked for changed paths: svn log URL/trunk must not return
		// revisions that only touched /branch.
		if rv > 0 && !anyPathTouched(changes, filters) {
			return true
		}
		count++
		rp, err := repo.GetRevProps(rv)
		if err != nil {
			return true
		}
		b.WriteString("  <S:log-item>\n")
		if discoverChanged {
			s.logChangedPaths(&b, repo, rv, changes)
		}
		b.WriteString("    <D:version-name>" + strconv.FormatUint(rv, 10) + "</D:version-name>\n")
		if !noRevProps && rp != nil && len(rp.Props) > 0 {
			keys := make([]string, 0, len(rp.Props))
			for k := range rp.Props {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				v := rp.Props[k]
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
		return true
	}
	if ascending {
		for rv := start; rv <= end; rv++ {
			if !emit(rv) {
				break
			}
			if rv == end {
				break
			}
		}
	} else {
		for rv := start; ; rv-- {
			if !emit(rv) {
				break
			}
			if rv == end || rv == 0 {
				break
			}
		}
	}
	b.WriteString("</S:log-report>\n")
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, b.String())
}

// anyPathTouched reports whether any changed path is the filter path, lies
// beneath it, or is one of its ancestors (a deleted parent removes the child).
func anyPathTouched(changes map[string]change, filters []string) bool {
	for _, f := range filters {
		if f == "" {
			return true // repository root
		}
		for p := range changes {
			if p == f || strings.HasPrefix(p, f+"/") || strings.HasPrefix(f, p+"/") {
				return true
			}
		}
	}
	return false
}

// logChangedPaths diffs rev vs rev-1 and emits added/modified/deleted paths.
func (s *Server) logChangedPaths(b *strings.Builder, repo *fs.Repo, rev uint64, changes map[string]change) {
	if changes == nil {
		return
	}
	// A directory copy is ONE change: its subtree comes with it. svn only
	// lists descendants whose content differs from the copy source (as
	// replaced), never as plain additions; dumping every child made svn log -v
	// and dump streams disagree with the reference implementation.
	type copyRoot struct {
		path, src string
		srcRev    uint64
	}
	var roots []copyRoot
	for p, ch := range changes {
		if ch.action != chAdded {
			continue
		}
		if nr, err := repo.Resolve(rev, p); err == nil && nr.CopyFrom != nil && nr.CopyFromPath != "" {
			roots = append(roots, copyRoot{path: p, src: nr.CopyFromPath, srcRev: nr.CopyFrom.Rev})
		}
	}
	for _, cr := range roots {
		for p := range changes {
			if !strings.HasPrefix(p, cr.path+"/") {
				continue
			}
			suffix := p[len(cr.path):]
			nr, err := repo.Resolve(rev, p)
			if err != nil {
				continue
			}
			// Redundant copy node (the client re-copies every child) or the
			// very same object as the source: not a distinct change.
			if nr.CopyFrom != nil && nr.CopyFromPath == cr.src+suffix && nr.CopyFrom.Rev == cr.srcRev {
				delete(changes, p)
				continue
			}
			src, serr := repo.Resolve(cr.srcRev, cr.src+suffix)
			if serr == nil && src.Node == nr.Node {
				delete(changes, p)
				continue
			}
			if serr != nil {
				// Not part of the copy source (added after the copy): a plain
				// addition, not a replacement.
				continue
			}
			// Present in the source but different here: svn reports replaced
			// with the child's own copy source (the source path with this
			// suffix, as of the revision the child came from).
			cfRev := nr.Node.Rev
			if nr.CopyFrom != nil && nr.CopyFrom.Rev > 0 {
				cfRev = nr.CopyFrom.Rev
			}
			changes[p] = change{action: chReplaced, kind: nr.Kind,
				textMods: nr.Kind == fs.KindFile, propMods: false,
				copyFromPath: cr.src + suffix, copyFromRev: cfRev}
		}
	}
	paths := make([]string, 0, len(changes))
	for p := range changes {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	kindName := func(k byte) string {
		if k == fs.KindDir {
			return "dir"
		}
		return "file"
	}
	for _, p := range paths {
		ch := changes[p]
		// svn exposes node-kind/text-mods/prop-mods on changed paths; clients
		// use them (mergeinfo, svn log -v) to tell content from property edits.
		attrs := " node-kind=\"" + kindName(ch.kind) + "\" text-mods=\"" +
			strconv.FormatBool(ch.textMods) + "\" prop-mods=\"" + strconv.FormatBool(ch.propMods) + "\""
		switch ch.action {
		case chAdded:
			if nr, err := repo.Resolve(rev, p); err == nil && nr.CopyFrom != nil {
				b.WriteString("    <S:added-path copyfrom-path=\"" + xmlEscape(nr.CopyFromPath) + "\" copyfrom-rev=\"" +
					strconv.FormatUint(nr.CopyFrom.Rev, 10) + "\"" + attrs + ">" + xmlEscape(p) + "</S:added-path>\n")
				continue
			}
			b.WriteString("    <S:added-path" + attrs + ">" + xmlEscape(p) + "</S:added-path>\n")
		case chModified:
			b.WriteString("    <S:modified-path" + attrs + ">" + xmlEscape(p) + "</S:modified-path>\n")
		case chDeleted:
			b.WriteString("    <S:deleted-path" + attrs + ">" + xmlEscape(p) + "</S:deleted-path>\n")
		case chReplaced:
			cf := ""
			if ch.copyFromPath != "" {
				cf = " copyfrom-path=\"" + xmlEscape(ch.copyFromPath) + "\" copyfrom-rev=\"" +
					strconv.FormatUint(ch.copyFromRev, 10) + "\""
			}
			b.WriteString("    <S:replaced-path" + cf + attrs + ">" + xmlEscape(p) + "</S:replaced-path>\n")
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

type change struct {
	action   changeKind
	kind     byte
	textMods bool
	propMods bool
	// copyfrom for replaced entries inside a copied directory (svn records
	// the child's own copy source there, which merge tracking uses).
	copyFromPath string
	copyFromRev  uint64
}

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
			out[path] = change{action: chAdded, kind: cur.Kind, textMods: cur.Kind == fs.KindFile, propMods: len(cur.Props) > 0}
			if cur.Kind == fs.KindDir {
				children, _ := repo.ListDir(rev, path)
				for _, c := range children {
					if err := walk(prevRev, rev, strings.TrimSuffix(path, "/")+"/"+c.Name); err != nil {
						return err
					}
				}
			}
			return nil
		case err != nil && prevErr2 == nil:
			out[path] = change{action: chDeleted, kind: prev.Kind}
			return nil
		case err != nil && prevErr2 != nil:
			return nil
		}
		// both exist: compare kind + (for files) text + props
		if cur.Kind != prev.Kind {
			out[path] = change{action: chReplaced, kind: cur.Kind, textMods: cur.Kind == fs.KindFile, propMods: true}
		} else if cur.Kind == fs.KindDir {
			curCh, _ := repo.ListDir(rev, path)
			prevCh, _ := repo.ListDir(prevRev, path)
			names := map[string]bool{}
			for _, c := range curCh {
				names[c.Name] = true
			}
			for _, c := range prevCh {
				if !names[c.Name] {
					if err := walk(prevRev, rev, strings.TrimSuffix(path, "/")+"/"+c.Name); err != nil {
						return err
					}
				}
			}
			// record directory modification if its own props changed
			if !sameProps(cur.Props, prev.Props) {
				out[path] = change{action: chModified, kind: cur.Kind, propMods: true}
			}
			for _, c := range curCh {
				if err := walk(prevRev, rev, strings.TrimSuffix(path, "/")+"/"+c.Name); err != nil {
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
				out[path] = change{action: chModified, kind: cur.Kind,
					textMods: !bytesEqual(ct, pt), propMods: !sameProps(cur.Props, prev.Props)}
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
func (s *Server) reportList(w http.ResponseWriter, r *http.Request, repo *fs.Repo, base, repoName, reportPath string, body []byte) {
	var root xmlEl
	if err := xml.Unmarshal(body, &root); err != nil {
		http.Error(w, "bad list-report", http.StatusBadRequest)
		return
	}
	head, err := repo.Head()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rev := head
	if e, ok := root.child("revision"); ok {
		if n, err := strconv.ParseUint(strings.TrimSpace(e.Content), 10, 64); err == nil && n <= head {
			rev = n
		}
	}
	depth := "infinity"
	if e, ok := root.child("depth"); ok && strings.TrimSpace(e.Content) != "" {
		depth = strings.TrimSpace(e.Content)
	}
	// Requested props: libsvn asks for DAV:* and expects them as item
	// attributes (size/created-rev/date) or child elements.
	var props map[string]bool
	for _, c := range root.Children {
		if c.XMLName.Local == "prop" {
			if props == nil {
				props = map[string]bool{}
			}
			props[strings.TrimSpace(c.Content)] = true
		}
	}
	target := strings.TrimSuffix(reportPath, "/")
	if rel := strings.Trim(strings.TrimSpace(childText(root, "path")), "/"); rel != "" {
		target = strings.TrimSuffix(target, "/") + "/" + rel
	}
	if target == "" {
		target = "/"
	}
	nr, err := repo.Resolve(rev, target)
	if err != nil {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
	// The svn protocol namespace is literally "svn:" (NS_SVN is the property
	// namespace and makes clients reject the report).
	b.WriteString(`<S:list-report xmlns:S="svn:" xmlns:D="` + NS_DAV + `">` + "\n")

	az := s.cfg.authzFor(repoName)
	user := IdentityFrom(r.Context()).Username
	allowed := func(p string) bool {
		return az == nil || az.Authorize(user, repoName, p, false)
	}
	var emit func(path string, node *fs.NodeRev)
	emit = func(path string, node *fs.NodeRev) {
		if !allowed(path) {
			return
		}
		kind := "file"
		size := int64(-1)
		if node.Kind == fs.KindDir {
			kind = "dir"
		} else if txt, err := repo.GetText(node.Node); err == nil {
			size = int64(len(txt))
		}
		attrs := ` node-kind="` + kind + `"`
		if props["DAV:getcontentlength"] {
			attrs += ` size="` + strconv.FormatInt(size, 10) + `"`
		}
		if props["DAV:version-name"] {
			attrs += ` created-rev="` + strconv.FormatUint(node.Node.Rev, 10) + `"`
		}
		var author, date string
		if rp, err := repo.GetRevProps(node.Node.Rev); err == nil && rp != nil {
			author = rp.Props["svn:author"]
			date = rp.Props["svn:date"]
		}
		if props["DAV:creationdate"] && date != "" {
			attrs += ` date="` + xmlEscape(date) + `"`
		}
		b.WriteString("<S:item" + attrs + ">" + xmlEscape(path))
		if props["DAV:creator-displayname"] {
			b.WriteString("<D:creator-displayname>" + xmlEscape(author) + "</D:creator-displayname>")
		}
		b.WriteString("</S:item>\n")
	}
	emit(target, nr)

	var walk func(path string, level int)
	walk = func(path string, level int) {
		children, err := repo.ListDir(rev, path)
		if err != nil {
			return
		}
		sort.Slice(children, func(i, j int) bool { return children[i].Name < children[j].Name })
		for _, c := range children {
			childPath := strings.TrimSuffix(path, "/") + "/" + c.Name
			if !allowed(childPath) {
				continue
			}
			cnr, err := repo.GetNodeRev(c.ID)
			if err != nil {
				continue
			}
			if depth == "files" && cnr.Kind == fs.KindDir {
				continue
			}
			emit(childPath, cnr)
			if cnr.Kind == fs.KindDir && depth == "infinity" {
				walk(childPath, level+1)
			}
		}
	}
	switch depth {
	case "empty":
		// target only
	case "files", "immediates", "infinity":
		walk(target, 0)
	}
	b.WriteString("</S:list-report>\n")
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, b.String())
}

// childText returns the trimmed text of the first child element with local
// name local.
func childText(e xmlEl, local string) string {
	for _, c := range e.Children {
		if c.XMLName.Local == local {
			return strings.TrimSpace(c.Content)
		}
	}
	return ""
}

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
// copyOrigin finds the copy that produced a node's lineage: the revision in
// which the copy was committed (the copy root's node revision, which can be
// older than the node currently resolved) plus its source path. copyFrom.Rev
// alone is NOT the copy point — it is the source revision, which made
// get-location-segments report the wrong ranges for a copied branch.
func copyOrigin(repo *fs.Repo, nr *fs.NodeRev) (copyRev uint64, srcPath string, srcRev uint64, ok bool) {
	if nr == nil || nr.CopyFrom == nil || nr.CopyFromPath == "" {
		return 0, "", 0, false
	}
	srcPath = nr.CopyFromPath
	srcRev = nr.CopyFrom.Rev
	cur := nr
	for !cur.Pred.IsZero() {
		p, err := repo.GetNodeRev(cur.Pred)
		if err != nil {
			break
		}
		if p.CopyFrom == nil || p.CopyFromPath != srcPath || p.CopyFrom.Rev != srcRev {
			break
		}
		cur = p
	}
	return cur.Node.Rev, srcPath, srcRev, true
}

// lineagePaths maps every revision in [end, start] to the path the tracked
// node had there ("" when it did not exist at that revision).
func lineagePaths(repo *fs.Repo, path string, start, end uint64) map[uint64]string {
	out := map[uint64]string{}
	cur := path
	rev := start
	for rev >= end && rev >= 1 {
		nr, err := repo.Resolve(rev, cur)
		if err != nil {
			out[rev] = ""
			rev--
			continue
		}
		out[rev] = cur
		cr, src, srcRev, ok := copyOrigin(repo, nr)
		if !ok || rev-1 >= cr {
			rev--
			continue
		}
		// Crossing below the copy point. The copied node corresponds to the
		// source AT srcRev only: revisions between srcRev and the copy are a
		// gap (the path did not exist then), and svn reports them as a segment
		// without a path. Reporting the source path there made merge ranges
		// start one revision late.
		if srcRev >= 1 && rev-1 >= srcRev {
			for r := rev - 1; r > srcRev && r >= end && r >= 1; r-- {
				out[r] = ""
			}
			if srcRev >= end {
				out[srcRev] = src
			}
			cur = src
			if srcRev <= 1 {
				break
			}
			rev = srcRev - 1
			continue
		}
		if srcRev <= 1 {
			if srcRev == 1 && srcRev >= end {
				out[srcRev] = src
			}
			break
		}
		cur = src
		rev = srcRev - 1
	}
	return out
}

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
	// The path as it existed at each revision from peg down to loc, following
	// copy sources at the revision where each copy was committed.
	lineage := lineagePaths(repo, path, peg, loc)
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

// reportGetLocationSegments answers get-location-segments, the report svn uses
// for switch/relocate and merge source tracking (SVN 1.8+). The response root
// and child element are protocol-defined: <S:get-location-segments-report>
// with <S:location-segment range-start= range-end= path=>. Answering it with
// the older get-locations-report shape makes ra_serf abort with
// "Unexpected root element".
func (s *Server) reportGetLocationSegments(w http.ResponseWriter, repo *fs.Repo, resPath string, body []byte) {
	var root xmlEl
	if err := xml.Unmarshal(body, &root); err != nil {
		http.Error(w, "bad get-location-segments", http.StatusBadRequest)
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
	num := func(name string, def uint64) uint64 {
		if e, ok := root.child(name); ok {
			if n, err := strconv.ParseUint(strings.TrimSpace(e.Content), 10, 64); err == nil {
				return n
			}
		}
		return def
	}
	peg := num("peg-revision", head)
	if peg > head {
		peg = head
	}
	start := num("start-revision", peg)
	end := num("end-revision", 1)
	if start > peg {
		start = peg
	}
	if end < 1 {
		end = 1
	}
	if start < end {
		start, end = end, start
	}

	pathAt := lineagePaths(repo, path, start, end)

	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
	b.WriteString(`<S:get-location-segments-report xmlns:S="svn:" xmlns:D="` + NS_DAV + `">` + "\n")
	// Emit contiguous runs, highest range first (svn_ra_get_location_segments
	// delivers segments in descending order).
	runHi := start
	runPath := pathAt[start]
	for rev := int64(start) - 1; rev >= int64(end); rev-- {
		p := pathAt[uint64(rev)]
		if p == runPath {
			continue
		}
		writeSegment(&b, runPath, uint64(rev+1), runHi)
		runHi = uint64(rev)
		runPath = p
	}
	writeSegment(&b, runPath, end, runHi)
	b.WriteString("</S:get-location-segments-report>\n")
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	fmt.Fprint(w, b.String())
}

// writeSegment emits one <S:location-segment>. svn_location_segment_t defines
// range_start as the OLDEST revision and range_end as the YOUNGEST (both
// inclusive), so lo goes to range-start. The path must be absolute *without* a
// leading slash (clients re-add it when deriving merge ranges).
func writeSegment(b *strings.Builder, path string, lo, hi uint64) {
	p := ""
	if path != "" {
		p = ` path="` + xmlEscape(strings.TrimPrefix(path, "/")) + `"`
	}
	b.WriteString(`  <S:location-segment range-start="` + strconv.FormatUint(lo, 10) +
		`" range-end="` + strconv.FormatUint(hi, 10) + `"` + p + "/>\n")
}

// reportGetLocks answers a get-locks REPORT: the set of active locks for the
// repository (or, if a <S:path> is given, locks at/under that path).
func (s *Server) reportGetLocks(w http.ResponseWriter, repo *fs.Repo, repoName, urlPath string, body []byte) {
	var root xmlEl
	if err := xml.Unmarshal(body, &root); err != nil {
		http.Error(w, "bad get-locks", http.StatusBadRequest)
		return
	}
	// The report targets the URL it was sent to; an explicit <S:path> (if any)
	// narrows it further. depth=immediates means "this node and its direct
	// children", infinity means the whole subtree.
	filter := urlPath
	if c, ok := root.child("path"); ok && strings.TrimSpace(c.Content) != "" {
		filter = strings.TrimSpace(c.Content)
	}
	filter = "/" + strings.Trim(filter, "/")
	if filter == "/" {
		filter = ""
	}
	depth := root.attr("depth")
	if depth == "" {
		depth = "infinity"
	}
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
	// svn_ra_serf's getlocks parser requires this exact root/child shape:
	// <S:get-locks-report><S:lock><S:path/><S:token/><S:owner/>...  The DAV
	// <D:locks> shape used previously is rejected as an unexpected root.
	b.WriteString(`<S:get-locks-report xmlns:S="svn:" xmlns:D="` + NS_DAV + `">` + "\n")
	s.mu.Lock()
	prefix := repoName + "\x00"
	keys := make([]string, 0, len(s.locks))
	for key := range s.locks {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		p := key[len(prefix):]
		if filter != "" && p != filter && !strings.HasPrefix(p, filter+"/") {
			continue
		}
		if filter != "" && p != filter && depth == "immediates" {
			rel := strings.TrimPrefix(p, filter+"/")
			if strings.Contains(rel, "/") {
				continue
			}
		}
		e := s.locks[key]
		if e.expired() {
			continue
		}
		b.WriteString("  <S:lock>\n")
		b.WriteString("    <S:path>" + xmlEscape(p) + "</S:path>\n")
		b.WriteString("    <S:token>" + xmlEscape(e.token) + "</S:token>\n")
		b.WriteString("    <S:owner>" + xmlEscape(e.owner) + "</S:owner>\n")
		b.WriteString("    <S:creationdate>" + e.created.UTC().Format("2006-01-02T15:04:05.000000Z") + "</S:creationdate>\n")
		if e.timeout > 0 {
			exp := e.created.Add(time.Duration(e.timeout) * time.Second).UTC()
			b.WriteString("    <S:expirationdate>" + exp.Format("2006-01-02T15:04:05.000000Z") + "</S:expirationdate>\n")
		}
		b.WriteString("  </S:lock>\n")
	}
	s.mu.Unlock()
	b.WriteString("</S:get-locks-report>\n")
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	fmt.Fprint(w, b.String())
}

// reportGetMergeinfo answers a mergeinfo-report: for each requested (relative)
// path it returns that node's svn:mergeinfo property at the requested
// revision. The response shape is fixed by libsvn_ra_serf/mergeinfo.c:
//
//	<S:mergeinfo-report><S:mergeinfo-item>
//	  <S:mergeinfo-path>REL</S:mergeinfo-path>
//	  <S:mergeinfo-info>SOURCE:RANGES</S:mergeinfo-info>
//	</S:mergeinfo-item></S:mergeinfo-report>
//
// Inherited mergeinfo (<S:inherit>inherited</S:inherit>) is not expanded yet:
// own mergeinfo is returned, which is what svn merge/mergeinfo need in the
// common case where mergeinfo lives on the merge target itself.
func (s *Server) reportGetMergeinfo(w http.ResponseWriter, repo *fs.Repo, urlPath string, body []byte) {
	var root xmlEl
	rev := uint64(0)
	haveRev := false
	inherited := false
	var relPaths []string
	if err := xml.Unmarshal(body, &root); err == nil {
		if e, ok := root.child("revision"); ok {
			if n, err := strconv.ParseUint(strings.TrimSpace(e.Content), 10, 64); err == nil {
				rev, haveRev = n, true
			}
		}
		if e, ok := root.child("inherit"); ok {
			inherited = strings.EqualFold(strings.TrimSpace(e.Content), "inherited")
		}
		for _, c := range root.Children {
			if c.XMLName.Local == "path" {
				relPaths = append(relPaths, strings.TrimSpace(c.Content))
			}
		}
	}
	if !haveRev {
		head, err := repo.Head()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		rev = head
	}
	if len(relPaths) == 0 {
		relPaths = []string{""}
	}
	base := strings.TrimSuffix(urlPath, "/")

	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
	b.WriteString(`<S:mergeinfo-report xmlns:S="svn:" xmlns:D="` + NS_DAV + `">` + "\n")
	for _, rel := range relPaths {
		actual := base
		if rel != "" {
			actual = strings.TrimSuffix(base, "/") + "/" + strings.Trim(rel, "/")
		}
		if actual == "" {
			actual = "/"
		}
		nr, err := repo.Resolve(rev, actual)
		if err != nil {
			continue
		}
		mi := parseMergeinfo(nr.Props["svn:mergeinfo"])
		if inherited {
			// Ancestors' mergeinfo applies to the descendant with its source
			// paths shifted by the relative path; only inheritable ranges
			// propagate.
			anc := actual
			for {
				anc = strings.TrimSuffix(anc, "/")
				if i := strings.LastIndex(anc, "/"); i > 0 {
					anc = anc[:i]
				} else {
					anc = "/"
				}
				anr, aerr := repo.Resolve(rev, anc)
				if aerr == nil && anr.Props["svn:mergeinfo"] != "" {
					rel := strings.TrimPrefix(actual, anc)
					rel = strings.TrimPrefix(rel, "/")
					if anc == "/" {
						rel = strings.TrimPrefix(actual, "/")
					}
					mi = unionMergeinfo(mi, translateMergeinfo(inheritableOnly(parseMergeinfo(anr.Props["svn:mergeinfo"])), rel))
				}
				if anc == "/" {
					break
				}
			}
		}
		info := formatMergeinfo(mi)
		if info == "" {
			continue
		}
		b.WriteString("  <S:mergeinfo-item>\n")
		b.WriteString("    <S:mergeinfo-path>" + xmlEscape(rel) + "</S:mergeinfo-path>\n")
		b.WriteString("    <S:mergeinfo-info>" + xmlEscape(info) + "</S:mergeinfo-info>\n")
		b.WriteString("  </S:mergeinfo-item>\n")
	}
	b.WriteString("</S:mergeinfo-report>\n")
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	fmt.Fprint(w, b.String())
}
