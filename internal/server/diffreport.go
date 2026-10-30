package server

import (
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"svngod/internal/fs"
)

// reportDiff answers the two-tree form of update-report:
//
//	src-path @ entry-rev  ->  dst-path @ target-revision
//
// svn uses it for cross-URL merges (including --reintegrate) and for
// repository-to-repository diffs. Treating such a request as a normal update
// (target path against the WC baseline) made merges compare the target tree
// with itself, so incoming files were reported as conflicts and branch-only
// files were never merged.
func (s *Server) reportDiff(w http.ResponseWriter, repo *fs.Repo, base string, dstRev uint64, srcPath, dstPath string, entryRev uint64, limit int, filesOnly bool) {
	srcPath = normalizeWCPath(srcPath)
	dstPath = normalizeWCPath(dstPath)
	uuid, _ := repo.UUID()

	srcNR, srcErr := repo.Resolve(entryRev, srcPath)
	dstNR, dstErr := repo.Resolve(dstRev, dstPath)
	if dstErr != nil {
		http.NotFound(w, nil)
		return
	}
	if srcErr != nil {
		srcNR = nil
	}

	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
	b.WriteString(`<S:update-report xmlns:S="svn:" xmlns:D="` + NS_DAV + `" xmlns:V="http://subversion.tigris.org/xmlns/dav/" inline-props="true">` + "\n")
	b.WriteString("  <S:target-revision rev=\"" + strconv.FormatUint(dstRev, 10) + "\"/>\n")
	b.WriteString("  <S:open-directory rev=\"" + strconv.FormatUint(entryRev, 10) + "\">\n")
	b.WriteString("    <D:checked-in><D:href>" + xmlEscape(diffCheckedIn(repo, base, dstPath, dstNR)) + "</D:href></D:checked-in>\n")
	writeEntryProps(&b, nodeRevProps(repo, dstNR), uuid, dstNR.Node.Rev)
	writeNodeProps(&b, dstNR.Props)

	d := &diffCtx{repo: repo, base: base, entryRev: entryRev, dstRev: dstRev, srcRoot: srcPath, dstRoot: dstPath}
	if dstNR.Kind == fs.KindDir {
		content, _, err := d.renderDir(srcPath, dstPath, srcNR, dstNR, limit, 0)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		b.WriteString(content)
	}
	b.WriteString("  </S:open-directory>\n")
	b.WriteString("</S:update-report>\n")
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, b.String())
}

type diffCtx struct {
	repo     *fs.Repo
	base     string
	entryRev uint64
	dstRev   uint64
	srcRoot  string
	dstRoot  string
}

func nodeRevProps(repo *fs.Repo, nr *fs.NodeRev) *fs.RevProps {
	if nr == nil {
		return nil
	}
	rp, _ := repo.GetRevProps(nr.Node.Rev)
	return rp
}

// diffCheckedIn builds the checked-in href for a node of the target tree.
func diffCheckedIn(repo *fs.Repo, base, path string, nr *fs.NodeRev) string {
	if nr == nil {
		return rvrHref(base, path, 0)
	}
	rev := pathCreatedRev(repo, nr)
	if rev == 0 {
		rev = nr.Node.Rev
	}
	return rvrHref(base, path, rev)
}

func dirChild(repo *fs.Repo, rev uint64, path, name string) *fs.NodeRev {
	nr, err := repo.Resolve(rev, joinWCPath(path, name))
	if err != nil {
		return nil
	}
	return nr
}

// renderDir diffs one directory of the target tree against the source tree and
// returns the editor commands for its changed children.
func (d *diffCtx) renderDir(srcPath, dstPath string, srcNR, dstNR *fs.NodeRev, limit, level int) (string, bool, error) {
	var out strings.Builder
	changed := false

	// List at the report revision, not the node's revision: nodes inside a
	// copy share the source's node-rev ids and do not exist at that revision
	// under their new path.
	children, err := d.repo.ListDir(d.dstRev, dstPath)
	if err != nil {
		return "", false, err
	}
	sort.Slice(children, func(i, j int) bool { return children[i].Name < children[j].Name })

	dstNames := map[string]bool{}
	for _, c := range children {
		dstNames[c.Name] = true
		cnr, err := d.repo.GetNodeRev(c.ID)
		if err != nil {
			return "", false, err
		}
		if limit >= 0 && level+1 > limit {
			continue
		}
		var srcChild *fs.NodeRev
		if srcNR != nil && srcNR.Kind == fs.KindDir {
			srcChild = dirChild(d.repo, d.entryRev, srcPath, c.Name)
		}
		cpath := joinWCPath(dstPath, c.Name)
		switch {
		case srcChild != nil && srcChild.Kind == fs.KindDir && cnr.Kind == fs.KindDir:
			sub, subChanged, err := d.renderDir(joinWCPath(srcPath, c.Name), cpath, srcChild, cnr, limit, level+1)
			if err != nil {
				return "", false, err
			}
			if !subChanged {
				continue
			}
			changed = true
			out.WriteString("    <S:open-directory name=\"" + xmlEscape(c.Name) + "\" rev=\"" + strconv.FormatUint(d.entryRev, 10) + "\">\n")
			out.WriteString("      <D:checked-in><D:href>" + xmlEscape(diffCheckedIn(d.repo, d.base, cpath, cnr)) + "</D:href></D:checked-in>\n")
			writeEntryProps(&out, nodeRevProps(d.repo, cnr), "", cnr.Node.Rev)
			writeNodeProps(&out, cnr.Props)
			out.WriteString(sub)
			out.WriteString("    </S:open-directory>\n")
		case cnr.Kind == fs.KindDir:
			// New directory (or a file replaced by a directory): add its
			// whole subtree.
			if srcChild != nil {
				out.WriteString("    <S:delete-entry name=\"" + xmlEscape(c.Name) + "\"/>\n")
			}
			changed = true
			out.WriteString(d.renderAddDir(cpath, c.Name, cnr, limit, level+1))
		case srcChild != nil && srcChild.Kind == fs.KindFile:
			ops, diff := d.renderFileChange(cpath, c.Name, srcChild, cnr)
			if diff {
				changed = true
				out.WriteString(ops)
			}
		default:
			// Added file (or directory replaced by a file).
			if srcChild != nil {
				out.WriteString("    <S:delete-entry name=\"" + xmlEscape(c.Name) + "\"/>\n")
			}
			changed = true
			out.WriteString(d.renderAddFile(cpath, c.Name, cnr))
		}
	}
	// Deletions: in the source tree but not in the target.
	if srcNR != nil && srcNR.Kind == fs.KindDir && (limit < 0 || level+1 <= limit) {
		srcChildren, err := d.repo.ListDir(d.entryRev, srcPath)
		if err == nil {
			var deleted []string
			for _, sc := range srcChildren {
				if !dstNames[sc.Name] {
					deleted = append(deleted, sc.Name)
				}
			}
			sort.Strings(deleted)
			for _, name := range deleted {
				changed = true
				out.WriteString("    <S:delete-entry name=\"" + xmlEscape(name) + "\"/>\n")
			}
		}
	}
	return out.String(), changed, nil
}

// renderFileChange emits open-file when text or props differ.
func (d *diffCtx) renderFileChange(path, name string, srcNR, dstNR *fs.NodeRev) (string, bool) {
	srcText, err1 := d.repo.GetText(srcNR.Node)
	dstText, err2 := d.repo.GetText(dstNR.Node)
	if err1 != nil || err2 != nil {
		return "", false
	}
	propsChanged := !sameProps(srcNR.Props, dstNR.Props)
	if string(srcText) == string(dstText) && !propsChanged {
		return "", false
	}
	var b strings.Builder
	b.WriteString("    <S:open-file name=\"" + xmlEscape(name) + "\" rev=\"" + strconv.FormatUint(d.entryRev, 10) + "\">\n")
	b.WriteString("      <D:checked-in><D:href>" + xmlEscape(diffCheckedIn(d.repo, d.base, path, dstNR)) + "</D:href></D:checked-in>\n")
	writeEntryProps(&b, nodeRevProps(d.repo, dstNR), "", dstNR.Node.Rev)
	writeNodeProps(&b, dstNR.Props)
	if string(srcText) != string(dstText) {
		baseSum := md5.Sum(srcText)
		sum := sha1.Sum(dstText)
		b.WriteString("      <S:fetch-file base-checksum=\"" + hex.EncodeToString(baseSum[:]) +
			"\" sha1-checksum=\"" + hex.EncodeToString(sum[:]) + "\"/>\n")
		m := md5.Sum(dstText)
		b.WriteString("      <S:prop><V:md5-checksum>" + hex.EncodeToString(m[:]) + "</V:md5-checksum></S:prop>\n")
	}
	b.WriteString("    </S:open-file>\n")
	return b.String(), true
}

// renderAddFile emits add-file. Like mod_dav_svn's diff reports it carries no
// txdelta: the client fetches the text from the checked-in href.
func (d *diffCtx) renderAddFile(path, name string, nr *fs.NodeRev) string {
	var b strings.Builder
	text, _ := d.repo.GetText(nr.Node)
	sum := sha1.Sum(text)
	m := md5.Sum(text)
	b.WriteString("    <S:add-file name=\"" + xmlEscape(name) + "\" sha1-checksum=\"" + hex.EncodeToString(sum[:]) + "\">\n")
	b.WriteString("      <D:checked-in><D:href>" + xmlEscape(diffCheckedIn(d.repo, d.base, path, nr)) + "</D:href></D:checked-in>\n")
	writeEntryProps(&b, nodeRevProps(d.repo, nr), "", nr.Node.Rev)
	writeNodeProps(&b, nr.Props)
	b.WriteString("      <S:prop><V:md5-checksum>" + hex.EncodeToString(m[:]) + "</V:md5-checksum></S:prop>\n")
	b.WriteString("    </S:add-file>\n")
	return b.String()
}

// renderAddDir emits an added directory and its whole subtree.
func (d *diffCtx) renderAddDir(path, name string, nr *fs.NodeRev, limit, level int) string {
	var b strings.Builder
	b.WriteString("    <S:add-directory name=\"" + xmlEscape(name) + "\" bc-url=\"" +
		xmlEscape(d.base+"!svn/bc/"+strconv.FormatUint(nr.Node.Rev, 10)+"/"+strings.TrimPrefix(escapePathSegments(path), "/")) + "\">\n")
	b.WriteString("      <D:checked-in><D:href>" + xmlEscape(diffCheckedIn(d.repo, d.base, path, nr)) + "</D:href></D:checked-in>\n")
	writeEntryProps(&b, nodeRevProps(d.repo, nr), "", nr.Node.Rev)
	writeNodeProps(&b, nr.Props)
	if limit < 0 || level < limit {
		children, err := d.repo.ListDir(d.dstRev, path)
		if err == nil {
			sort.Slice(children, func(i, j int) bool { return children[i].Name < children[j].Name })
			for _, c := range children {
				cnr, err := d.repo.GetNodeRev(c.ID)
				if err != nil {
					continue
				}
				cpath := joinWCPath(path, c.Name)
				if cnr.Kind == fs.KindDir {
					b.WriteString(d.renderAddDir(cpath, c.Name, cnr, limit, level+1))
				} else {
					b.WriteString(d.renderAddFile(cpath, c.Name, cnr))
				}
			}
		}
	}
	b.WriteString("    </S:add-directory>\n")
	return b.String()
}
