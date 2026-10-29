// Package server implements the WebDAV/DeltaV HTTP layer that lets standard
// Subversion clients (svn, TortoiseSVN, IDE plugins) talk to a svngod repo.
package server

import (
	"encoding/xml"
	"fmt"
	"strings"
)

// XML namespaces used by the Subversion DAV protocol (mirrors mod_dav_svn).
const (
	NS_DAV    = "DAV:"
	NS_SVN    = "http://subversion.tigris.org/xmlns/svn/"  // svn:* properties
	NS_DAVSVN = "http://subversion.tigris.org/xmlns/dav/" // protocol elements (update-report, checkout, ...)
	NS_CSVN   = "http://subversion.tigris.org/xmlns/custom/"
)

// The DAV capabilities advertised in OPTIONS (RFC 3253 DeltaV + SVN extras).
// svndiff1/svndiff2 advertise that we accept compressed (zlib/lz4) svndiff
// windows on PUT; the svn client then picks v1/v2 instead of plain v0.
const davCapabilities = "1, 2, 3, version-control, checkout, working-resource, merge, baseline, activity, version-controlled-collection, http://subversion.tigris.org/xmlns/dav/svn/svndiff1"

// allowedMethods advertised in OPTIONS Allow.
const allowedMethods = "OPTIONS, PROPFIND, GET, REPORT, MKACTIVITY, PROPPATCH, PUT, CHECKOUT, MERGE, DELETE, COPY, MOVE, LOCK, UNLOCK"

// Name is a qualified XML element/property name.
type Name struct {
	NS   string
	Local string
}

func (n Name) String() string { return "{" + n.NS + "}" + n.Local }

// davsn returns the svn-protocol namespace URI.
func davsn() string { return NS_DAVSVN }

// Names we recognise when parsing client requests.
var (
	// DAV delta-v elements (RFC 3253)
	enVcc          = Name{NS_DAV, "version-controlled-configuration"}
	enBaseline     = Name{NS_DAV, "baseline"}
	enBaselineColl = Name{NS_DAV, "baseline-collection"}
	enActivity     = Name{NS_DAV, "activity"}
	enCheckout     = Name{NS_DAV, "checkout"}
	enMerge        = Name{NS_DAV, "merge"}
	enMkactivity   = Name{NS_DAV, "mkactivity"}
	enResourcetype = Name{NS_DAV, "resourcetype"}
	enCollection   = Name{NS_DAV, "collection"}
	enProp         = Name{NS_DAV, "prop"}
	enPropstat     = Name{NS_DAV, "propstat"}
	enStatus       = Name{NS_DAV, "status"}
	enResponse     = Name{NS_DAV, "response"}
	enMultistatus  = Name{NS_DAV, "multistatus"}
	enHref         = Name{NS_DAV, "href"}
	enPropfind     = Name{NS_DAV, "propfind"}
	enAllprop      = Name{NS_DAV, "allprop"}
	enPropname     = Name{NS_DAV, "propname"}
	enGetetag      = Name{NS_DAV, "getetag"}
	enGetcontentlength = Name{NS_DAV, "getcontentlength"}
	enGetcontenttype   = Name{NS_DAV, "getcontenttype"}
	enDisplayname      = Name{NS_DAV, "displayname"}
	enVersionname      = Name{NS_DAV, "version-name"}
	enBaselineRelative = Name{NS_DAV, "baseline-relative-path"}
	enRepositoryUUID   = Name{NS_DAV, "repository-uuid"}
	enRoot             = Name{NS_DAV, "root"}
	enCreatorDisplayname = Name{NS_DAV, "creator-displayname"}
	enSupportedLock = Name{NS_DAV, "supportedlock"}
	enLockentry     = Name{NS_DAV, "lockentry"}
	enLockscope     = Name{NS_DAV, "lockscope"}
	enExclusive     = Name{NS_DAV, "exclusive"}
	enShared        = Name{NS_DAV, "shared"}
	enLocktype      = Name{NS_DAV, "locktype"}
	enWrite         = Name{NS_DAV, "write"}

	// svn protocol elements (namespace http://subversion.tigris.org/xmlns/dav/)
	enUpdateReport = Name{NS_DAVSVN, "update-report"}
	enLogReport    = Name{NS_DAVSVN, "log-report"}
	enGetLocations = Name{NS_DAVSVN, "get-locations"}
	enRev          = Name{NS_DAVSVN, "rev"}
	enStartRev     = Name{NS_DAVSVN, "start-revision"}
	enEndRev       = Name{NS_DAVSVN, "end-revision"}
	enPath         = Name{NS_DAVSVN, "path"}
	enTarget       = Name{NS_DAVSVN, "target"}
	enEntry        = Name{NS_DAVSVN, "entry"}
	enMissing      = Name{NS_DAVSVN, "missing"}
	enCheckedIn    = Name{NS_DAVSVN, "checked-in"}
	enOutOfDate    = Name{NS_DAVSVN, "out-of-date"}
	enAddedPath    = Name{NS_DAVSVN, "added-path"}
	enDeletedPath  = Name{NS_DAVSVN, "deleted-path"}
	enReplacedPath = Name{NS_DAVSVN, "replaced-path"}
	enModifiedPath = Name{NS_DAVSVN, "modified-path"}
	enProperty     = Name{NS_DAVSVN, "property"}
	enRevprop      = Name{NS_DAVSVN, "revprop"}
	enSetProp      = Name{NS_DAVSVN, "set-prop"}
	enDeleteProp   = Name{NS_DAVSVN, "delete-prop"}
	enItem         = Name{NS_DAVSVN, "item"}
	enLogItem      = Name{NS_DAVSVN, "log-item"}
	enChangedpaths = Name{NS_DAVSVN, "changed-paths"}
	enRevprops     = Name{NS_DAVSVN, "revprops"}

	// svn:* property namespace
	enSvnLog      = Name{NS_SVN, "log"}
	enSvnAuthor   = Name{NS_SVN, "author"}
	enSvnDate     = Name{NS_SVN, "date"}
	enSvnRevision = Name{NS_SVN, "revision"}
	enSvnProp     = Name{NS_SVN, "prop"}
	enSvnMimeType = Name{NS_SVN, "mime-type"}
	enSvnEntry    = Name{NS_SVN, "entry"}
	enSvnKind     = Name{NS_SVN, "kind"}
	enSvnNodeKind = Name{NS_SVN, "node-kind"}
	enSvnPath     = Name{NS_SVN, "path"}
	enSvnCopyFrom = Name{NS_SVN, "copy-from"}
	enSvnSize     = Name{NS_SVN, "size"}
	enSvnHidden   = Name{NS_SVN, "hidden"}
)

// el renders a start element to a strings.Builder with indentation.
func el(w *strings.Builder, indent int, name Name, attrs ...string) {
	writeIndent(w, indent)
	w.WriteString("<")
	writeName(w, name)
	for i := 0; i+1 < len(attrs); i += 2 {
		fmt.Fprintf(w, " %s=%q", attrs[i], attrs[i+1])
	}
	w.WriteString(">")
	w.WriteString("\n")
}

func elEnd(w *strings.Builder, indent int, name Name) {
	writeIndent(w, indent)
	w.WriteString("</")
	writeName(w, name)
	w.WriteString(">\n")
}

func elText(w *strings.Builder, indent int, name Name, text string) {
	writeIndent(w, indent)
	w.WriteString("<")
	writeName(w, name)
	w.WriteString(">")
	xml.EscapeText(w, []byte(text))
	w.WriteString("</")
	writeName(w, name)
	w.WriteString(">\n")
}

func writeIndent(w *strings.Builder, n int) {
	for i := 0; i < n; i++ {
		w.WriteString("  ")
	}
}

// writeName emits a prefix for a namespace. We use D: for DAV, S: for the svn
// protocol namespace, and the bare svn: prefix for svn: properties. To keep the
// documents small we use fixed prefixes and declare them on the root element.
func writeName(w *strings.Builder, n Name) {
	switch n.NS {
	case NS_DAV:
		w.WriteString("D:" + n.Local)
	case NS_DAVSVN:
		w.WriteString("S:" + n.Local)
	case NS_SVN:
		w.WriteString("svn:" + n.Local)
	case NS_CSVN:
		w.WriteString("c:" + n.Local)
	case "":
		w.WriteString(n.Local)
	default:
		w.WriteString(n.Local)
	}
}

// rootAttr returns the namespace declarations for the root element.
func rootAttr() string {
	return `xmlns:D="` + NS_DAV + `" xmlns:S="` + NS_DAVSVN + `" xmlns:svn="` + NS_SVN + `"`
}
