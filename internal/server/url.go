package server

import (
	"strconv"
	"strings"
)

// resourceType identifies which kind of DAV resource a request URL names.
type resourceType int

const (
	resRoot       resourceType = iota // /repo/        (the version-controlled collection root)
	resPublic                         // /repo/trunk/x  (a path in the repo as of HEAD)
	resVcc                            // /repo/!svn/vcc/default
	resBaselineColl                   // /repo/!svn/bc/<rev>/<path>
	resBaseline                       // /repo/!svn/bln/<rev>
	resActivity                       // /repo/!svn/act/<uuid>
	resMe                             // /repo/!svn/me  (REPORT endpoint)
	resTxn                            // /repo/!svn/txn/<uuid>
	resRev                            // /repo/!svn/rev/<rev>/<path>
	resVersion                        // /repo/!svn/ver/<rev>/<path>
	resWorking                        // /repo/!svn/wc/<uuid>/<path>
)

// resource is a parsed svngod request URL.
type resource struct {
	typ  resourceType
	repo string   // repository name ("" if not determined)
	rev  uint64   // revision for bc/bln/rev/ver
	path string   // absolute svn path ("/…", "" = root) for public/bc/rev/ver/wc
	uuid string   // activity / transaction / working-copy id
}

// parseResource splits a request path (already URL-decoded) into a resource.
// basePath is the URL prefix under which repos live (e.g. "/svn").
func parseResource(basePath, reqPath string) resource {
	p := reqPath
	if basePath != "" && basePath != "/" {
		basePath = strings.TrimSuffix(basePath, "/")
		if strings.HasPrefix(p, basePath) {
			p = strings.TrimPrefix(p, basePath)
		}
	}
	p = "/" + strings.Trim(p, "/") // normalise: "/repo/x"
	segs := splitPath(p)           // ["repo", ...]
	if len(segs) == 0 {
		return resource{typ: resRoot}
	}
	repo := segs[0]
	rest := segs[1:]
	// repo root: /repo or /repo/
	if len(rest) == 0 {
		return resource{typ: resRoot, repo: repo}
	}
	// public path: /repo/<something-not-!svn>
	if rest[0] != "!svn" {
		return resource{typ: resPublic, repo: repo, path: "/" + strings.Join(rest, "/")}
	}
	// private !svn URL
	priv := rest[1:]
	if len(priv) == 0 {
		return resource{typ: resRoot, repo: repo}
	}
	switch priv[0] {
	case "vcc":
		if len(priv) >= 2 && priv[1] == "default" {
			return resource{typ: resVcc, repo: repo}
		}
	case "me":
		return resource{typ: resMe, repo: repo}
	case "act":
		if len(priv) >= 2 {
			return resource{typ: resActivity, repo: repo, uuid: priv[1]}
		}
	case "txn":
		if len(priv) >= 2 {
			return resource{typ: resTxn, repo: repo, uuid: priv[1], path: "/" + strings.Join(priv[2:], "/")}
		}
	case "bc":
		if len(priv) >= 2 {
			rev, err := strconv.ParseUint(priv[1], 10, 64)
			if err == nil {
				return resource{typ: resBaselineColl, repo: repo, rev: rev, path: "/" + strings.Join(priv[2:], "/")}
			}
		}
	case "bln":
		if len(priv) >= 2 {
			rev, err := strconv.ParseUint(priv[1], 10, 64)
			if err == nil {
				return resource{typ: resBaseline, repo: repo, rev: rev}
			}
		}
	case "rev":
		if len(priv) >= 2 {
			rev, err := strconv.ParseUint(priv[1], 10, 64)
			if err == nil {
				return resource{typ: resRev, repo: repo, rev: rev, path: "/" + strings.Join(priv[2:], "/")}
			}
		}
	case "ver":
		if len(priv) >= 2 {
			rev, err := strconv.ParseUint(priv[1], 10, 64)
			if err == nil {
				return resource{typ: resVersion, repo: repo, rev: rev, path: "/" + strings.Join(priv[2:], "/")}
			}
		}
	case "wc":
		if len(priv) >= 2 {
			return resource{typ: resWorking, repo: repo, uuid: priv[1], path: "/" + strings.Join(priv[2:], "/")}
		}
	}
	return resource{typ: resRoot, repo: repo}
}

func splitPath(p string) []string {
	var out []string
	for _, s := range strings.Split(strings.Trim(p, "/"), "/") {
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}
