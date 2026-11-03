package server

import (
	"strings"
	"testing"
)

func TestParsePropertyUpdateCustomProp(t *testing.T) {
	// subversion sends user properties as <C:custom:NAME>; the local name
	// contains a colon, which encoding/xml rejects.
	body := []byte(`<?xml version="1.0" encoding="utf-8"?>` +
		`<D:propertyupdate xmlns:D="DAV:" xmlns:V="http://subversion.tigris.org/xmlns/dav/" ` +
		`xmlns:C="http://subversion.tigris.org/xmlns/custom/" xmlns:S="http://subversion.tigris.org/xmlns/svn/">` +
		`<D:set><D:prop><C:custom:myprop>hello &amp; bye</C:custom:myprop>` +
		`<S:eol-style>native</S:eol-style></D:prop></D:set></D:propertyupdate>`)
	sets, removes := parsePropertyUpdate(body)
	if len(sets) != 2 || len(removes) != 0 {
		t.Fatalf("sets=%d removes=%d, want 2/0", len(sets), len(removes))
	}
	got := map[string]string{}
	for _, p := range sets {
		got[svnPropertyKey(p.ns, p.local)] = p.value
	}
	if got["myprop"] != "hello & bye" {
		t.Errorf("user prop = %q, want %q", got["myprop"], "hello & bye")
	}
	if got["svn:eol-style"] != "native" {
		t.Errorf("svn prop = %q", got["svn:eol-style"])
	}
}

func TestParsePropertyUpdateRemove(t *testing.T) {
	body := []byte(`<D:propertyupdate xmlns:D="DAV:" ` +
		`xmlns:C="http://subversion.tigris.org/xmlns/custom/">` +
		`<D:remove><D:prop><C:custom:old/></D:prop></D:remove></D:propertyupdate>`)
	sets, removes := parsePropertyUpdate(body)
	if len(sets) != 0 || len(removes) != 1 {
		t.Fatalf("sets=%d removes=%d, want 0/1", len(sets), len(removes))
	}
	if key := svnPropertyKey(removes[0].ns, removes[0].local); key != "old" {
		t.Fatalf("remove key = %q, want old", key)
	}
}

func TestParsePropfindRequestCustomProps(t *testing.T) {
	body := []byte(`<D:propfind xmlns:D="DAV:" ` +
		`xmlns:C="http://subversion.tigris.org/xmlns/custom/">` +
		`<D:prop><C:custom:myprop/><D:getcontentlength/></D:prop></D:propfind>`)
	props, propnames := parsePropfindRequest(body)
	if propnames {
		t.Fatal("unexpected propnames")
	}
	keys := map[string]bool{}
	for _, p := range props {
		keys[svnPropertyKey(p.ns, p.local)] = true
	}
	if !keys["myprop"] || !keys["getcontentlength"] {
		t.Fatalf("parsed props = %v", keys)
	}
}

func TestUserPropSerializationName(t *testing.T) {
	if got := davPropElement("myprop", "v"); !strings.Contains(got, "<custom:myprop ") ||
		!strings.Contains(got, "</custom:myprop>") {
		t.Fatalf("plain user prop name not preserved: %s", got)
	}
	if got := davPropElement("bugzilla:url", "v"); !strings.Contains(got, "<custom:bugzilla:url ") {
		t.Fatalf("colon name not preserved: %s", got)
	}
	if got := davPropNameTag("myprop"); !strings.Contains(got, "<custom:myprop ") {
		t.Fatalf("propname tag not preserved: %s", got)
	}
}

func TestParsePropertyUpdateRemoveWithAttributes(t *testing.T) {
	// svn 1.14 sends removals as <C:name V:absent="1"></C:name>.
	body := []byte(`<?xml version="1.0" encoding="utf-8"?>` +
		`<D:propertyupdate xmlns:D="DAV:" xmlns:V="http://subversion.tigris.org/xmlns/dav/" ` +
		`xmlns:C="http://subversion.tigris.org/xmlns/custom/">` +
		`<D:remove><D:prop><C:delme V:absent="1"></C:delme></D:prop></D:remove></D:propertyupdate>`)
	sets, removes := parsePropertyUpdate(body)
	if len(sets) != 0 || len(removes) != 1 || removes[0].local != "delme" {
		t.Fatalf("sets=%+v removes=%+v; want one remove of delme", sets, removes)
	}
	if removes[0].ns != NS_CSVN {
		t.Fatalf("remove namespace = %q", removes[0].ns)
	}
}
