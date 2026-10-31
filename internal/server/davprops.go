package server

import (
	"bytes"
	"encoding/xml"
	"strings"
)

// DAV property bodies need a lenient parser: subversion encodes user
// properties as <C:custom:NAME> where the local name itself contains a colon.
// encoding/xml rejects that ("expected element name after <"), which silently
// dropped every user property on commit. The scanner below only understands
// the shallow structure of propertyupdate/propfind bodies, but it accepts
// arbitrary QNames by splitting on the first colon.

// davProp is one property in a PROPPATCH/PROPFIND body.
type davProp struct {
	ns    string // namespace URI
	local string // local name (may itself contain "custom:")
	value string
}

// svnPropertyKey converts a DAV property into the key used by fs node props:
// "svn:name" for the svn namespace, the bare name for user properties, and
// the local name otherwise.
func svnPropertyKey(ns, local string) string {
	switch ns {
	case NS_SVN:
		return "svn:" + local
	case NS_CSVN:
		return strings.TrimPrefix(local, "custom:")
	}
	return local
}

// collectNamespaces records xmlns / xmlns:prefix bindings anywhere in body.
func collectNamespaces(body []byte) map[string]string {
	ns := map[string]string{
		"D": NS_DAV, "S": NS_SVN, "C": NS_CSVN, "V": NS_DAVSVN,
	}
	pos := 0
	for {
		tag, _, next, ok := nextTag(body, pos)
		if !ok {
			break
		}
		pos = next
		if strings.HasPrefix(tag, "/") || strings.HasPrefix(tag, "?") || strings.HasPrefix(tag, "!") {
			continue
		}
		selfClose := strings.HasSuffix(tag, "/")
		if selfClose {
			tag = strings.TrimSuffix(tag, "/")
		}
		for _, a := range splitAttrs(tag) {
			if a.name == "xmlns" {
				ns[""] = a.val
				continue
			}
			if strings.HasPrefix(a.name, "xmlns:") {
				ns[strings.TrimPrefix(a.name, "xmlns:")] = a.val
			}
		}
	}
	return ns
}

type attr struct{ name, val string }

// splitAttrs parses the attributes of a raw tag body (name already included).
// It skips the element name and handles quoted values containing spaces.
func splitAttrs(tag string) []attr {
	// drop the element name
	i := strings.IndexAny(tag, " \t\r\n")
	if i < 0 {
		return nil
	}
	rest := tag[i:]
	var out []attr
	for len(rest) > 0 {
		rest = strings.TrimLeft(rest, " \t\r\n")
		if rest == "" {
			break
		}
		eq := strings.IndexByte(rest, '=')
		if eq < 0 {
			break
		}
		name := strings.TrimSpace(rest[:eq])
		rest = strings.TrimSpace(rest[eq+1:])
		if rest == "" {
			break
		}
		var val string
		if rest[0] == '"' || rest[0] == '\'' {
			q := rest[0]
			end := strings.IndexByte(rest[1:], q)
			if end < 0 {
				val = rest[1:]
				rest = ""
			} else {
				val = rest[1 : 1+end]
				rest = rest[2+end:]
			}
		} else {
			sp := strings.IndexAny(rest, " \t\r\n")
			if sp < 0 {
				val, rest = rest, ""
			} else {
				val, rest = rest[:sp], rest[sp:]
			}
		}
		out = append(out, attr{name: name, val: xmlUnescape(val)})
	}
	return out
}

// nextTag finds the next markup tag starting at pos. It returns the tag body
// (without angle brackets), the content offsets, the position after the tag,
// and whether a tag was found. Comments/CDATA/processing instructions are
// returned as opaque tags starting with "!" or "?".
func nextTag(body []byte, pos int) (tag string, start, next int, ok bool) {
	for pos < len(body) {
		i := bytes.IndexByte(body[pos:], '<')
		if i < 0 {
			return "", 0, len(body), false
		}
		i += pos
		if bytes.HasPrefix(body[i:], []byte("<!--")) {
			j := bytes.Index(body[i+4:], []byte("-->"))
			if j < 0 {
				return "", i, len(body), false
			}
			return "!--", i, i + 4 + j + 3, true
		}
		if bytes.HasPrefix(body[i:], []byte("<![CDATA[")) {
			return "!CDATA", i, i + 9, true
		}
		// find the closing '>' honouring quoted attribute values
		j := i + 1
		var quote byte
		for j < len(body) {
			c := body[j]
			if quote != 0 {
				if c == quote {
					quote = 0
				}
			} else if c == '"' || c == '\'' {
				quote = c
			} else if c == '>' {
				return string(body[i+1 : j]), i, j + 1, true
			}
			j++
		}
		return "", i, len(body), false
	}
	return "", len(body), len(body), false
}

// splitQName splits "C:custom:k" into ("C", "custom:k").
func splitQName(tag string) (prefix, local string) {
	if i := strings.IndexByte(tag, ':'); i >= 0 {
		return tag[:i], tag[i+1:]
	}
	return "", tag
}

// parsePropertyUpdate parses a PROPPATCH body into set/remove property lists.
func parsePropertyUpdate(body []byte) (sets, removes []davProp) {
	nsmap := collectNamespaces(body)
	var mode string // "set" or "remove"
	inProp := false
	var pending *davProp
	textStart := 0
	pos := 0
	for {
		tag, start, next, ok := nextTag(body, pos)
		if !ok {
			break
		}
		if pending != nil {
			pending.value += string(body[textStart:start])
		}
		pos = next
		switch {
		case strings.HasPrefix(tag, "?") || strings.HasPrefix(tag, "!"):
			textStart = pos
			continue
		case strings.HasPrefix(tag, "/"):
			name := strings.TrimSpace(strings.TrimPrefix(tag, "/"))
			prefix, local := splitQName(name)
			_ = prefix
			if pending != nil && local == pending.local {
				p := *pending
				p.value = strings.TrimSpace(xmlUnescape(p.value))
				if mode == "remove" {
					removes = append(removes, p)
				} else {
					sets = append(sets, p)
				}
				pending = nil
			} else if local == "prop" {
				inProp = false
			} else if local == "set" || local == "remove" {
				mode = ""
			}
		default:
			selfClose := strings.HasSuffix(tag, "/")
			if selfClose {
				tag = strings.TrimSuffix(tag, "/")
			}
			prefix, local := splitQName(strings.TrimSpace(tag))
			switch local {
			case "set":
				mode = "set"
			case "remove":
				mode = "remove"
			case "prop":
				inProp = true
			default:
				if inProp && mode != "" {
					nsURI := nsmap[prefix]
					if prefix == "" {
						nsURI = nsmap[""]
					}
					p := &davProp{ns: nsURI, local: local}
					if selfClose {
						if mode == "remove" {
							removes = append(removes, *p)
						} else {
							sets = append(sets, *p)
						}
					} else {
						pending = p
					}
				}
			}
		}
		textStart = pos
	}
	return sets, removes
}

// parsePropfindRequest inspects a PROPFIND body. It returns the requested
// property names and whether the request asked for propnames only. An empty
// body means allprop.
func parsePropfindRequest(body []byte) ([]davProp, bool) {
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, false
	}
	if bytes.Contains(body, []byte("propnames")) {
		return nil, true
	}
	nsmap := collectNamespaces(body)
	var out []davProp
	inProp := false
	pos := 0
	for {
		tag, _, next, ok := nextTag(body, pos)
		if !ok {
			break
		}
		pos = next
		if strings.HasPrefix(tag, "?") || strings.HasPrefix(tag, "!") || strings.HasPrefix(tag, "/") {
			continue
		}
		selfClose := strings.HasSuffix(tag, "/")
		if selfClose {
			tag = strings.TrimSuffix(tag, "/")
		}
		_, local := splitQName(strings.TrimSpace(tag))
		switch local {
		case "propfind":
			continue
		case "allprop", "propname":
			continue
		case "prop":
			inProp = true
		default:
			if inProp {
				// The tag body still carries its attributes; the element name
				// ends at the first whitespace. (An unprefixed property such as
				// <log xmlns="...svn/"/> otherwise parses as local name
				// `log xmlns=...`, so revprop reads silently miss.)
				name := tag
				if i := strings.IndexAny(name, " \t\r\n"); i >= 0 {
					name = name[:i]
				}
				prefix, local := splitQName(name)
				nsURI := nsmap[prefix]
				if prefix == "" && nsURI == "" {
					nsURI = nsmap[""]
				}
				// A default xmlns on the element itself overrides the document
				// default for that element.
				for _, a := range splitAttrs(tag) {
					if a.name == "xmlns" {
						nsURI = a.val
						break
					}
				}
				if prefix == "" && nsURI == "" {
					nsURI = nsmap[""]
				}
				out = append(out, davProp{ns: nsURI, local: local})
			}
		}
	}
	return out, false
}

// xmlUnescape decodes XML character/entity references in a text value.
func xmlUnescape(s string) string {
	if !strings.ContainsAny(s, "&") {
		return s
	}
	var v struct {
		Text string `xml:",chardata"`
	}
	if err := xml.Unmarshal([]byte("<v>"+s+"</v>"), &v); err != nil {
		return s
	}
	return v.Text
}

// davPropElement renders a property as a DAV element for a multistatus body:
// svn:* props use the svn namespace, user props use the custom namespace with
// the "custom:" name prefix that subversion expects.
func davPropElement(key, value string) string {
	if strings.HasPrefix(key, "svn:") {
		local := strings.TrimPrefix(key, "svn:")
		return "<" + local + ` xmlns="` + NS_SVN + `">` + xmlEscape(value) + "</" + local + ">"
	}
	return "<C:custom:" + key + ` xmlns:C="` + NS_CSVN + `">` + xmlEscape(value) + "</C:custom:" + key + ">"
}

// davPropNameTag renders just the property element name (for propnames).
func davPropNameTag(key string) string {
	if strings.HasPrefix(key, "svn:") {
		local := strings.TrimPrefix(key, "svn:")
		return "<" + local + ` xmlns="` + NS_SVN + `"/>`
	}
	return "<C:custom:" + key + ` xmlns:C="` + NS_CSVN + `"/>`
}
