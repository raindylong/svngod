// Package authz implements SVN-style path-based authorization from a local
// authz file: [groups] aliases, per-repository:path access sections, and
// rules keyed by user / @group / * / ~exclusion / $anonymous / $authenticated.
package authz

import (
	"bufio"
	"fmt"
	"strings"
)

// Access is the granted permission level.
type Access int

const (
	// NoAccess denies everything.
	NoAccess Access = iota
	// Read allows read operations.
	Read
	// Write allows read and write operations.
	Write
)

func (a Access) Allows(write bool) bool {
	if write {
		return a == Write
	}
	return a >= Read
}

// Rule is one subject→permission binding inside a section.
type Rule struct {
	Subject string
	Access  Access
}

// Section is an access control block for one repo:path (or all repos).
type Section struct {
	Repo  string // "" or "*" = all repositories
	Path  string // normalized absolute path, e.g. "/" or "/trunk"
	Rules []Rule
}

// Config is a parsed authz file.
type Config struct {
	Groups   map[string][]string
	Sections []Section
}

// Parse parses authz data. Lines starting with '#' or ';' are comments;
// blank lines are skipped. A group definition may continue on an indented
// next line while it ends with a comma.
func Parse(data []byte) (*Config, error) {
	cfg := &Config{Groups: map[string][]string{}}
	sc := bufio.NewScanner(strings.NewReader(string(data)))
	var section *Section
	var pendingGroup string // set when a group line ended with a comma
	inGroups := false

	flush := func() {
		if section != nil {
			cfg.Sections = append(cfg.Sections, *section)
			section = nil
		}
	}

	lineNo := 0
	for sc.Scan() {
		lineNo++
		raw := sc.Text()
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, ";") {
			continue
		}
		// indented continuation of the previous group line
		if pendingGroup != "" && (raw[0] == ' ' || raw[0] == '\t') {
			cfg.Groups[pendingGroup] = appendMembers(cfg.Groups[pendingGroup], trimmed)
			if !strings.HasSuffix(trimmed, ",") {
				pendingGroup = ""
			}
			continue
		}
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
			flush()
			name := strings.TrimSpace(trimmed[1 : len(trimmed)-1])
			if name == "groups" {
				inGroups = true
				section = nil
			} else {
				inGroups = false
				sec, err := parseSection(name)
				if err != nil {
					return nil, fmt.Errorf("authz:%d: %w", lineNo, err)
				}
				section = &sec
			}
			continue
		}
		// a rule or group member line
		if inGroups {
			flush()
			name, members, cont, err := parseGroupLine(trimmed)
			if err != nil {
				return nil, fmt.Errorf("authz:%d: %w", lineNo, err)
			}
			cfg.Groups[name] = appendMembers(cfg.Groups[name], members)
			if cont {
				pendingGroup = name
			}
			continue
		}
		if section == nil {
			return nil, fmt.Errorf("authz:%d: rule outside any section", lineNo)
		}
		rule, err := parseRule(trimmed)
		if err != nil {
			return nil, fmt.Errorf("authz:%d: %w", lineNo, err)
		}
		section.Rules = append(section.Rules, rule)
	}
	flush()
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func parseSection(name string) (Section, error) {
	var sec Section
	if i := strings.IndexByte(name, ':'); i >= 0 {
		sec.Repo = strings.TrimSpace(name[:i])
		sec.Path = name[i+1:]
	} else {
		sec.Path = name
	}
	if !strings.HasPrefix(sec.Path, "/") {
		return sec, fmt.Errorf("invalid section path %q", sec.Path)
	}
	sec.Path = normalizePath(sec.Path)
	return sec, nil
}

func parseGroupLine(line string) (name, members string, cont bool, err error) {
	i := strings.IndexByte(line, '=')
	if i < 0 {
		return "", "", false, fmt.Errorf("malformed group line %q", line)
	}
	name = strings.TrimSpace(line[:i])
	if name == "" {
		return "", "", false, fmt.Errorf("empty group name")
	}
	members = strings.TrimSpace(line[i+1:])
	return name, members, strings.HasSuffix(members, ","), nil
}

func appendMembers(dst []string, members string) []string {
	for _, m := range strings.Split(members, ",") {
		m = strings.TrimSpace(m)
		if m != "" {
			dst = append(dst, m)
		}
	}
	return dst
}

func parseRule(line string) (Rule, error) {
	i := strings.IndexByte(line, '=')
	if i < 0 {
		return Rule{}, fmt.Errorf("malformed rule %q", line)
	}
	subject := strings.TrimSpace(line[:i])
	perm := strings.TrimSpace(line[i+1:])
	var acc Access
	switch perm {
	case "r":
		acc = Read
	case "rw":
		acc = Write
	case "", "none":
		acc = NoAccess
	default:
		return Rule{}, fmt.Errorf("unknown permission %q", perm)
	}
	if subject == "" {
		return Rule{}, fmt.Errorf("empty subject")
	}
	return Rule{Subject: subject, Access: acc}, nil
}

func normalizePath(p string) string {
	if p == "" {
		return "/"
	}
	parts := strings.Split(p, "/")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}
		out = append(out, part)
	}
	if len(out) == 0 {
		return "/"
	}
	return "/" + strings.Join(out, "/")
}
