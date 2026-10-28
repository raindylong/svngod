package authz

import (
	"strings"
)

// userGroups resolves the transitive set of groups a user belongs to,
// following @group member references. If user is in G and H lists @G, user is
// in H too.
func (c *Config) userGroups(username string) map[string]bool {
	out := map[string]bool{}
	for g, members := range c.Groups {
		for _, m := range members {
			if m == username {
				out[g] = true
			}
		}
	}
	// expand to fixpoint: H is added when any member of H is "@G" and G ∈ out.
	for changed := true; changed; {
		changed = false
		for g, members := range c.Groups {
			if out[g] {
				continue
			}
			for _, m := range members {
				if strings.HasPrefix(m, "@") && out[strings.TrimPrefix(m, "@")] {
					out[g] = true
					changed = true
					break
				}
			}
		}
	}
	return out
}

// bestSection finds the most specific matching section: exact repo (or all
// repos) and the longest path prefix that covers the requested path.
func bestSection(c *Config, repo, path string) *Section {
	path = normalizePath(path)
	var best *Section
	bestLen := -1
	for i := range c.Sections {
		s := &c.Sections[i]
		if s.Repo != "" && s.Repo != "*" && s.Repo != repo {
			continue
		}
		// path must equal s.Path or be under it (s.Path "/" matches all).
		if s.Path != "/" && path != s.Path && !strings.HasPrefix(path, s.Path+"/") {
			continue
		}
		score := len(s.Path)
		if s.Repo == repo {
			score += 1000 // specific repo beats all-repos on tie
		}
		if score > bestLen {
			bestLen = score
			best = s
		}
	}
	return best
}

// subjectMatches reports whether a rule subject matches the (user, groups,
// isAuthenticated) context. A leading '~' negates the match.
func subjectMatches(subject, username string, groups map[string]bool, authenticated bool) bool {
	negate := false
	if strings.HasPrefix(subject, "~") {
		negate = true
		subject = subject[1:]
	}
	var m bool
	switch {
	case subject == "*":
		m = true // everyone, including anonymous
	case subject == "$anonymous":
		m = !authenticated
	case subject == "$authenticated":
		m = authenticated
	case strings.HasPrefix(subject, "@"):
		m = groups[strings.TrimPrefix(subject, "@")]
	default:
		m = subject == username
	}
	if negate {
		return !m
	}
	return m
}

// Evaluate returns the effective access for (username, repo, path). username
// may be empty to represent an anonymous request. write requests are checked
// against Write; read against Read.
func (c *Config) Evaluate(username, repo, path string) Access {
	if c == nil {
		return NoAccess
	}
	groups := c.userGroups(username)
	authenticated := username != ""
	sec := bestSection(c, repo, path)
	if sec == nil {
		return NoAccess
	}
	// Effective access is the maximum over every matching rule. Verified
	// against real svnserve 1.14: a later, weaker rule does NOT override an
	// earlier stronger one (e.g. "alice = rw" then "alice = r" still grants
	// rw), so this is max-of-matches, not last-match-wins. The most-specific
	// path section still wins across sections (bestSection above).
	var acc Access = NoAccess
	for _, r := range sec.Rules {
		if subjectMatches(r.Subject, username, groups, authenticated) {
			if r.Access > acc {
				acc = r.Access
			}
		}
	}
	return acc
}

// Authorize returns true if the request is allowed.
func (c *Config) Authorize(username, repo, path string, write bool) bool {
	return c.Evaluate(username, repo, path).Allows(write)
}
