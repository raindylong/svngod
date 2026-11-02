package main

import (
	"errors"
	"testing"
)

// TestSvnTransportFailure: only connectivity failures stop the parent-URL
// walk in resolveRepoRoot; "path not found" must keep walking.
func TestSvnTransportFailure(t *testing.T) {
	cases := []struct {
		msg  string
		want bool
	}{
		{"svn info: exit status 1: svn: E170013: Unable to connect to a repository at URL", true},
		{"svn info: timed out after 20s", true},
		{"svn: E000111: Error running context: Connection refused", true},
		{"svn: E200009: Could not display info for '/x': not a working copy", false},
		{"svn: E170000: URL 'http://host/nope' doesn't exist", false},
	}
	for _, c := range cases {
		if got := svnTransportFailure(errors.New(c.msg)); got != c.want {
			t.Errorf("svnTransportFailure(%q) = %v, want %v", c.msg, got, c.want)
		}
	}
}

func TestRepoPathOf(t *testing.T) {
	cases := []struct{ target, root, want string }{
		{"http://h/svn/r", "http://h/svn/r", "/"},
		{"http://h/svn/r/trunk/a", "http://h/svn/r", "/trunk/a"},
		{"http://h/svn/r/trunk/", "http://h/svn/r/", "/trunk"},
	}
	for _, c := range cases {
		if got := repoPathOf(c.target, c.root); got != c.want {
			t.Errorf("repoPathOf(%q, %q) = %q, want %q", c.target, c.root, got, c.want)
		}
	}
}
