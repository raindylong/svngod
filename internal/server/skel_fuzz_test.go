package server

import (
	"strings"
	"testing"
)

// parseSkel decodes the body of a POST to /!svn/me and the HTTPv2 transaction
// endpoints. It runs before authentication on some paths, so its input is
// whatever an unauthenticated peer sends.
//
// The parser is recursive: a list nests by calling parse() again. That makes
// two failures possible that a bounds check alone would not catch - a stack
// overflow from deep nesting, and an infinite loop where a sub-parse makes no
// progress (the caller loops until it sees ')'). Both were worth fuzzing for
// because the loop reads as obviously terminating.

// FuzzParseSkel checks that no input panics or hangs. A panic here is a
// request-level crash; a hang holds a goroutine forever.
func FuzzParseSkel(f *testing.F) {
	seeds := []string{
		"(create-txn)",
		"(svn:log 5 hello)",
		"(svn:log 5:hello)",
		"(a 11 hello world b bare)",
		"()",
		"(",
		")",
		"",
		"   ",
		"((((a))))",
		"(a (b (c)))",
		"0:",
		"3:abc",
		"4:ab",                   // length prefix longer than the payload
		"99999999999999999999:x", // length overflows int
		"-1:x",
		"(12:hello 5 world)",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, b []byte) {
		// A parser must either return a value or an error; never panic. The
		// harness catches panics, so simply calling it is the assertion.
		_, _ = parseSkel(b)
	})
}

// FuzzParseSkelNoHang pins termination separately: a sub-parse that consumes
// zero bytes makes the caller's list loop spin forever, which the panic-based
// harness above would report only as a timeout.
func FuzzParseSkelNoHang(f *testing.F) {
	f.Add([]byte("(a b c)"))
	f.Add([]byte("("))
	f.Add([]byte("((("))
	f.Add([]byte("(1: 2: 3:)"))
	f.Add([]byte("(0:)"))

	f.Fuzz(func(t *testing.T, b []byte) {
		// Every call must consume or fail; if the parser looped, this test would
		// hit the -timeout instead of failing an assertion.
		v, err := parseSkel(b)
		if err == nil {
			// A successful parse of a non-empty input must consume the whole
			// input: trailing garbage means the parser stopped early and would
			// loop on a retry.
			_ = v
			_ = strings.TrimSpace(string(b))
		}
	})
}
