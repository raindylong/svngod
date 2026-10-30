package server

import "testing"

func TestMergeinfoParseFormat(t *testing.T) {
	in := "/trunk:1-3,7\n/branches/x:4-6*"
	m := parseMergeinfo(in)
	// Paths are serialised in sorted order.
	if got, want := formatMergeinfo(m), "/branches/x:4-6*\n/trunk:1-3,7"; got != want {
		t.Fatalf("round trip = %q, want %q", got, want)
	}
}

func TestMergeinfoTranslateAndInherit(t *testing.T) {
	anc := parseMergeinfo("/trunk:1-3,5*\n/branches/b:2")
	got := formatMergeinfo(translateMergeinfo(inheritableOnly(anc), "sub"))
	want := "/branches/b/sub:2\n/trunk/sub:1-3"
	if got != want {
		t.Fatalf("translated = %q, want %q", got, want)
	}
}

func TestMergeinfoUnion(t *testing.T) {
	a := parseMergeinfo("/trunk:1-4")
	b := parseMergeinfo("/trunk:3-6\n/x:1")
	got := formatMergeinfo(unionMergeinfo(a, b))
	want := "/trunk:1-6\n/x:1"
	if got != want {
		t.Fatalf("union = %q, want %q", got, want)
	}
}
