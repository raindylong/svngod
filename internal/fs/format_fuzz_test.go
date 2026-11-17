package fs

import (
	"strings"
	"testing"
)

// UnmarshalFormat parses the one object that decides how everything else is
// read. It is fed bytes from the object store rather than the network, but the
// store is the thing we are protecting: a format marker that parses into a
// half-initialised Format would make every later read wrong.
//
// Two properties matter and are easy to lose:
//   - Marshal/Unmarshal must round-trip: whatever we read back out of a marker
//     and write again must mean the same thing (this is what Format.Extra
//     exists for).
//   - Malformed input must produce an error, never a partially valid Format.
//     Accepting "FORMAT=1//garbage" silently would let a corrupt marker look
//     like a healthy repository.

// FuzzUnmarshalFormat checks for panics and for silent acceptance of garbage.
func FuzzUnmarshalFormat(f *testing.F) {
	seeds := []string{
		"SVNGOBJ1\nFORMAT=1\nSHARD_SIZE=4096\nMAX_DELTA_CHAIN=64\n",
		"SVNGOBJ1\nFORMAT=1\n",
		"SVNGOBJ1\n",
		"",
		"SVNGOBJ1\nFORMAT=0\n",
		"SVNGOBJ1\nFORMAT=99\n",
		"SVNGOBJ1\nFORMAT=1\nSHARD_SIZE=0\n",
		"SVNGOBJ1\nFORMAT=abc\n",
		"SVNGOBJ1\nFORMAT=1\nNOEQUALSIGN\n",
		"SVNGOBJ1\nFORMAT=1\nCOMPRESS=zstd\n",
		"SVNGOBJ1\nFORMAT=1\nDIR_BUCKETS=1\nSEGMENTS=1\nSEGMENT_FROM=1\n",
		"SVNGOBJ2\nFORMAT=1\n",
		"SVNGOBJ1\r\nFORMAT=1\r\n",
		"SVNGOBJ1\nFORMAT=1\n" + strings.Repeat("X=1\n", 100),
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, b []byte) {
		fm, err := UnmarshalFormat(b)
		if err != nil {
			return
		}
		if fm == nil {
			t.Fatal("nil Format with nil error")
		}
		// A version this build cannot honour must be refused, not returned:
		// OpenRepo relies on the check, but the parser is also called directly
		// by svnadmin info and migrate.
		if fm.Version > formatVersion {
			// UnmarshalFormat itself only rejects version 0; checkFormatVersion
			// is the gate for newer. Returning it is correct here as long as the
			// caller checks - so assert the gate refuses it.
			if err := checkFormatVersion(fm); err == nil {
				t.Fatalf("checkFormatVersion accepted FORMAT=%d", fm.Version)
			}
		}
		// Round trip: re-encoding and re-parsing must be idempotent. A field
		// lost on the second parse is data loss on the next rewrite.
		re := fm.Marshal()
		fm2, err := UnmarshalFormat(re)
		if err != nil {
			t.Fatalf("re-parsing our own output failed: %v\ninput: %q\noutput: %q", err, b, re)
		}
		if fm2.Version != fm.Version || fm2.ShardSize != fm.ShardSize ||
			fm2.MaxDeltaChain != fm.MaxDeltaChain || fm2.DirBuckets != fm.DirBuckets ||
			fm2.Segments != fm.Segments || fm2.SegmentFrom != fm.SegmentFrom ||
			fm2.SegmentTarget != fm.SegmentTarget {
			t.Fatalf("round trip changed the format:\n first: %+v\nsecond: %+v", fm, fm2)
		}
		if strings.Join(fm2.Extra, "|") != strings.Join(fm.Extra, "|") {
			t.Fatalf("unknown keys changed across a round trip: %v -> %v", fm.Extra, fm2.Extra)
		}
	})
}

// FuzzUnmarshalFormatKeepsUnknownKeys is the property the Extra field exists
// for, fuzzed separately so a regression points at the right thing: an unknown
// key must survive parse -> marshal -> parse.
func FuzzUnmarshalFormatKeepsUnknownKeys(f *testing.F) {
	f.Add("FUTURE_A", "1")
	f.Add("COMPRESS", "zstd")
	f.Add("SEGMENT_TARGET", "999")
	f.Add("", "x")

	f.Fuzz(func(t *testing.T, key, val string) {
		// Build a marker with one extra key. Reject inputs that cannot be a key
		// (the format is line-oriented "KEY=VALUE"), so the test stays about the
		// preservation logic rather than about splitting.
		if key == "" || strings.ContainsAny(key, "=\n\r") || strings.ContainsAny(val, "\n\r") {
			t.Skip()
		}
		if knownFormatKey(key) {
			t.Skip() // not an unknown key; other tests cover those
		}
		raw := "SVNGOBJ1\nFORMAT=1\nSHARD_SIZE=4096\nMAX_DELTA_CHAIN=64\n" + key + "=" + val + "\n"
		fm, err := UnmarshalFormat([]byte(raw))
		if err != nil {
			t.Fatalf("a valid marker with an unknown key was rejected: %v", err)
		}
		if len(fm.Extra) != 1 || fm.Extra[0] != key+"="+val {
			t.Fatalf("unknown key not preserved verbatim: got %v, want %q", fm.Extra, key+"="+val)
		}
		if !strings.Contains(string(fm.Marshal()), key+"="+val) {
			t.Fatal("Marshal dropped the unknown key")
		}
	})
}

// TestFormatSegmentFromImpliesSegments is the regression fuzzing found: a marker
// carrying SEGMENT_FROM without SEGMENTS parsed as Segments=false, but Marshal
// emits SEGMENTS=1 whenever SegmentFrom is set, so the value changed identity
// across a rewrite. That silently switches a repository into segment mode on
// the next migrate.
func TestFormatSegmentFromImpliesSegments(t *testing.T) {
	raw := "SVNGOBJ1\nFORMAT=1\nSHARD_SIZE=4096\nMAX_DELTA_CHAIN=64\nSEGMENT_FROM=1\n"
	fm, err := UnmarshalFormat([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if !fm.Segments {
		t.Fatal("SEGMENT_FROM=1 parsed as Segments=false; Marshal would then write SEGMENTS=1")
	}
	// The round trip must be idempotent.
	fm2, err := UnmarshalFormat(fm.Marshal())
	if err != nil {
		t.Fatal(err)
	}
	if fm2.Segments != fm.Segments || fm2.SegmentFrom != fm.SegmentFrom {
		t.Fatalf("round trip changed segment flags: %+v -> %+v", fm, fm2)
	}
}
