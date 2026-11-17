package fs

import (
	"context"
	"errors"
	"strings"
	"testing"

	"svngod/internal/fs/store"
)

// The format marker is the one object that tells a server how to read
// everything else, so both failure modes below are silent corruption rather
// than a crash:
//
//   - A future format version opened by an older binary reads fields it does not
//     know about and interprets the rest under old rules.
//   - Unknown keys dropped on rewrite. Any path that reads the format and writes
//     it back (migrate, switchFormat) would erase them, so a repository touched
//     once by an older build loses what a newer build wrote, with no error.
//
// Both are refused/survivable now: OpenRepo rejects a version it cannot honour,
// and Marshal re-emits keys it did not understand.

// putFormatBytes overwrites the format marker with raw bytes.
func putFormatBytes(t *testing.T, st store.Store, prefix string, b []byte) {
	t.Helper()
	if err := st.Put(context.Background(), store.NewKeys(prefix).Format(), b); err != nil {
		t.Fatal(err)
	}
}

// formatBytes reads the raw format marker.
func formatBytes(t *testing.T, st store.Store, prefix string) string {
	t.Helper()
	b, err := st.Get(context.Background(), store.NewKeys(prefix).Format(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestOpenRepoRefusesFutureFormatVersion fails on pre-fix code: a repository
// written by a newer build opens with err == nil and is then read under rules it
// does not follow.
func TestOpenRepoRefusesFutureFormatVersion(t *testing.T) {
	st := store.NewMemoryStore()
	if _, err := CreateRepo(st, "future", 0, 0); err != nil {
		t.Fatal(err)
	}
	raw := formatBytes(t, st, "future")
	bumped := strings.Replace(raw, "FORMAT=1", "FORMAT=99", 1)
	if bumped == raw {
		t.Fatalf("fixture did not contain FORMAT=1: %q", raw)
	}
	putFormatBytes(t, st, "future", []byte(bumped))

	if _, err := OpenRepo(st, "future"); err == nil {
		t.Fatal("opened a repository whose format version this build does not implement")
	} else if !errors.Is(err, ErrFormatTooNew) {
		t.Fatalf("error should identify the unsupported version, got: %v", err)
	}
}

// TestOpenRepoAcceptsCurrentFormatVersion is the control: the guard must not
// reject the version this build writes.
func TestOpenRepoAcceptsCurrentFormatVersion(t *testing.T) {
	st := store.NewMemoryStore()
	if _, err := CreateRepo(st, "same", 0, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenRepo(st, "same"); err != nil {
		t.Fatalf("refused a repository at the current format version: %v", err)
	}
}

// TestFormatRewriteKeepsUnknownKeys fails on pre-fix code: Marshal rebuilt the
// object from known struct fields only, so an unknown key vanished on the next
// write.
func TestFormatRewriteKeepsUnknownKeys(t *testing.T) {
	st := store.NewMemoryStore()
	if _, err := CreateRepo(st, "keep", 0, 0); err != nil {
		t.Fatal(err)
	}
	const future = "FUTURE_CRITICAL=1"
	raw := formatBytes(t, st, "keep")
	putFormatBytes(t, st, "keep", []byte(raw+future+"\n"))

	// Any read-then-write cycle stands in for migrate/switchFormat.
	if _, err := UpdateFormat(st, "keep", func(f *Format) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if got := formatBytes(t, st, "keep"); !strings.Contains(got, future) {
		t.Fatalf("rewriting the format dropped a key this build does not understand:\n%s", got)
	}
}

// TestFormatRewriteKeepsUnknownKeysThroughMigration is the same contract via the
// path that actually rewrites the marker in production.
func TestFormatRewriteKeepsUnknownKeysThroughMigration(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepo(st, "keepmig", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	const future = "FUTURE_CRITICAL=1"
	raw := formatBytes(t, st, "keepmig")
	putFormatBytes(t, st, "keepmig", []byte(raw+future+"\n"))

	// Re-open so the repository reflects the edited marker, then flip a format
	// switch exactly as `svnadmin migrate --dir-buckets on` does.
	if _, err := OpenRepo(st, "keepmig"); err != nil {
		t.Fatal(err)
	}
	if _, err := UpdateFormat(st, "keepmig", func(f *Format) error {
		f.DirBuckets = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	got := formatBytes(t, st, "keepmig")
	if !strings.Contains(got, future) {
		t.Fatalf("migration dropped an unknown format key:\n%s", got)
	}
	if !strings.Contains(got, "DIR_BUCKETS=1") {
		t.Fatalf("migration did not apply the requested change:\n%s", got)
	}
	_ = r
}

// TestFormatUnknownKeysAreReported makes the preserved keys visible instead of
// merely keeping them: an operator needs to see that a repository carries
// settings this build cannot act on.
func TestFormatUnknownKeysAreReported(t *testing.T) {
	raw := "SVNGOBJ1\nFORMAT=1\nSHARD_SIZE=4096\nMAX_DELTA_CHAIN=64\nFUTURE_CRITICAL=1\n"
	f, err := UnmarshalFormat([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Extra) == 0 {
		t.Fatal("unknown keys were dropped instead of recorded")
	}
	joined := strings.Join(f.Extra, ";")
	if !strings.Contains(joined, "FUTURE_CRITICAL=1") {
		t.Fatalf("unknown key not reported, got %v", f.Extra)
	}
}

// TestFormatVersionZeroRejected closes an adjacent hole: Version 0 is not a
// format this build ever wrote, so treating it as valid would mean silently
// applying current rules to an unrecognised repository.
func TestFormatVersionZeroRejected(t *testing.T) {
	if _, err := UnmarshalFormat([]byte("SVNGOBJ1\nFORMAT=0\nSHARD_SIZE=4096\nMAX_DELTA_CHAIN=64\n")); err == nil {
		t.Fatal("accepted FORMAT=0")
	}
}

// TestFormatUnknownNonNumericKeySurvives covers a future field that is not even
// a number. Parsing before the known-key check would make such a marker
// unreadable here, and refusing to read is not enough: the key must also come
// back intact on rewrite.
func TestFormatUnknownNonNumericKeySurvives(t *testing.T) {
	raw := "SVNGOBJ1\nFORMAT=1\nSHARD_SIZE=4096\nMAX_DELTA_CHAIN=64\nCOMPRESS=zstd\n"
	f, err := UnmarshalFormat([]byte(raw))
	if err != nil {
		t.Fatalf("a non-numeric unknown key made the marker unreadable: %v", err)
	}
	if got := string(f.Marshal()); !strings.Contains(got, "COMPRESS=zstd") {
		t.Fatalf("non-numeric unknown key was dropped on rewrite:\n%s", got)
	}
}

// TestMigrateRefusesFutureFormatVersion covers the rewrite paths that do not go
// through OpenRepo. Guarding only OpenRepo would leave `svnadmin migrate` free
// to reinterpret and overwrite a marker written by a newer build.
func TestMigrateRefusesFutureFormatVersion(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepo(st, "futmig", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	raw := formatBytes(t, st, "futmig")
	putFormatBytes(t, st, "futmig", []byte(strings.Replace(raw, "FORMAT=1", "FORMAT=99", 1)))

	if _, err := UpdateFormat(st, "futmig", func(f *Format) error {
		f.DirBuckets = true
		return nil
	}); !errors.Is(err, ErrFormatTooNew) {
		t.Fatalf("UpdateFormat accepted FORMAT=99, err=%v", err)
	}
	// And it must not have written anything on the way out.
	if got := formatBytes(t, st, "futmig"); !strings.Contains(got, "FORMAT=99") ||
		strings.Contains(got, "DIR_BUCKETS=1") {
		t.Fatalf("refused update still rewrote the marker:\n%s", got)
	}
	_ = r
}
