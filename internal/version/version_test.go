package version

import (
	"runtime/debug"
	"strings"
	"testing"
)

// Build metadata has to be useful to whoever is staring at a production box
// asking "what am I running". Two ways that can fail silently:
//
//   - Reporting a version that was never injected, so a locally built binary
//     claims to be a release and nobody can tell the two apart.
//   - Reporting nothing at all, which is the state this package was added to
//     fix (svngod and svnadmin had no --version whatsoever).
//
// These tests pin both: an injected value wins, an un-injected build says so
// plainly instead of inventing a number.

func TestInjectedVersionWins(t *testing.T) {
	// Save and restore the linker-set values: these are normally written by
	// -ldflags -X at build time.
	oldV, oldC, oldD := Version, Commit, BuildDate
	defer func() { Version, Commit, BuildDate = oldV, oldC, oldD }()

	Version = "1.2.3"
	Commit = "abc1234"
	BuildDate = "2026-11-17T00:00:00Z"

	got := Get()
	if got.Version != "1.2.3" {
		t.Fatalf("version = %q, want 1.2.3", got.Version)
	}
	if got.Commit != "abc1234" {
		t.Fatalf("commit = %q, want abc1234", got.Commit)
	}
	if got.BuildDate != "2026-11-17T00:00:00Z" {
		t.Fatalf("build date = %q", got.BuildDate)
	}
	// An injected build must not claim to be un-injected.
	if got.Unreleased {
		t.Fatal("an injected build reported itself as unreleased")
	}
}

func TestUninjectedBuildSaysSo(t *testing.T) {
	oldV, oldC, oldD := Version, Commit, BuildDate
	defer func() { Version, Commit, BuildDate = oldV, oldC, oldD }()

	Version, Commit, BuildDate = "", "", ""

	got := Get()
	if got.Version == "" {
		t.Fatal("version is empty: a binary must always report something")
	}
	// The point: it must not pretend to be a release.
	if !got.Unreleased {
		t.Fatalf("un-injected build reported version %q as a release", got.Version)
	}
	if !strings.Contains(strings.ToLower(got.Version), "dev") &&
		!strings.Contains(got.Version, "(devel)") {
		t.Fatalf("un-injected build version %q does not identify itself as a dev build", got.Version)
	}
}

func TestStringIsOneLine(t *testing.T) {
	oldV, oldC, oldD := Version, Commit, BuildDate
	defer func() { Version, Commit, BuildDate = oldV, oldC, oldD }()

	Version, Commit, BuildDate = "1.2.3", "abc1234", "2026-11-17T00:00:00Z"

	line := "svngod " + Get().String()
	if strings.Contains(line, "\n") {
		t.Fatalf("version line contains a newline: %q", line)
	}
	for _, part := range []string{"1.2.3", "abc1234", "2026-11-17"} {
		if !strings.Contains(line, part) {
			t.Fatalf("version line %q is missing %q", line, part)
		}
	}
}

// TestUninjectedFallsBackToBuildInfo covers `go install`/`go build` without
// ldflags: the Go toolchain still records the module version and VCS revision,
// so a binary built that way can be identified instead of just saying "dev".
func TestUninjectedFallsBackToBuildInfo(t *testing.T) {
	info := &debug.BuildInfo{
		Main: debug.Module{Version: "v0.9.1"},
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "deadbeefcafe1234567890abcdef0123456789abcd"},
			{Key: "vcs.time", Value: "2026-11-01T10:00:00Z"},
			{Key: "vcs.modified", Value: "false"},
		},
	}
	got := fromBuildInfo(info, Meta{})
	if got.Version != "v0.9.1" {
		t.Fatalf("version = %q, want v0.9.1", got.Version)
	}
	if got.Commit != "deadbeefcafe" {
		t.Fatalf("commit = %q, want deadbeefcafe (12 chars)", got.Commit)
	}
	if got.Unreleased {
		t.Fatal("a tagged module version should not be reported as unreleased")
	}
}

// TestBuildInfoMarkedDirty: a binary built from a modified tree must be marked,
// otherwise two different binaries report the same version.
func TestBuildInfoMarkedDirty(t *testing.T) {
	info := &debug.BuildInfo{
		Main: debug.Module{Version: "v0.9.1"},
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "deadbeefcafe1234567890abcdef0123456789abcd"},
			{Key: "vcs.modified", Value: "true"},
		},
	}
	got := fromBuildInfo(info, Meta{})
	if !strings.Contains(got.Version, "dirty") {
		t.Fatalf("version %q does not mark a modified build tree", got.Version)
	}
}

// TestBuildInfoDevelIsUnreleased: `go build` in a checkout reports version
// "(devel)", which must not be presented as a release.
func TestBuildInfoDevelIsUnreleased(t *testing.T) {
	info := &debug.BuildInfo{
		Main: debug.Module{Version: "(devel)"},
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "deadbeefcafe1234567890abcdef0123456789abcd"},
		},
	}
	got := fromBuildInfo(info, Meta{})
	if !got.Unreleased {
		t.Fatalf("(devel) build reported version %q as a release", got.Version)
	}
	if got.Commit != "deadbeefcafe" {
		t.Fatalf("commit lost on a devel build: %q", got.Commit)
	}
}

// TestNilBuildInfo does not panic: binaries built with an older toolchain, or
// with -buildvcs=false, can have no build info at all.
func TestNilBuildInfo(t *testing.T) {
	got := fromBuildInfo(nil, Meta{})
	if got.Version == "" {
		t.Fatal("no build info produced an empty version")
	}
	if !got.Unreleased {
		t.Fatal("no build info should be reported as unreleased")
	}
}

// TestDirtyNotDoubled: the toolchain's pseudo-version already ends in "+dirty"
// when the tree is modified. Appending our own marker produced "+dirty-dirty",
// which reads like a reporter bug instead of a property of the build.
func TestDirtyNotDoubled(t *testing.T) {
	info := &debug.BuildInfo{
		Main: debug.Module{Version: "v0.0.0-20261117073222-0f9cdcb0593f+dirty"},
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "0f9cdcb0593faaaa"},
			{Key: "vcs.modified", Value: "true"},
		},
	}
	got := fromBuildInfo(info, Meta{})
	if strings.Contains(got.Version, "dirty-dirty") {
		t.Fatalf("dirty marker doubled: %q", got.Version)
	}
	if !strings.Contains(got.Version, "dirty") {
		t.Fatalf("dirty build lost its marker entirely: %q", got.Version)
	}
	if !got.Unreleased {
		t.Fatal("dirty build not marked unreleased")
	}
}

// TestInjectedVersionIgnoresDirtyTree: a release build passes an explicit
// version and the tree it was cut from may still have uncommitted files. The
// artifact is still v2.5.0, and reporting "v2.5.0-dirty" makes an intentional
// release look like a local experiment.
func TestInjectedVersionIgnoresDirtyTree(t *testing.T) {
	info := &debug.BuildInfo{
		Main: debug.Module{Version: "(devel)"},
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "abc123def456789"},
			{Key: "vcs.modified", Value: "true"},
		},
	}
	got := fromBuildInfo(info, Meta{Version: "v2.5.0"})
	if got.Version != "v2.5.0" {
		t.Fatalf("injected release version became %q", got.Version)
	}
	if got.Unreleased {
		t.Fatal("an explicitly versioned build was marked unreleased")
	}
}
