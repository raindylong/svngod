// Package version reports what build of svngod is running.
//
// The values are set at link time:
//
//	go build -ldflags "-X svngod/internal/version.Version=1.2.3 \
//	  -X svngod/internal/version.Commit=$(git rev-parse --short HEAD) \
//	  -X svngod/internal/version.BuildDate=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
//
// When they are not set — a plain `go build`, a `go install`, a developer's
// checkout — the module version and VCS stamp the toolchain records are used
// instead. If even those are absent the binary reports itself as an unreleased
// dev build rather than inventing a number: an operator must be able to tell a
// release from a working copy, and a fake version makes that impossible.
package version

import (
	"fmt"
	"runtime/debug"
	"strings"
)

// Set at link time with -ldflags -X. Empty means "not injected".
var (
	Version   string
	Commit    string
	BuildDate string
)

// Meta is the resolved build identity.
type Meta struct {
	Version string
	Commit  string
	// BuildDate is the commit time (or link time), formatted RFC3339.
	BuildDate string
	// Unreleased is true when the version does not identify a tagged release:
	// no injected version, a "(devel)" module, or a modified working tree.
	Unreleased bool
}

// String is the one-line form used by --version. It never contains a newline so
// callers can embed it in a log line or an audit record.
func (m Meta) String() string {
	var b strings.Builder
	b.WriteString(m.Version)
	if m.Commit != "" {
		b.WriteString(" (commit " + m.Commit)
		if m.BuildDate != "" {
			b.WriteString(", built " + m.BuildDate)
		}
		b.WriteString(")")
	} else if m.BuildDate != "" {
		b.WriteString(" (built " + m.BuildDate + ")")
	}
	return b.String()
}

// Get resolves the build identity, preferring injected values.
func Get() Meta {
	return fromBuildInfo(readBuildInfo(), Meta{
		Version: Version, Commit: Commit, BuildDate: BuildDate,
	})
}

// readBuildInfo is a variable so tests can supply their own.
var readBuildInfo = func() *debug.BuildInfo {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return nil
	}
	return info
}

// fromBuildInfo fills in whatever the injected metadata left empty, using the
// module version and VCS stamp recorded by the toolchain.
func fromBuildInfo(info *debug.BuildInfo, injected Meta) Meta {
	out := injected
	out.Unreleased = injected.Version == ""

	var moduleVersion, revision, vcsTime string
	dirty := false
	if info != nil {
		moduleVersion = info.Main.Version
		for _, s := range info.Settings {
			switch s.Key {
			case "vcs.revision":
				revision = s.Value
			case "vcs.time":
				vcsTime = s.Value
			case "vcs.modified":
				dirty = s.Value == "true"
			}
		}
	}

	if out.Version == "" {
		// "(devel)" and "" both mean the toolchain has no version to report;
		// call it dev rather than echoing a placeholder that looks like a tag.
		if moduleVersion != "" && moduleVersion != "(devel)" {
			out.Version = moduleVersion
			out.Unreleased = false
		} else {
			out.Version = "dev"
			out.Unreleased = true
		}
	}
	if out.Commit == "" && revision != "" {
		out.Commit = revision
		if len(out.Commit) > 12 {
			out.Commit = out.Commit[:12]
		}
	}
	if out.BuildDate == "" && vcsTime != "" {
		out.BuildDate = vcsTime
	}
	// The dirty marker only applies to a version the toolchain derived. When a
	// release build injected an explicit version, the tree state is already
	// accounted for by whoever produced that version; appending "-dirty" there
	// would make a deliberate release artifact look unreproducible.
	if dirty && injected.Version == "" {
		// The toolchain already appends "+dirty" to a module pseudo-version, so
		// only add the marker when it is not there: otherwise the output reads
		// "+dirty-dirty", which looks like a bug in the reporter.
		if !strings.Contains(out.Version, "dirty") {
			out.Version += "-dirty"
		}
		out.Unreleased = true
	}
	return out
}

// Line returns "<program> <version>" for a --version banner.
func Line(program string) string {
	return fmt.Sprintf("%s %s", program, Get().String())
}
