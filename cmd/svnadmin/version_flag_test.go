package main

import (
	"os/exec"
	"strings"
	"testing"
)

// --version has to work without any other configuration. For svnadmin that is
// the easy thing to get wrong: it validates the subcommand arguments before
// doing anything, so a version request would exit with the usage error instead
// of printing a version. Same for svngod, which wants --repo-root before it
// will start.
//
// These build the real binary: the flag lives in main(), which a unit test in
// the same package cannot call without running the server.

// buildBinary builds cmd/<name> into a temp dir and returns its path.
func buildBinary(t *testing.T, name string) string {
	t.Helper()
	out := t.TempDir() + "/" + name
	cmd := exec.Command("go", "build", "-o", out, "svngod/cmd/"+name)
	cmd.Env = append(cmd.Environ(), "CGO_ENABLED=0")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", name, err, b)
	}
	return out
}

// TestVersionFlagPrintsAndExits covers the three binaries with the spelling a
// human types.
func TestVersionFlagPrintsAndExits(t *testing.T) {
	for _, name := range []string{"svngod", "svnadmin", "svngocli"} {
		t.Run(name, func(t *testing.T) {
			bin := buildBinary(t, name)
			out, err := exec.Command(bin, "--version").CombinedOutput()
			// A clean exit is part of the contract: `set -e` scripts check it.
			// (Before this was implemented svnadmin exited 2 with usage text,
			// and logging the error instead of failing let that pass.)
			if err != nil {
				t.Fatalf("%s --version exited with %v: %q", name, err, out)
			}
			got := string(out)
			if !strings.Contains(got, name) {
				t.Fatalf("%s --version did not name the program: %q", name, got)
			}
			// It must report something version-shaped, not an empty string.
			trimmed := strings.TrimSpace(got)
			if len(trimmed) <= len(name)+1 {
				t.Fatalf("%s --version printed no version: %q", name, got)
			}
		})
	}
}

// TestVersionNeedsNoOtherFlags is the regression this file exists for: neither
// binary may demand its required flags before honouring --version.
func TestVersionNeedsNoOtherFlags(t *testing.T) {
	for _, name := range []string{"svngod", "svnadmin"} {
		t.Run(name, func(t *testing.T) {
			bin := buildBinary(t, name)
			// No --repo-root (svngod) / no subcommand (svnadmin).
			out, _ := exec.Command(bin, "--version").CombinedOutput()
			got := string(out)
			if strings.Contains(got, "--repo-root required") {
				t.Fatalf("%s demanded --repo-root before printing the version", name)
			}
			if strings.Contains(strings.ToLower(got), "usage:") && !strings.Contains(got, "commit") {
				t.Fatalf("%s printed only usage for --version: %q", name, got)
			}
			if !strings.Contains(got, name) {
				t.Fatalf("%s --version printed %q, which does not identify the build", name, got)
			}
		})
	}
}

// TestVersionInjectedViaLdflags proves the injection path the release build
// uses actually reaches the output.
func TestVersionInjectedViaLdflags(t *testing.T) {
	out := t.TempDir() + "/svngod"
	cmd := exec.Command("go", "build",
		"-ldflags", "-X svngod/internal/version.Version=9.9.9-test -X svngod/internal/version.Commit=abcdef123456 -X svngod/internal/version.BuildDate=2026-11-17T00:00:00Z",
		"-o", out, "svngod/cmd/svngod")
	cmd.Env = append(cmd.Environ(), "CGO_ENABLED=0")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build with ldflags: %v\n%s", err, b)
	}
	got, _ := exec.Command(out, "--version").CombinedOutput()
	s := string(got)
	for _, want := range []string{"9.9.9-test", "abcdef123456", "2026-11-17"} {
		if !strings.Contains(s, want) {
			t.Fatalf("--version output %q is missing injected %q", s, want)
		}
	}
}

// TestVersionGoesToStdout keeps --version scriptable: version output written to
// stderr disappears when a caller does `v=$(svngod --version)`.
func TestVersionGoesToStdout(t *testing.T) {
	bin := buildBinary(t, "svnadmin")
	var stdout, stderr strings.Builder
	cmd := exec.Command(bin, "--version")
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	_ = cmd.Run()

	if strings.TrimSpace(stdout.String()) == "" {
		t.Fatalf("nothing on stdout; stderr had %q", stderr.String())
	}
	if !strings.Contains(stdout.String(), "svnadmin") {
		t.Fatalf("stdout %q does not identify the program", stdout.String())
	}
}
