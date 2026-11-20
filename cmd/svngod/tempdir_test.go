package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// Mirror transfers buffer a whole object through os.CreateTemp so a standby can
// receive the same bytes twice. Those objects can be as large as a committed
// file (the bundle cap is 64 GiB), and the temp copy used to land in
// os.TempDir(). On a container /tmp is often tmpfs (so a large commit consumes
// memory) or a small partition (so it fails with ENOSPC), and neither shows up
// until someone commits something big.
//
// The fix routes temp files through a configurable directory. TMPDIR is what
// os.TempDir() reads, so setting it is enough to move all seven call sites -
// but the setting has to be applied and validated at startup, which is what
// these tests cover.

func TestApplyTempDirCreatesAndUsesDirectory(t *testing.T) {
	base := t.TempDir()
	want := filepath.Join(base, "svngod-tmp")

	cleanup, err := applyTempDir(want)
	if err != nil {
		t.Fatalf("applyTempDir(%q): %v", want, err)
	}
	defer cleanup()

	if got := os.TempDir(); got != want {
		t.Fatalf("os.TempDir()=%q, want %q", got, want)
	}
	if fi, err := os.Stat(want); err != nil || !fi.IsDir() {
		t.Fatalf("temp dir was not created: %v", err)
	}
	// A temp file must actually appear there.
	f, err := createTempForTest()
	if err != nil {
		t.Fatalf("create temp: %v", err)
	}
	name := f.Name()
	f.Close()
	defer os.Remove(name)
	if filepath.Dir(name) != want {
		t.Fatalf("temp file created in %q, want %q", filepath.Dir(name), want)
	}
}

// TestApplyTempDirRestoresOnCleanup makes the previous TMPDIR survive, so a test
// (or an embedding process) that changes it does not poison later work.
func TestApplyTempDirRestoresOnCleanup(t *testing.T) {
	orig := os.Getenv("TMPDIR")
	want := filepath.Join(t.TempDir(), "tmp2")
	cleanup, err := applyTempDir(want)
	if err != nil {
		t.Fatal(err)
	}
	cleanup()
	if got := os.Getenv("TMPDIR"); got != orig {
		t.Fatalf("TMPDIR=%q after cleanup, want %q", got, orig)
	}
}

// TestApplyTempDirRejectsUnusablePath fails fast at startup instead of at the
// first large commit. A path that exists as a regular file is the cheap case to
// pin; a read-only directory would need root to construct portably.
func TestApplyTempDirRejectsUnusablePath(t *testing.T) {
	base := t.TempDir()
	file := filepath.Join(base, "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := applyTempDir(file); err == nil {
		t.Fatal("applyTempDir accepted a regular file as the temp directory")
	}
}

// TestApplyTempDirEmptyIsNoop keeps the default behaviour (os.TempDir()) for
// deployments that do not set the flag.
func TestApplyTempDirEmptyIsNoop(t *testing.T) {
	orig := os.TempDir()
	cleanup, err := applyTempDir("")
	if err != nil {
		t.Fatalf("applyTempDir(\"\"): %v", err)
	}
	defer cleanup()
	if got := os.TempDir(); got != orig {
		t.Fatalf("os.TempDir() changed to %q for an empty setting", got)
	}
}

// createTempForTest goes through the same helper the handlers use, so the test
// proves the code path, not just os.TempDir().
func createTempForTest() (*os.File, error) {
	return os.CreateTemp("", "svngod-body-"+itoa(os.Getpid())+"-")
}

// TestConfigTempDirIsAppliedOnStartup covers the deployment path, not the flag.
//
// The documented production setup is `svngod --config ...` (the systemd unit and
// the container entrypoint both use it), and -temp-dir is a flag - so before this
// was wired up, the one deployment shape that most needs temp files off /tmp
// could not set them at all.
//
// The check is the directory itself: applyTempDir creates and write-probes it,
// so a directory that appears at the configured path proves the config value
// reached it. That is stronger than asserting a parsed struct field, which would
// pass even if nothing ever called applyTempDir.
func TestConfigTempDirIsAppliedOnStartup(t *testing.T) {
	if os.Getenv("SVNGOD_TEST_TEMPDIR_CHILD") == "1" {
		// Child: behave as the real entrypoint and never return on success.
		args := os.Args[len(os.Args)-1:]
		runMultiRepo(args[0], 16, 2, "", "", "", false)
		return
	}

	dir := t.TempDir()
	tempDir := filepath.Join(dir, "t", "nested") // must be created, not just used
	repoRoot := filepath.Join(dir, "repos")
	if err := os.MkdirAll(repoRoot, 0o700); err != nil {
		t.Fatalf("mkdir repos: %v", err)
	}
	cfgPath := filepath.Join(dir, "svngod.json")
	// addr on port 0: the test does not care where it listens, only that startup
	// gets far enough to apply the temp dir.
	content := `{"addr":"127.0.0.1:0","base-path":"/svn","temp-dir":"` + tempDir + `","repos":[{"name":"demo","storage":{"type":"file","root":"` + repoRoot + `"}}]}`
	if err := os.WriteFile(cfgPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("executable: %v", err)
	}
	cmd := exec.Command(exe, "-test.run=TestConfigTempDirIsAppliedOnStartup", "--", cfgPath)
	cmd.Env = append(os.Environ(), "SVNGOD_TEST_TEMPDIR_CHILD=1")
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}()

	// applyTempDir runs during startup, before the listener; poll briefly for it.
	deadline := time.Now().Add(20 * time.Second)
	for {
		if fi, err := os.Stat(tempDir); err == nil && fi.IsDir() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("configured temp-dir %q was never created; startup output:\n%s", tempDir, out.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
}
