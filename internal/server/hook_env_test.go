package server

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"svngod/internal/fs"
)

// Hooks used to receive os.Environ() wholesale, so every secret the server
// process holds was visible to them: SVNGOD_ADMIN_TOKEN and
// SVNGOD_METRICS_TOKEN (the documented place to keep them is the environment -
// see the systemd unit's EnvironmentFile comment), plus AWS_* / RUSTFS_*
// storage credentials when EnvCreds is used.
//
// A hook is operator-installed, so this is not remote code execution; the risk
// is that a repository's hook - often written by a team, not the operator, and
// often a wrapper around a third-party script - can read and exfiltrate the
// server's credentials. The hook contract only documents SVN_* variables, so
// anything else being present is unintended exposure.
//
// These tests pin the contract: exactly the SVN_* variables the hook is
// documented to get, plus a minimal PATH, and nothing that looks like a secret.

// writeEnvDumpingHook installs a pre-commit that records its environment.
func writeEnvDumpingHook(t *testing.T, dir, out string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir hooks: %v", err)
	}
	script := "#!/bin/sh\nenv > '" + out + "'\necho \"PATH=$PATH\" >> '" + out + "'\n"
	if err := os.WriteFile(filepath.Join(dir, "pre-commit"), []byte(script), 0o755); err != nil {
		t.Fatalf("write hook: %v", err)
	}
}

// TestHookDoesNotInheritServerSecrets is the core assertion: a secret present in
// the server's environment must not reach the hook.
func TestHookDoesNotInheritServerSecrets(t *testing.T) {
	secrets := map[string]string{
		"SVNGOD_ADMIN_TOKEN":    "admin-token-do-not-leak",
		"SVNGOD_METRICS_TOKEN":  "metrics-token-do-not-leak",
		"AWS_SECRET_ACCESS_KEY": "aws-secret-do-not-leak",
		"AWS_ACCESS_KEY_ID":     "aws-key-do-not-leak",
		"LDAP_BIND_PASSWORD":    "ldap-do-not-leak",
	}
	for k, v := range secrets {
		t.Setenv(k, v)
	}

	dir := t.TempDir()
	out := filepath.Join(dir, "hook-env.txt")
	hooksDir := filepath.Join(dir, "hooks")
	writeEnvDumpingHook(t, hooksDir, out)

	// Run the hook through the real entry point so the test covers the code
	// path a commit takes, not a helper the caller could bypass.
	if err := runHook(hooksDir, "pre-commit", "demo", "txn-1", nil, nil); err != nil {
		t.Fatalf("hook failed: %v", err)
	}

	dump, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read hook env: %v", err)
	}
	text := string(dump)
	for k, v := range secrets {
		if strings.Contains(text, v) {
			t.Fatalf("hook environment leaked %s=%s:\n%s", k, v, text)
		}
	}
}

// TestHookGetsDocumentedVariables is the false-negative guard: stripping the
// environment must not strip what hooks legitimately need, or every existing
// hook breaks. SVN_REPO / SVN_TXN / SVN_USER are the documented contract.
func TestHookGetsDocumentedVariables(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "hook-env.txt")
	hooksDir := filepath.Join(dir, "hooks")

	if err := os.MkdirAll(hooksDir, 0o755); err != nil {
		t.Fatalf("mkdir hooks: %v", err)
	}
	script := "#!/bin/sh\nenv > '" + out + "'\n"
	if err := os.WriteFile(filepath.Join(hooksDir, "pre-commit"), []byte(script), 0o755); err != nil {
		t.Fatalf("write hook: %v", err)
	}

	env := map[string]string{
		"SVN_REPO": "myrepo",
		"SVN_TXN":  "txn-abc",
		"SVN_USER": "alice",
	}
	if err := runHookArgs(hooksDir, "pre-commit", []string{"myrepo", "txn-abc"}, env, nil); err != nil {
		t.Fatalf("hook failed: %v", err)
	}

	dump, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read hook env: %v", err)
	}
	text := string(dump)
	for k, v := range env {
		if !strings.Contains(text, k+"="+v) {
			t.Fatalf("hook did not receive %s=%s:\n%s", k, v, text)
		}
	}
}

// TestHookGetsUsablePATH: the hook interpreter needs to find sh/env/grep. A
// stripped environment must still carry a working PATH, or every shell hook
// fails with "command not found" - a regression that would be far more damaging
// than the leak being fixed.
func TestHookGetsUsablePATH(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "hook-env.txt")
	hooksDir := filepath.Join(dir, "hooks")

	if err := os.MkdirAll(hooksDir, 0o755); err != nil {
		t.Fatalf("mkdir hooks: %v", err)
	}
	// `env` only resolves if PATH is set. Its own output proves PATH worked.
	script := "#!/bin/sh\nenv > '" + out + "'\n"
	if err := os.WriteFile(filepath.Join(hooksDir, "pre-commit"), []byte(script), 0o755); err != nil {
		t.Fatalf("write hook: %v", err)
	}

	if err := runHook(hooksDir, "pre-commit", "demo", "txn-1", nil, nil); err != nil {
		t.Fatalf("hook failed to run, which is what a broken PATH looks like: %v", err)
	}
	dump, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("hook produced no output, so its interpreter could not run: %v", err)
	}
	var pathLine string
	for _, line := range strings.Split(string(dump), "\n") {
		if strings.HasPrefix(line, "PATH=") {
			pathLine = line
			break
		}
	}
	if pathLine == "" {
		t.Fatalf("hook has no PATH, so it cannot resolve commands:\n%s", dump)
	}
	if strings.TrimPrefix(pathLine, "PATH=") == "" {
		t.Fatalf("hook PATH is empty:\n%s", dump)
	}
}

// TestHookEnvironmentIsDeterministic: the same inputs must produce the same
// environment. Inheriting os.Environ() made hook behaviour depend on however
// the server happened to be launched, which is a reproducibility problem
// independent of the secret leak.
func TestHookEnvironmentIsDeterministic(t *testing.T) {
	dir := t.TempDir()
	hooksDir := filepath.Join(dir, "hooks")
	if err := os.MkdirAll(hooksDir, 0o755); err != nil {
		t.Fatalf("mkdir hooks: %v", err)
	}
	out := filepath.Join(dir, "env.txt")
	script := "#!/bin/sh\nenv | sort > '" + out + "'\n"
	if err := os.WriteFile(filepath.Join(hooksDir, "pre-commit"), []byte(script), 0o755); err != nil {
		t.Fatalf("write hook: %v", err)
	}

	read := func() string {
		if err := runHook(hooksDir, "pre-commit", "demo", "t", nil, nil); err != nil {
			t.Fatalf("hook: %v", err)
		}
		b, err := os.ReadFile(out)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		return string(b)
	}

	first := read()
	// Change the parent environment the way a redeploy might.
	t.Setenv("SOME_UNRELATED_VARIABLE", "value-"+time.Now().Format(time.RFC3339Nano))
	second := read()
	if first != second {
		t.Fatalf("hook environment changed with the parent environment:\n--- first ---\n%s\n--- second ---\n%s", first, second)
	}
}

// TestCommitHookDoesNotSeeServerSecrets runs the whole path a real commit takes
// (MKACTIVITY -> PROPPATCH -> MERGE), not the helper directly. The point is that
// no call site can route around hookEnv: three places invoke hooks
// (pre-commit, post-commit, pre-revprop-change) and they all funnel through
// runHookArgs, which this pins.
func TestCommitHookDoesNotSeeServerSecrets(t *testing.T) {
	const secret = "admin-token-must-not-reach-hook"
	t.Setenv("SVNGOD_ADMIN_TOKEN", secret)

	dir := t.TempDir()
	out := filepath.Join(dir, "hook-env.txt")
	hooksDir := filepath.Join(dir, "hooks")
	writeEnvDumpingHook(t, hooksDir, out)

	repo := buildRepo(t)
	srv := New(Config{
		BasePath: "/svn",
		HooksDir: hooksDir,
		OpenRepo: func(name string) (*fs.Repo, error) {
			if name == "demo" {
				return repo, nil
			}
			return nil, fs.ErrRepoNotFound
		},
	})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	commitRequest(t, ts.URL, "demo", "hook-env-1")

	dump, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("the pre-commit hook did not run, so this test proves nothing: %v", err)
	}
	if strings.Contains(string(dump), secret) {
		t.Fatalf("a real commit's hook environment leaked SVNGOD_ADMIN_TOKEN:\n%s", dump)
	}
	// And it did receive its documented contract.
	if !strings.Contains(string(dump), "SVN_REPO=demo") {
		t.Fatalf("hook did not receive SVN_REPO:\n%s", dump)
	}
}

// TestHookEnvironmentMatchesDocumentedContract pins the variable set against
// the table in docs/配置与多实例部署.md §7.1. Adding a variable to a call site
// without documenting it - or the reverse - is how the docs and reality drift
// apart, and operators rely on this list when writing hooks.
func TestHookEnvironmentMatchesDocumentedContract(t *testing.T) {
	dir := t.TempDir()
	hooksDir := filepath.Join(dir, "hooks")
	if err := os.MkdirAll(hooksDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	out := filepath.Join(dir, "env.txt")
	script := "#!/bin/sh\nenv > '" + out + "'\n"
	if err := os.WriteFile(filepath.Join(hooksDir, "pre-commit"), []byte(script), 0o755); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Exactly what the pre-commit call site passes.
	if err := runHook(hooksDir, "pre-commit", "demo", "txn-9", map[string]string{
		"SVN_USER": "alice", "SVN_REPO": "demo", "SVN_TXN": "txn-9",
	}, nil); err != nil {
		t.Fatalf("hook: %v", err)
	}
	dump, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	got := map[string]bool{}
	for _, line := range strings.Split(string(dump), "\n") {
		if i := strings.IndexByte(line, '='); i > 0 {
			got[line[:i]] = true
		}
	}
	want := map[string]bool{"PATH": true, "SVN_USER": true, "SVN_REPO": true, "SVN_TXN": true}
	// The shell adds these to its own environment; they say nothing about what
	// svngod passed (the hook binary is /bin/sh, which sets SHLVL, _ and PWD by
	// itself). Excluding them keeps the assertion about OUR contract.
	shellAdded := map[string]bool{"SHLVL": true, "_": true, "PWD": true, "OLDPWD": true}
	for k := range got {
		if want[k] || shellAdded[k] {
			continue
		}
		t.Fatalf("hook received undocumented variable %q; update the §7.1 table or stop passing it", k)
	}
	for k := range want {
		if !got[k] {
			t.Fatalf("hook did not receive documented variable %q", k)
		}
	}
}

// TestHookEnvFallsBackWhenPATHIsUnset covers the branch a shell-level test
// cannot reach: when the server itself has no PATH, a hook must still get a
// usable one, or every script hook dies with "command not found".
func TestHookEnvFallsBackWhenPATHIsUnset(t *testing.T) {
	t.Setenv("PATH", "")
	got := hookEnv(map[string]string{"SVN_REPO": "demo"})
	var pathLine string
	for _, e := range got {
		if strings.HasPrefix(e, "PATH=") {
			pathLine = e
		}
	}
	if pathLine == "" {
		t.Fatalf("hookEnv produced no PATH entry: %v", got)
	}
	if strings.TrimPrefix(pathLine, "PATH=") == "" {
		t.Fatalf("hookEnv produced an empty PATH: %v", got)
	}
}

// TestHookEnvIsSorted pins determinism at the unit level: a hook's environment
// must not depend on Go's randomized map iteration order.
func TestHookEnvIsSorted(t *testing.T) {
	env := map[string]string{"SVN_Z": "1", "SVN_A": "2", "SVN_M": "3"}
	first := hookEnv(env)
	for i := 0; i < 20; i++ {
		if cur := hookEnv(env); !equalStrings(first, cur) {
			t.Fatalf("hookEnv is not deterministic:\n%v\n%v", first, cur)
		}
	}
	// PATH first, then the variables in order.
	if !strings.HasPrefix(first[0], "PATH=") {
		t.Fatalf("first entry is %q, want PATH", first[0])
	}
	if first[1] != "SVN_A=2" || first[2] != "SVN_M=3" || first[3] != "SVN_Z=1" {
		t.Fatalf("variables not sorted: %v", first)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
