package server

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// runHook executes <dir>/<name> with the repository name and transaction id as
// arguments plus the given environment. Output is captured so a failing
// pre-commit can report the hook's message to the client.
func runHook(dir, name, repo, tx string, env map[string]string, stdin []byte) error {
	return runHookArgs(dir, name, []string{repo, tx}, env, stdin)
}

// hookInstalled reports whether an executable hook of that name exists.
func hookInstalled(dir, name string) bool {
	if dir == "" {
		return false
	}
	info, err := os.Stat(filepath.Join(dir, name))
	return err == nil && info.Mode()&0o111 != 0
}

// runRevPropHook runs pre-revprop-change with subversion's argument order:
// <repos> <rev> <user> <propname> <action> [<value>]. The same values are also
// exported as SVN_* environment variables for scripts that prefer them.
func runRevPropHook(dir, repoName string, rev uint64, user, prop, action, value string) error {
	args := []string{repoName, strconv.FormatUint(rev, 10), user, prop, action}
	env := map[string]string{
		"SVN_REPO": repoName, "SVN_REV": strconv.FormatUint(rev, 10),
		"SVN_USER": user, "SVN_PROPNAME": prop, "SVN_ACTION": action,
		"SVN_VALUE": value,
	}
	if action == "M" {
		args = append(args, value)
	}
	return runHookArgs(dir, "pre-revprop-change", args, env, nil)
}

func runHookArgs(dir, name string, args []string, env map[string]string, stdin []byte) error {
	path := filepath.Join(dir, name)
	info, err := os.Stat(path)
	if err != nil || info.Mode()&0o111 == 0 {
		return nil // no hook installed
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Env = hookEnv(env)
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(out.String())
		if len(msg) > 4000 {
			msg = msg[:4000]
		}
		if msg == "" {
			msg = err.Error()
		}
		return errors.New(msg)
	}
	return nil
}

// hookEnv builds the environment a hook runs with: the documented SVN_*
// variables plus a minimal PATH, and nothing else.
//
// Hooks used to inherit os.Environ(), which handed every operator secret to
// them - SVNGOD_ADMIN_TOKEN and SVNGOD_METRICS_TOKEN (the systemd unit puts
// those in an EnvironmentFile precisely because the environment is where they
// live), and AWS_*/RUSTFS_* when the S3 client uses environment credentials.
// Hooks are usually written by repository teams and often wrap third-party
// scripts, so "trusted because the operator installed it" is not a safe
// assumption for credentials that grant control of the whole server.
//
// PATH is inherited deliberately: hooks are shell scripts and need to resolve
// sh, grep, awk. An empty PATH breaks every existing hook, which would be a
// worse regression than the leak. Nothing else is inherited - a hook's
// behaviour now depends only on its arguments and the SVN_* contract, not on
// how the server happened to be launched.
func hookEnv(env map[string]string) []string {
	path := os.Getenv("PATH")
	if path == "" {
		// A PATH-less environment is broken for scripts; fall back to the
		// directories a hook realistically needs rather than leaving it unset.
		path = "/usr/local/bin:/usr/bin:/bin"
	}
	// Sorted and explicit: map iteration order is random, and an environment
	// that reorders between runs makes hook behaviour harder to reproduce.
	out := make([]string, 0, len(env)+1)
	out = append(out, "PATH="+path)
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		out = append(out, k+"="+env[k])
	}
	return out
}
