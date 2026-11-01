package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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
	cmd.Env = os.Environ()
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
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

var _ = fmt.Sprintf
