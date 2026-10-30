package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// runHook executes <dir>/<name> with the repository name and transaction id as
// arguments plus the given environment. Output is captured so a failing
// pre-commit can report the hook's message to the client.
func runHook(dir, name, repo, tx string, env map[string]string, stdin []byte) error {
	path := filepath.Join(dir, name)
	info, err := os.Stat(path)
	if err != nil || info.Mode()&0o111 == 0 {
		return nil // no hook installed
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, repo, tx)
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
