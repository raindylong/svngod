package server

import (
	"fmt"
	"os"
)

// createTemp creates a temp file whose name carries this process id, e.g.
// svngod-body-1234-98765. A later server start can then reap files left by a
// process that died mid-upload (see cmd/svngod's startup sweep).
func createTemp(kind string) (*os.File, error) {
	return os.CreateTemp("", fmt.Sprintf("svngod-%s-%d-", kind, os.Getpid()))
}
