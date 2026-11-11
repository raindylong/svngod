//go:build linux

package store

import "golang.org/x/sys/unix"

// syncFs performs a filesystem-wide flush of every dirty page behind fd.
// handled is always true on Linux; one syncfs replaces hundreds of per-chunk
// fsyncs for a large upload.
func syncFs(fd int) (handled bool, err error) {
	return true, unix.Syncfs(fd)
}
