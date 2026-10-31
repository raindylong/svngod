package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCleanupStaleTempFiles(t *testing.T) {
	dir := t.TempDir()
	dead := filepath.Join(dir, "svngod-body-999999-1234")
	live := filepath.Join(dir, "svngod-text-4242-9")
	self := filepath.Join(dir, "svngod-mirror-"+itoa(os.Getpid())+"-1")
	other := filepath.Join(dir, "unrelated.tmp")
	for _, p := range []string{dead, live, self, other} {
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cleanupStaleTempFilesIn(dir, func(pid int) bool { return pid == 4242 })
	for _, p := range []string{dead} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("%s should have been removed", p)
		}
	}
	for _, p := range []string{live, self, other} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("%s should have been kept: %v", p, err)
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
