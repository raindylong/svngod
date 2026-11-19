package main

import (
	"os"
	"path/filepath"
	"testing"
)

// staleTempRe decides which leftover transfer files get reclaimed. Three
// prefixes were missing from it: svngod-mirror-fast-<pid>-, svngod-repair- and
// svngod-recover-. Those are exactly the files a crashed mirror repair leaves
// behind, and they can be as large as the object being repaired, so an uncovered
// prefix means the disk fills up slowly and permanently.
//
// Every prefix in this table is produced by a real os.CreateTemp call in the
// tree; the test fails if the reclaimer does not know one of them.
func TestCleanupStaleTempFilesCoversEveryPrefix(t *testing.T) {
	dir := t.TempDir()
	self := itoa(os.Getpid())

	// name -> shouldBeRemoved for a dead owner pid.
	cases := []struct {
		name     string
		removeIt bool
	}{
		{"svngod-body-999999-1234", true},
		{"svngod-delta-999999-1", true},
		{"svngod-text-999999-1", true},
		{"svngod-txn-999999-1", true},
		{"svngod-put-999999-1", true},
		{"svngod-mirror-999999-1", true},
		{"svngod-mirror-fast-999999-1", true}, // was missing
		{"svngod-stream-999999-1", true},
		{"svngod-repair-999999", true}, // was missing
		{"svngod-recover-999999", true},
		// Own and live processes must survive, whatever the prefix.
		{"svngod-mirror-fast-" + self + "-1", false},
		{"svngod-repair-" + self, false},
		{"svngod-recover-" + self, false},
		{"svngod-repair-4242", false},
		{"svngod-recover-4242", false},
		// Not ours at all.
		{"unrelated.tmp", false},
		{"svngocli-out-999999", false},
	}
	for _, c := range cases {
		if err := os.WriteFile(filepath.Join(dir, c.name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	cleanupStaleTempFilesIn(dir, func(pid int) bool { return pid == 4242 })

	for _, c := range cases {
		_, err := os.Stat(filepath.Join(dir, c.name))
		gone := os.IsNotExist(err)
		if c.removeIt && !gone {
			t.Errorf("%s should have been reclaimed: a stale file with this prefix never expires", c.name)
		}
		if !c.removeIt && gone {
			t.Errorf("%s was removed but belongs to a live or unrelated owner", c.name)
		}
	}
}

// TestCleanupStaleTempFilesKeepsAllLive confirms the whole set survives when the
// owner is alive, so a running mirror repair is never undercut by the sampler.
func TestCleanupStaleTempFilesKeepsAllLive(t *testing.T) {
	dir := t.TempDir()
	names := []string{
		"svngod-mirror-fast-4242-1", "svngod-repair-4242", "svngod-recover-4242",
	}
	for _, n := range names {
		if err := os.WriteFile(filepath.Join(dir, n), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cleanupStaleTempFilesIn(dir, func(pid int) bool { return pid == 4242 })
	for _, n := range names {
		if _, err := os.Stat(filepath.Join(dir, n)); err != nil {
			t.Errorf("%s (live owner) should have been kept: %v", n, err)
		}
	}
}
