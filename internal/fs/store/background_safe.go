package store

import (
	"log"
	"runtime/debug"
)

// goSafe starts a background task that survives a panic. See the fs package for
// the rationale: these loops keep mirror replication healthy, and losing one of
// them is a silent degradation, while losing the process stops every repository.
func goSafe(name string, fn func()) {
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("PANIC in background task %q: %v\n%s", name, rec, debug.Stack())
			}
		}()
		fn()
	}()
}

// recoverLoop keeps a long-running loop alive across a panic in one iteration.
func recoverLoop(name string, body func()) (panicked bool) {
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("PANIC in background loop %q: %v\n%s", name, rec, debug.Stack())
			panicked = true
		}
	}()
	body()
	return false
}
