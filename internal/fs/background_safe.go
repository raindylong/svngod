package fs

import (
	"log"
	"runtime/debug"
)

// goSafe starts a background task that survives a panic.
//
// Background work (loose reclaim, compaction, scrub) has no request to unwind
// into: an unrecovered panic terminates the whole process, taking every
// repository on the node with it. These tasks are also best-effort by design -
// each is retried by the next sweep - so dropping one iteration is strictly
// better than dropping the process.
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
// Returning from such a loop would silently disable the feature it provides
// (compaction, health probing, mirror repair) while the process looks healthy.
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
