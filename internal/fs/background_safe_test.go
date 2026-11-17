package fs

import (
	"sync/atomic"
	"testing"
)

// An unrecovered panic in a background task terminates the process, and an
// unrecovered panic in a background *loop* is quieter and worse: the loop exits,
// the feature it implements stops, and the process keeps reporting healthy.
//
// These helpers are the guard for both. They are used by compaction, loose
// reclaim and the mirror loops, so a regression here re-opens every one of them.

func TestGoSafeRunsWork(t *testing.T) {
	var ran atomic.Bool
	done := make(chan struct{})
	goSafe("test-runs", func() {
		ran.Store(true)
		close(done)
	})
	<-done
	if !ran.Load() {
		t.Fatal("goSafe did not run the task")
	}
}

func TestGoSafeSurvivesPanic(t *testing.T) {
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		goSafe("test-panics", func() { panic("injected background panic") })
	}()
	// If goSafe let the panic escape, the test binary dies here instead of
	// reporting a failure, which is exactly what would happen in production.
	<-finished
}

func TestRecoverLoopSurvivesPanicInOneIteration(t *testing.T) {
	// Model a loop body that panics once and then succeeds. Pre-fix, the panic
	// would end the function (and, in a goroutine, the process).
	var iterations atomic.Int32
	completed := 0
	for i := 0; i < 3; i++ {
		panicked := recoverLoop("test-loop", func() {
			n := iterations.Add(1)
			if n == 1 {
				panic("injected loop panic")
			}
			completed++
		})
		if panicked && i != 0 {
			t.Fatalf("iteration %d should not have panicked", i)
		}
	}
	if completed != 2 {
		t.Fatalf("loop body ran %d successful iterations, want 2: a panic ended the loop", completed)
	}
}

func TestRecoverLoopReportsPanic(t *testing.T) {
	panicked := recoverLoop("test-reports", func() { panic("boom") })
	if !panicked {
		t.Fatal("recoverLoop did not report the panic it swallowed")
	}
}

func TestRecoverLoopPassesThroughNormalReturn(t *testing.T) {
	ran := false
	panicked := recoverLoop("test-normal", func() { ran = true })
	if panicked {
		t.Fatal("recoverLoop reported a panic for a normal return")
	}
	if !ran {
		t.Fatal("recoverLoop did not run the body")
	}
}
