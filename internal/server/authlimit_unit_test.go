package server

import (
	"testing"
	"time"
)

// Direct tests for the throttle. The handler-level tests above verify the
// contract "a success ends that identity's streak", but they cannot distinguish
// "clears the user key" from "clears every key" - a stale address counter is
// masked by the user counter also being full. These pin the primitive.

func TestThrottleSucceedClearsOnlyItsOwnKey(t *testing.T) {
	a := newAuthThrottle()
	now := time.Now()
	a.now = func() time.Time { return now }

	a.fail("user:alice", time.Minute)
	a.fail("user:alice", time.Minute)
	a.fail("ip:10.0.0.1", time.Minute)

	if blocked, _ := a.throttled("user:alice", 2, time.Minute); !blocked {
		t.Fatal("user:alice should be at its limit")
	}
	a.succeed("user:alice")

	if blocked, _ := a.throttled("user:alice", 2, time.Minute); blocked {
		t.Fatal("succeed did not clear its own key")
	}
	if blocked, _ := a.throttled("ip:10.0.0.1", 1, time.Minute); !blocked {
		t.Fatal("succeed cleared an unrelated key")
	}
}

func TestThrottleWindowExpiry(t *testing.T) {
	a := newAuthThrottle()
	now := time.Now()
	a.now = func() time.Time { return now }

	a.fail("user:bob", time.Minute)
	if blocked, _ := a.throttled("user:bob", 1, time.Minute); !blocked {
		t.Fatal("expected blocked within the window")
	}
	now = now.Add(61 * time.Second)
	if blocked, _ := a.throttled("user:bob", 1, time.Minute); blocked {
		t.Fatal("entry survived past its window")
	}
}

func TestThrottleBurstZeroDisables(t *testing.T) {
	a := newAuthThrottle()
	for i := 0; i < 50; i++ {
		a.fail("user:x", time.Minute)
	}
	if blocked, _ := a.throttled("user:x", 0, time.Minute); blocked {
		t.Fatal("burst 0 must disable the limiter")
	}
}

// TestThrottleBoundedGrowth: a rotating attacker must not grow the table without
// limit. Trading a CPU problem for a memory problem is not a fix.
func TestThrottleBoundedGrowth(t *testing.T) {
	a := newAuthThrottle()
	for i := 0; i < maxThrottleEntries*3; i++ {
		a.fail("ip:10."+itoa(i/65536)+"."+itoa((i/256)%256)+"."+itoa(i%256), time.Minute)
	}
	a.mu.Lock()
	n := len(a.entries)
	a.mu.Unlock()
	if n > maxThrottleEntries {
		t.Fatalf("throttle table grew to %d entries, cap is %d", n, maxThrottleEntries)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
