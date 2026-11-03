package store

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// listFailStore can be switched into a "process paused" state where listings
// never answer (modeled as an immediate error; the production timeout path is
// exercised by the probe's context deadline).
type listFailStore struct {
	Store
	fail atomic.Bool
}

func (s *listFailStore) ListPrefix(ctx context.Context, prefix, cont string, max int) ([]ListedObj, string, error) {
	if s.fail.Load() {
		return nil, "", errors.New("store: no answer")
	}
	return s.Store.ListPrefix(ctx, prefix, cont, max)
}

// TestHealthProbeMarksHungMirror: a replica that stops answering is taken out
// of service after two probe failures, and its recovery is forced to a full
// pass because writes buffered in its socket may never have been persisted.
func TestHealthProbeMarksHungMirror(t *testing.T) {
	a := NewMemoryStore()
	bad := &listFailStore{Store: NewMemoryStore()}
	m := NewMirror([]Store{a, bad})
	defer m.Close()

	bad.fail.Store(true)
	fails := make([]int, 2)
	m.probeOnce(fails)
	if !m.isHealthy(1) {
		t.Fatal("one failed probe took the mirror out of service")
	}
	m.probeOnce(fails)
	if m.isHealthy(1) {
		t.Fatal("two failed probes did not take the mirror out of service")
	}
	m.mu.Lock()
	whole, over := m.wholeAtDown[1], m.pendingOverflow[1]
	m.mu.Unlock()
	if whole {
		t.Fatal("hung mirror kept the delta-recovery shortcut")
	}
	if !over {
		t.Fatal("hung mirror did not force a full verification pass")
	}

	// Recovery clears both flags once the mirror is whole again.
	bad.fail.Store(false)
	m.RepairNow()
	if !m.isHealthy(1) {
		t.Fatal("mirror did not return to service after repair")
	}
	m.mu.Lock()
	over = m.pendingOverflow[1]
	m.mu.Unlock()
	if over {
		t.Fatal("pendingOverflow was not cleared on heal")
	}
}
