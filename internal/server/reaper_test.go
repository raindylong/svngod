package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"svngod/internal/fs"
)

func TestReapExpiredActivities(t *testing.T) {
	repo := buildRepo(t)
	srv := New(Config{
		BasePath: "/svn",
		OpenRepo: func(name string) (*fs.Repo, error) {
			if name == "demo" {
				return repo, nil
			}
			return nil, fs.ErrRepoNotFound
		},
	})

	dead := &activity{
		id:       "dead-activity",
		repo:     "demo",
		created:  time.Now().Add(-2 * sessionTTL).Unix(),
		revProps: map[string]string{"svn:log": "abandoned"},
		txn:      &txn{id: "txn-dead-activity", baseRev: 1, changes: map[string]*txnChange{}},
	}
	live := &activity{
		id:       "live-activity",
		repo:     "demo",
		created:  time.Now().Unix(),
		revProps: map[string]string{},
		txn:      &txn{id: "txn-live-activity", baseRev: 1, changes: map[string]*txnChange{}},
	}
	srv.mu.Lock()
	srv.activities[dead.id] = dead
	srv.txns[dead.txn.id] = dead.txn
	srv.activities[live.id] = live
	srv.txns[live.txn.id] = live.txn
	srv.mu.Unlock()
	srv.persistActivity(repo, dead.id)
	srv.persistActivity(repo, live.id)

	ctx := context.Background()
	stateKey := activityStateKey(repo, dead.id)
	if _, ok, err := repo.Store().Head(ctx, stateKey); err != nil || !ok {
		t.Fatalf("precondition: activity state missing (ok=%v err=%v)", ok, err)
	}

	srv.reapExpiredActivities()

	if _, ok := srv.activityByID(dead.id); ok {
		t.Fatal("expired activity still in memory")
	}
	if _, ok := srv.txnByID(dead.txn.id); ok {
		t.Fatal("expired transaction still in memory")
	}
	if _, ok, err := repo.Store().Head(ctx, stateKey); err != nil || ok {
		t.Fatalf("expired activity state not dropped (ok=%v err=%v)", ok, err)
	}
	if _, ok := srv.activityByID(live.id); !ok {
		t.Fatal("live activity was reaped")
	}
	if _, ok, _ := repo.Store().Head(ctx, activityStateKey(repo, live.id)); !ok {
		t.Fatal("live activity state was dropped")
	}
}

// TestReapPersistedSessions: a session created by another replica exists only
// in the store, so the in-memory pass cannot see it. The periodic reaper must
// still drop it once it is older than sessionTTL.
func TestReapPersistedSessions(t *testing.T) {
	repo := buildRepo(t)
	srv := New(Config{
		BasePath: "/svn",
		OpenRepo: func(name string) (*fs.Repo, error) {
			if name == "demo" {
				return repo, nil
			}
			return nil, fs.ErrRepoNotFound
		},
	})
	// Registered as open so the persisted pass scans it, without putting the
	// activity in memory.
	srv.repoMapMu.Lock()
	srv.repoCache["demo"] = repo
	srv.repoMapMu.Unlock()

	ctx := context.Background()
	key := activityStateKey(repo, "foreign-dead")
	body, _ := json.Marshal(persistedActivity{
		ID: "foreign-dead", Created: time.Now().Add(-2 * sessionTTL).Unix(),
	})
	if err := repo.Store().Put(ctx, key, body); err != nil {
		t.Fatal(err)
	}
	// A young foreign session must survive.
	youngKey := activityStateKey(repo, "foreign-live")
	young, _ := json.Marshal(persistedActivity{ID: "foreign-live", Created: time.Now().Unix()})
	if err := repo.Store().Put(ctx, youngKey, young); err != nil {
		t.Fatal(err)
	}
	srv.reapExpiredActivities()
	if _, ok, _ := repo.Store().Head(ctx, key); ok {
		t.Fatal("expired persisted session survived")
	}
	if _, ok, _ := repo.Store().Head(ctx, youngKey); !ok {
		t.Fatal("young persisted session was reaped")
	}
}
