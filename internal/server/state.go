package server

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"os"
	"time"

	"svngod/internal/fs"
	"svngod/internal/fs/store"
)

// Commit sessions (DeltaV activities and their checked-out transactions) live
// in memory while a commit is in flight. Persisting them lets a restart or a
// second replica continue the same commit instead of answering 409 and losing
// the client's uploaded data.
//
// Layout (per repository):
//
//	dav-txns/activity-<id>.json   activity + transaction manifest
//	dav-txns/data/<sha1>          file bodies referenced by the manifest
//
// Data objects are content-addressed, so identical PUTs share one object.

type persistedChange struct {
	Path         string            `json:"path"`
	Streamed     bool              `json:"streamed,omitempty"`
	Action       string            `json:"action"`
	Kind         string            `json:"kind,omitempty"`
	DataRef      string            `json:"dataRef,omitempty"`
	Props        map[string]string `json:"props,omitempty"`
	CopyFromPath string            `json:"copyFromPath,omitempty"`
	CopyFromRev  uint64            `json:"copyFromRev,omitempty"`
	BaseRev      uint64            `json:"baseRev,omitempty"`
	DataTextRef  string            `json:"dataTextRef,omitempty"`
	DataTextSize uint64            `json:"dataTextSize,omitempty"`
	DataSHA1     string            `json:"dataSha1,omitempty"`
}

type persistedTxn struct {
	ID      string            `json:"id"`
	BaseRev uint64            `json:"baseRev"`
	Changes []persistedChange `json:"changes"`
}

type persistedActivity struct {
	ID       string            `json:"id"`
	RevProps map[string]string `json:"revProps,omitempty"`
	Txn      *persistedTxn     `json:"txn,omitempty"`
}

func activityStateKey(repo *fs.Repo, id string) string {
	return repo.Keys().Prefix + "/dav-txns/activity-" + id + ".json"
}

func activityDataKey(repo *fs.Repo, ref string) string {
	return repo.Keys().Prefix + "/dav-txns/data/" + ref
}

func actionName(a fs.ChangeAction) string {
	switch a {
	case fs.ActionAdd:
		return "add"
	case fs.ActionModify:
		return "modify"
	case fs.ActionDelete:
		return "delete"
	case fs.ActionCopy:
		return "copy"
	}
	return "add"
}

func actionByName(s string) fs.ChangeAction {
	switch s {
	case "modify":
		return fs.ActionModify
	case "delete":
		return fs.ActionDelete
	case "copy":
		return fs.ActionCopy
	}
	return fs.ActionAdd
}

func kindName(k byte) string {
	if k == fs.KindDir {
		return "dir"
	}
	if k == fs.KindFile {
		return "file"
	}
	return ""
}

func kindByName(s string) byte {
	switch s {
	case "dir":
		return fs.KindDir
	case "file":
		return fs.KindFile
	}
	return 0
}

// persistActivity snapshots one commit session and writes it to the repo
// store. Best effort: a failing write must not fail the request.
func (s *Server) persistActivity(repo *fs.Repo, actID string) {
	s.mu.Lock()
	a, ok := s.activities[actID]
	if !ok {
		s.mu.Unlock()
		return
	}
	pa := persistedActivity{ID: a.id}
	if len(a.revProps) > 0 {
		pa.RevProps = map[string]string{}
		for k, v := range a.revProps {
			pa.RevProps[k] = v
		}
	}
	type fileBody struct {
		index int
		path  string
	}
	var files []fileBody
	if a.txn != nil {
		pt := &persistedTxn{ID: a.txn.id, BaseRev: a.txn.baseRev}
		for p, ch := range a.txn.changes {
			pc := persistedChange{
				Path: p, Action: actionName(ch.action), Kind: kindName(ch.kind),
				Props: copyMap(ch.props), CopyFromPath: ch.copyFromPath,
				CopyFromRev: ch.copyFromRev, BaseRev: ch.baseRev,
			}
			switch {
			case ch.dataTextRef != "":
				// The body is already a durable text object; only the reference
				// needs to be persisted.
				pc.DataTextRef = ch.dataTextRef
				pc.DataTextSize = ch.dataTextSize
				if ch.hasSHA1 {
					pc.DataSHA1 = hex.EncodeToString(ch.dataSHA1[:])
				}
			case ch.dataFile != "":
				pc.Streamed = true
				files = append(files, fileBody{index: len(pt.Changes), path: ch.dataFile})
			case len(ch.data) > 0:
				sum := sha1.Sum(ch.data)
				pc.DataRef = hex.EncodeToString(sum[:])
			}
			pt.Changes = append(pt.Changes, pc)
		}
		pa.Txn = pt
	}
	// Copy payloads while holding the lock so a concurrent PUT cannot mutate
	// the slice under us.
	payloads := map[string][]byte{}
	if a.txn != nil {
		for _, ch := range a.txn.changes {
			if len(ch.data) > 0 {
				sum := sha1.Sum(ch.data)
				payloads[hex.EncodeToString(sum[:])] = append([]byte(nil), ch.data...)
			}
		}
	}
	s.mu.Unlock()

	ctx := context.Background()
	// Bodies on disk are hashed and uploaded (streaming) so a restart can
	// resume the commit without the temp file.
	for _, fb := range files {
		ref, err := putTxnFile(ctx, repo, fb.path)
		if err != nil {
			log.Printf("txn: persist body %s: %v", fb.path, err)
			continue
		}
		pa.Txn.Changes[fb.index].DataRef = ref
	}
	for ref, data := range payloads {
		if _, exists, err := repo.Store().Head(ctx, activityDataKey(repo, ref)); err == nil && exists {
			continue
		}
		if err := repo.Store().Put(ctx, activityDataKey(repo, ref), data); err != nil {
			log.Printf("txn: persist data %s: %v", ref, err)
		}
	}
	if data, err := json.Marshal(pa); err == nil {
		if err := repo.Store().Put(ctx, activityStateKey(repo, actID), data); err != nil {
			log.Printf("txn: persist %s: %v", actID, err)
		}
	}
}

// putTxnFile streams a local body into the transaction store and returns its
// content reference.
func putTxnFile(ctx context.Context, repo *fs.Repo, path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha1.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", err
	}
	ref := hex.EncodeToString(h.Sum(nil))
	key := activityDataKey(repo, ref)
	if _, ok, herr := repo.Store().Head(ctx, key); herr == nil && ok {
		return ref, nil
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	if err := repo.Store().PutFrom(ctx, key, f, n); err != nil {
		return "", err
	}
	return ref, nil
}

// decodeSHA1 parses a persisted hex SHA-1; a bad value is treated as unknown.
func decodeSHA1(s string) [20]byte {
	var out [20]byte
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != 20 {
		return out
	}
	copy(out[:], b)
	return out
}

// dropActivityState removes a persisted session (commit finished or aborted).
// Data objects are content addressed and left for a future GC.
func (s *Server) dropActivityState(repo *fs.Repo, actID string) {
	if repo == nil || actID == "" {
		return
	}
	if err := repo.Store().Delete(context.Background(), []string{activityStateKey(repo, actID)}); err != nil {
		log.Printf("txn: drop %s: %v", actID, err)
	}
}

// ensureActivitiesLoaded restores persisted commit sessions for a repository.
// Called once per repository (and process).
func (s *Server) ensureActivitiesLoaded(repoName string, repo *fs.Repo) {
	s.mu.Lock()
	if s.stateLoaded[repoName] {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	ctx := context.Background()
	prefix := repo.Keys().Prefix + "/dav-txns/"
	next := ""
	var manifests []persistedActivity
	for {
		objs, n, err := repo.Store().ListPrefix(ctx, prefix, next, 200)
		if err != nil {
			log.Printf("txn: list %q: %v", repoName, err)
			break
		}
		for _, o := range objs {
			if len(o.Key) < len("activity-") || o.Key[len(o.Key)-5:] != ".json" {
				continue
			}
			data, err := repo.Store().Get(ctx, o.Key, nil)
			if err != nil {
				if err != store.ErrNotFound {
					log.Printf("txn: read %s: %v", o.Key, err)
				}
				continue
			}
			var pa persistedActivity
			if err := json.Unmarshal(data, &pa); err != nil {
				log.Printf("txn: parse %s: %v", o.Key, err)
				continue
			}
			manifests = append(manifests, pa)
		}
		if n == "" {
			break
		}
		if n == next {
			log.Printf("txn: list %q: cursor did not advance, stopping", repoName)
			break
		}
		next = n
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, pa := range manifests {
		if _, exists := s.activities[pa.ID]; exists {
			continue
		}
		a := &activity{id: pa.ID, revProps: pa.RevProps}
		if a.revProps == nil {
			a.revProps = map[string]string{}
		}
		if pa.Txn != nil {
			tx := &txn{id: pa.Txn.ID, baseRev: pa.Txn.BaseRev, changes: map[string]*txnChange{}}
			for _, pc := range pa.Txn.Changes {
				tx.changes[pc.Path] = &txnChange{
					action: actionByName(pc.Action), kind: kindByName(pc.Kind),
					props: copyMap(pc.Props), copyFromPath: pc.CopyFromPath,
					copyFromRev: pc.CopyFromRev, baseRev: pc.BaseRev, dataRef: pc.DataRef,
					streamed: pc.Streamed, dataTextRef: pc.DataTextRef, dataTextSize: pc.DataTextSize,
					dataSHA1: decodeSHA1(pc.DataSHA1), hasSHA1: pc.DataSHA1 != "",
				}
			}
			a.txn = tx
			s.txns[tx.id] = tx
		}
		s.activities[a.id] = a
	}
	s.stateLoaded[repoName] = true
}

// loadChangeData materializes a change's body from its content-addressed object
// when the session was restored from the store.
func loadChangeData(repo *fs.Repo, ch *txnChange) {
	if len(ch.data) > 0 || ch.dataRef == "" {
		return
	}
	data, err := repo.Store().Get(context.Background(), activityDataKey(repo, ch.dataRef), nil)
	if err != nil {
		log.Printf("txn: load data %s: %v", ch.dataRef, err)
		return
	}
	ch.data = data
}

var _ = time.Now
