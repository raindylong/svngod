package server

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"os"
	"strings"
	"sync"
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
//	dav-txns/activity-<id>.json            activity metadata + transaction id
//	dav-txns/changes/<id>/<sha1(path)>     one pending change (small bodies inline)
//	dav-txns/data/<sha1>                   larger bodies/deltas by reference
//
// Each PUT writes only its own change fragment, so staging a large commit is
// one object per file instead of rewriting a growing manifest (S3: fewer and
// smaller requests). Data objects are content-addressed.

type persistedChange struct {
	Path         string            `json:"path"`
	Streamed     bool              `json:"streamed,omitempty"`
	Action       string            `json:"action"`
	Kind         string            `json:"kind,omitempty"`
	DataRef      string            `json:"dataRef,omitempty"`
	TextSet      bool              `json:"textSet,omitempty"`
	Props        map[string]string `json:"props,omitempty"`
	CopyFromPath string            `json:"copyFromPath,omitempty"`
	CopyFromRev  uint64            `json:"copyFromRev,omitempty"`
	BaseRev      uint64            `json:"baseRev,omitempty"`
	DataTextRef  string            `json:"dataTextRef,omitempty"`
	DataTextSize uint64            `json:"dataTextSize,omitempty"`
	DataSHA1     string            `json:"dataSha1,omitempty"`
	DataMD5      string            `json:"dataMd5,omitempty"`
	DeltaRef     string            `json:"deltaRef,omitempty"`
	DeltaBaseRev uint64            `json:"deltaBaseRev,omitempty"`
	DeltaBaseID  uint64            `json:"deltaBaseId,omitempty"`
	DeltaTotal   uint64            `json:"deltaTotal,omitempty"`
	// Small bodies are embedded in the fragment; larger ones live under
	// dav-txns/data (DataRef/DeltaRef). Embedding avoids a second object and
	// round trip for the common small-file commit.
	DataInline    []byte `json:"dataInline,omitempty"`
	DeltaInline   []byte `json:"deltaInline,omitempty"`
	ChunkManifest []byte `json:"chunkManifest,omitempty"`
	ChunkTotal    uint64 `json:"chunkTotal,omitempty"`
}

type persistedTxn struct {
	ID      string            `json:"id"`
	BaseRev uint64            `json:"baseRev"`
	Changes []persistedChange `json:"changes"`
}

type persistedActivity struct {
	ID       string            `json:"id"`
	Created  int64             `json:"created,omitempty"`
	RevProps map[string]string `json:"revProps,omitempty"`
	Txn      *persistedTxn     `json:"txn,omitempty"`
}

// sessionTTL bounds how long an abandoned commit session (client gave up
// mid-commit) keeps its staged fragments. An active commit is far shorter;
// the next repo load reaps older ones.
const sessionTTL = 24 * time.Hour

func activityStateKey(repo *fs.Repo, id string) string {
	return repo.Keys().Prefix + "/dav-txns/activity-" + id + ".json"
}

func activityDataKey(repo *fs.Repo, ref string) string {
	return repo.Keys().Prefix + "/dav-txns/data/" + ref
}

// activityChangesPrefix is the listing prefix of one transaction's change
// fragments. It uses the listing form (no leading slash for scoped stores) so
// the keys it returns can be compared and re-read directly.
func activityChangesPrefix(repo *fs.Repo, id string) string {
	return repo.Keys().ListingPrefix("dav-txns", "changes", id)
}

// activityChangeKey is the object key of one change fragment (the store adds
// its own repository prefix).
func activityChangeKey(repo *fs.Repo, id, path string) string {
	sum := sha1.Sum([]byte(path))
	return repo.Keys().Prefix + "/dav-txns/changes/" + id + "/" + hex.EncodeToString(sum[:])
}

// fragmentInlineCap is the largest body embedded in a change fragment.
const fragmentInlineCap = 256 << 10

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

// persistActivity writes the activity-level state: revision properties and the
// transaction identity. Per-change state is stored by persistChange.
func (s *Server) persistActivity(repo *fs.Repo, actID string) {
	if repo == nil || actID == "" {
		return
	}
	s.mu.Lock()
	a, ok := s.activities[actID]
	if !ok {
		s.mu.Unlock()
		return
	}
	if a.txn == nil && len(a.revProps) == 0 {
		// Nothing durable yet (MKACTIVITY before CHECKOUT): skipping the write
		// saves an object per commit and loses only an empty session.
		s.mu.Unlock()
		return
	}
	pa := persistedActivity{ID: a.id, Created: a.created}
	if len(a.revProps) > 0 {
		pa.RevProps = copyMap(a.revProps)
	}
	if a.txn != nil {
		pa.Txn = &persistedTxn{ID: a.txn.id, BaseRev: a.txn.baseRev}
	}
	s.mu.Unlock()
	data, err := json.Marshal(pa)
	if err != nil {
		return
	}
	if err := store.PutReconstructible(context.Background(), repo.Store(), activityStateKey(repo, actID), data); err != nil {
		log.Printf("txn: persist %s: %v", actID, err)
	}
}

// snapshotChange builds the persisted form of one change. Caller holds s.mu.
func snapshotChange(path string, ch *txnChange) persistedChange {
	pc := persistedChange{
		Path: path, Action: actionName(ch.action), Kind: kindName(ch.kind),
		Props: copyMap(ch.props), CopyFromPath: ch.copyFromPath,
		CopyFromRev: ch.copyFromRev, BaseRev: ch.baseRev, TextSet: ch.textSet,
		Streamed: ch.streamed || ch.dataFile != "",
		DataRef:  ch.dataRef, DataTextRef: ch.dataTextRef, DataTextSize: ch.dataTextSize,
		DeltaRef: ch.deltaRef, DeltaTotal: ch.deltaTotal,
		DeltaBaseRev: ch.deltaBase.Rev, DeltaBaseID: ch.deltaBase.ID,
		ChunkManifest: ch.chunkManifest, ChunkTotal: ch.chunkTotal,
	}
	if ch.hasMD5 {
		pc.DataMD5 = hex.EncodeToString(ch.dataMD5[:])
	}
	if ch.hasSHA1 {
		pc.DataSHA1 = hex.EncodeToString(ch.dataSHA1[:])
	}
	return pc
}

// persistChange writes one pending change as its own fragment object. This is
// what turns staging into O(1) per PUT: the manifest is no longer rewritten,
// and small bodies need no separate data object either.
func (s *Server) persistChange(repo *fs.Repo, actID, path string) {
	if repo == nil || actID == "" {
		return
	}
	s.mu.Lock()
	a, ok := s.activities[actID]
	if !ok || a.txn == nil {
		s.mu.Unlock()
		return
	}
	ch, exists := a.txn.changes[path]
	if !exists {
		// The change was dropped inside the transaction (e.g. MOVE of a file
		// added in the same commit); its fragment must not survive.
		s.mu.Unlock()
		s.dropChangeFragment(repo, actID, path)
		return
	}
	pc := snapshotChange(path, ch)
	gen := ch.gen
	file := ch.dataFile
	var body, delta []byte
	switch {
	case ch.dataRef != "" || file != "":
		// already persisted (ref) or streamed to disk (flushed below)
	case len(ch.data) > 0:
		if len(ch.data) <= fragmentInlineCap {
			pc.DataInline = append([]byte(nil), ch.data...)
		} else {
			body = append([]byte(nil), ch.data...)
		}
	}
	if len(ch.delta) > 0 && ch.deltaRef == "" {
		if len(ch.delta) <= fragmentInlineCap {
			pc.DeltaInline = append([]byte(nil), ch.delta...)
		} else {
			delta = append([]byte(nil), ch.delta...)
		}
	}
	s.mu.Unlock()

	ctx := context.Background()
	if file != "" && pc.DataRef == "" {
		if ref, err := putTxnFile(ctx, repo, file); err == nil {
			pc.DataRef = ref
		} else {
			log.Printf("txn: persist body %s: %v", file, err)
		}
	}
	if body != nil {
		ref := hex.EncodeToString(sha1Sum(body))
		if err := store.PutReconstructible(ctx, repo.Store(), activityDataKey(repo, ref), body); err != nil {
			log.Printf("txn: persist data %s: %v", ref, err)
			return
		}
		pc.DataRef = ref
		s.mu.Lock()
		if ch.gen == gen {
			ch.dataRef = ref
		}
		s.mu.Unlock()
	}
	if delta != nil {
		ref := hex.EncodeToString(sha1Sum(delta))
		if err := store.PutReconstructible(ctx, repo.Store(), activityDataKey(repo, ref), delta); err != nil {
			log.Printf("txn: persist delta %s: %v", ref, err)
			return
		}
		pc.DeltaRef = ref
		s.mu.Lock()
		if ch.gen == gen {
			ch.deltaRef = ref
		}
		s.mu.Unlock()
	}
	data, err := json.Marshal(pc)
	if err != nil {
		return
	}
	if err := store.PutReconstructible(ctx, repo.Store(), activityChangeKey(repo, actID, path), data); err != nil {
		log.Printf("txn: persist change %s: %v", path, err)
	}
}

func sha1Sum(b []byte) []byte {
	h := sha1.Sum(b)
	return h[:]
}

// dropChangeFragment removes one change's persisted state.
func (s *Server) dropChangeFragment(repo *fs.Repo, actID, path string) {
	ctx := context.Background()
	if err := repo.Store().Delete(ctx, []string{activityChangeKey(repo, actID, path)}); err != nil {
		log.Printf("txn: drop change %s: %v", path, err)
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
	if err := store.PutFromReconstructible(ctx, repo.Store(), key, f, n); err != nil {
		return "", err
	}
	return ref, nil
}

// decodeMD5 parses a persisted hex MD5; a bad value is treated as unknown.
func decodeMD5(s string) [16]byte {
	var out [16]byte
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != 16 {
		return out
	}
	copy(out[:], b)
	return out
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

// dropActivityState removes a persisted session (commit finished or aborted):
// one bulk prefix delete for its fragments plus the activity manifest.
func (s *Server) dropActivityState(repo *fs.Repo, actID string) {
	if repo == nil || actID == "" {
		return
	}
	ctx := context.Background()
	// Remove the manifest first: once it is gone no process can resurrect the
	// transaction. The per-change fragments are garbage from that point on and
	// are reclaimed in the background -- one object per staged file, and paying
	// ~150ms per delete on the client's critical path made a 500-file commit
	// wait tens of seconds after its revision was already durable.
	if err := repo.Store().Delete(ctx, []string{activityStateKey(repo, actID)}); err != nil {
		log.Printf("txn: drop %s: %v", actID, err)
	}
	item := txnCleanupItem{repo: repo, actID: actID}
	select {
	case s.txnCleanup <- item:
	default:
		s.cleanTxnFragments(item)
	}
}

// txnCleanupItem is one transaction whose staged fragments are being reclaimed.
type txnCleanupItem struct {
	repo  *fs.Repo
	actID string
}

// cleanTxnFragments deletes the staged changes of a finished transaction on the
// authoritative replica. Standby copies are harmless (no manifest points at
// them) and are left to the repair/GC passes.
func (s *Server) cleanTxnFragments(it txnCleanupItem) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if err := store.DeletePrefixPrimary(ctx, it.repo.Store(), activityChangesPrefix(it.repo, it.actID)); err != nil {
		log.Printf("txn: drop changes %s: %v", it.actID, err)
	}
}

func (s *Server) txnCleanupLoop() {
	for it := range s.txnCleanup {
		s.cleanTxnFragments(it)
	}
}

// txnChangeFromPersisted rebuilds the in-memory change from its stored form.
func txnChangeFromPersisted(pc persistedChange) *txnChange {
	return &txnChange{
		action: actionByName(pc.Action), kind: kindByName(pc.Kind),
		props: copyMap(pc.Props), copyFromPath: pc.CopyFromPath,
		copyFromRev: pc.CopyFromRev, baseRev: pc.BaseRev, textSet: pc.TextSet, dataRef: pc.DataRef,
		data: pc.DataInline, delta: pc.DeltaInline,
		streamed: pc.Streamed, dataTextRef: pc.DataTextRef, dataTextSize: pc.DataTextSize,
		dataSHA1: decodeSHA1(pc.DataSHA1), hasSHA1: pc.DataSHA1 != "",
		dataMD5: decodeMD5(pc.DataMD5), hasMD5: pc.DataMD5 != "",
		deltaRef: pc.DeltaRef, deltaTotal: pc.DeltaTotal,
		deltaBase:     fs.NodeID{Rev: pc.DeltaBaseRev, ID: pc.DeltaBaseID},
		chunkManifest: pc.ChunkManifest, chunkTotal: pc.ChunkTotal,
	}
}

// restoreActivity loads one commit session from the shared store if this
// process does not know it yet. A commit can start on one replica (POST
// create-txn / MKACTIVITY) and continue on another after a load balancer
// switch, so unknown transaction URLs must be restored on demand rather than
// answered with 409.
func (s *Server) restoreActivity(repoName string, repo *fs.Repo, actID string) bool {
	if repo == nil || actID == "" {
		return false
	}
	if _, ok := s.activityByID(actID); ok {
		return true
	}
	data, err := repo.Store().Get(context.Background(), activityStateKey(repo, actID), nil)
	if err != nil {
		return false
	}
	var pa persistedActivity
	if err := json.Unmarshal(data, &pa); err != nil || pa.ID == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.activities[pa.ID]; ok {
		return true
	}
	a := &activity{id: pa.ID, repo: repoName, created: pa.Created, revProps: pa.RevProps}
	if a.revProps == nil {
		a.revProps = map[string]string{}
	}
	if pa.Txn != nil {
		tx := &txn{id: pa.Txn.ID, baseRev: pa.Txn.BaseRev, changes: map[string]*txnChange{}}
		for _, pc := range pa.Txn.Changes {
			tx.changes[pc.Path] = txnChangeFromPersisted(pc)
		}
		tx.changesLoaded = false
		a.txn = tx
		s.txns[tx.id] = tx
	}
	s.activities[a.id] = a
	return true
}

// loadTxnChanges reads a restored transaction's change fragments once, when a
// request actually touches that transaction. If the transaction is not in
// memory (created by another replica), it is restored first.
func (s *Server) loadTxnChanges(repo *fs.Repo, repoName, txid string) {
	if repo == nil || txid == "" {
		return
	}
	s.mu.Lock()
	tx := s.txns[txid]
	s.mu.Unlock()
	if tx == nil {
		if !s.restoreActivity(repoName, repo, strings.TrimPrefix(txid, "txn-")) {
			return
		}
		s.mu.Lock()
		tx = s.txns[txid]
		s.mu.Unlock()
	}
	if tx == nil {
		return
	}
	s.mu.Lock()
	if tx.changesLoaded {
		s.mu.Unlock()
		return
	}
	actID := strings.TrimPrefix(txid, "txn-")
	s.mu.Unlock()

	frags := loadChangeFragments(context.Background(), repo, actID)

	s.mu.Lock()
	defer s.mu.Unlock()
	tx = s.txns[txid]
	if tx == nil || tx.changesLoaded {
		return
	}
	for _, pc := range frags {
		if pc.Path == "" {
			continue
		}
		tx.changes[pc.Path] = txnChangeFromPersisted(pc)
	}
	tx.changesLoaded = true
}

// fragmentParallelism bounds concurrent fragment reads while a commit is
// assembled. A 30k-file commit used to fetch its changes one round trip at a
// time, which on object storage is tens of minutes; the fragments are
// independent immutable objects, so they are read in parallel.
const fragmentParallelism = 32

// loadChangeFragments reads every persisted change of one activity.
func loadChangeFragments(ctx context.Context, repo *fs.Repo, actID string) []persistedChange {
	prefix := activityChangesPrefix(repo, actID)
	var keys []string
	next := ""
	for {
		objs, n, err := repo.Store().ListPrefix(ctx, prefix, next, 500)
		if err != nil {
			log.Printf("txn: list changes %s: %v", actID, err)
			break
		}
		for _, o := range objs {
			keys = append(keys, o.Key)
		}
		if n == "" || n == next {
			break
		}
		next = n
	}
	out := make([]persistedChange, len(keys))
	ok := make([]bool, len(keys))
	sem := make(chan struct{}, fragmentParallelism)
	var wg sync.WaitGroup
	for i, key := range keys {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, key string) {
			defer wg.Done()
			defer func() { <-sem }()
			data, err := repo.Store().Get(ctx, key, nil)
			if err != nil {
				if err != store.ErrNotFound {
					log.Printf("txn: read change %s: %v", key, err)
				}
				return
			}
			var pc persistedChange
			if err := json.Unmarshal(data, &pc); err != nil || pc.Path == "" {
				return
			}
			out[i] = pc
			ok[i] = true
		}(i, key)
	}
	wg.Wait()
	frags := out[:0]
	for i := range out {
		if ok[i] {
			frags = append(frags, out[i])
		}
	}
	return frags
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
	prefix := repo.Keys().ListingPrefix("dav-txns")
	next := ""
	var manifests []persistedActivity
	for {
		objs, n, err := repo.Store().ListPrefix(ctx, prefix, next, 200)
		if err != nil {
			log.Printf("txn: list %q: %v", repoName, err)
			break
		}
		for _, o := range objs {
			rel := strings.TrimPrefix(o.Key, prefix)
			if strings.HasPrefix(rel, "changes/") || strings.HasPrefix(rel, "data/") {
				continue
			}
			if !strings.HasPrefix(rel, "activity-") || !strings.HasSuffix(rel, ".json") {
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

	// Reap sessions whose client gave up long ago; their staged fragments
	// would otherwise accumulate after aborted large commits. This happens
	// before any fragment is read (fragments load lazily per transaction).
	now := time.Now().Unix()
	kept := make([]persistedActivity, 0, len(manifests))
	for _, pa := range manifests {
		if pa.Created > 0 && now-pa.Created > int64(sessionTTL/time.Second) {
			log.Printf("txn: reaping expired session %s (age %ds)", pa.ID, now-pa.Created)
			s.dropActivityState(repo, pa.ID)
			continue
		}
		kept = append(kept, pa)
	}
	manifests = kept

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, pa := range manifests {
		if _, exists := s.activities[pa.ID]; exists {
			continue
		}
		a := &activity{id: pa.ID, repo: repoName, created: pa.Created, revProps: pa.RevProps}
		if a.revProps == nil {
			a.revProps = map[string]string{}
		}
		if pa.Txn != nil {
			tx := &txn{id: pa.Txn.ID, baseRev: pa.Txn.BaseRev, changes: map[string]*txnChange{}}
			for _, pc := range pa.Txn.Changes {
				tx.changes[pc.Path] = txnChangeFromPersisted(pc)
			}
			// Fragments are loaded on first use: a large abandoned session
			// must not delay startup by thousands of object reads.
			tx.changesLoaded = false
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

// reapExpiredActivities drops commit sessions whose client gave up (or
// crashed) more than sessionTTL ago. Startup already reaps them; this keeps a
// long-running server from accumulating in-memory transactions and staged
// store objects for the lifetime of the process.
func (s *Server) reapExpiredActivities() {
	now := time.Now().Unix()
	var dead []*activity
	s.mu.Lock()
	for id, a := range s.activities {
		if a.created <= 0 || now-a.created <= int64(sessionTTL/time.Second) {
			continue
		}
		delete(s.activities, id)
		if a.txn != nil {
			delete(s.txns, a.txn.id)
		}
		dead = append(dead, a)
	}
	s.mu.Unlock()
	for _, a := range dead {
		log.Printf("txn: reaping expired session %s (repo %s)", a.id, a.repo)
		if a.repo == "" {
			continue
		}
		repo, err := s.cfg.OpenRepo(a.repo)
		if err != nil {
			log.Printf("txn: reap %s: open %s: %v", a.id, a.repo, err)
			continue
		}
		s.dropActivityState(repo, a.id)
	}
	// Sessions created by another replica are only in the store; they never
	// enter this process's maps, so the in-memory pass above cannot see them.
	s.reapPersistedSessions()
}

// reapPersistedSessions scans the repositories this process has open and drops
// stored commit sessions older than sessionTTL that were never restored here.
func (s *Server) reapPersistedSessions() {
	s.repoMapMu.Lock()
	repos := make(map[string]*fs.Repo, len(s.repoCache))
	for name, repo := range s.repoCache {
		repos[name] = repo
	}
	s.repoMapMu.Unlock()
	now := time.Now().Unix()
	for name, repo := range repos {
		if repo == nil {
			continue
		}
		ctx := context.Background()
		prefix := repo.Keys().ListingPrefix("dav-txns")
		next := ""
		for {
			objs, cont, err := repo.Store().ListPrefix(ctx, prefix, next, 200)
			if err != nil {
				log.Printf("txn: reap %s: list: %v", name, err)
				break
			}
			for _, o := range objs {
				rel := strings.TrimPrefix(o.Key, prefix)
				if !strings.HasPrefix(rel, "activity-") || !strings.HasSuffix(rel, ".json") {
					continue
				}
				data, err := repo.Store().Get(ctx, o.Key, nil)
				if err != nil {
					continue
				}
				var pa persistedActivity
				if json.Unmarshal(data, &pa) != nil || pa.ID == "" {
					continue
				}
				if pa.Created <= 0 || now-pa.Created <= int64(sessionTTL/time.Second) {
					continue
				}
				if _, ok := s.activityByID(pa.ID); ok {
					continue // the in-memory pass already dropped it
				}
				log.Printf("txn: reaping persisted session %s (repo %s)", pa.ID, name)
				s.dropActivityState(repo, pa.ID)
			}
			if cont == "" || cont == next {
				break
			}
			next = cont
		}
	}
}

// RunMaintenance periodically reaps abandoned commit sessions. Call it once
// (typically from main) with a process-lifetime context.
func (s *Server) RunMaintenance(ctx context.Context, every time.Duration) {
	if every <= 0 {
		every = 10 * time.Minute
	}
	log.Printf("maintenance: reaping abandoned commit sessions every %s", every)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.reapExpiredActivities()
		}
	}
}
