package server

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// HTTPv2 transaction channel (enabled with SVNGOD_HTTPV2=1).
//
// mod_dav_svn advertises v2 through response headers, not the body:
//
//	SVN-Repository-Root: /svn/repo
//	SVN-Supported-Posts: create-txn
//	SVN-Supported-Posts: create-txn-with-props
//
// The client then POSTs !svn/me <S:create-txn/>, uploads to the returned txn
// URL with the existing PUT/MKCOL handlers, and finalizes the commit. The
// transaction id is the activity id, so persistence, restart recovery and the
// MERGE finalize path are shared with v1.
func httpV2Enabled() bool { return os.Getenv("SVNGOD_HTTPV2") == "1" }

func v2UUID() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func (s *Server) handlePostMe(w http.ResponseWriter, r *http.Request) {
	if !httpV2Enabled() {
		http.Error(w, "HTTPv2 disabled", http.StatusNotImplemented)
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, strings.TrimSuffix(s.cfg.BasePath, "/")+"/")
	name := strings.SplitN(rest, "/", 2)[0]
	if name == "" {
		http.Error(w, "repository missing", http.StatusBadRequest)
		return
	}
	repo, ok := s.repo(w, resource{repo: name})
	if !ok {
		return
	}
	if _, ok := s.authenticate(w, r, name); !ok {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var root xmlEl
	if xml.Unmarshal(body, &root) != nil {
		http.Error(w, "bad POST body", http.StatusBadRequest)
		return
	}
	switch root.XMLName.Local {
	case "create-txn", "create-txn-with-props":
		head, herr := repo.HeadFresh()
		if herr != nil {
			http.Error(w, herr.Error(), http.StatusInternalServerError)
			return
		}
		uuid := v2UUID()
		act := &activity{id: uuid, created: time.Now().Unix(), revProps: map[string]string{},
			txn: &txn{id: "txn-" + uuid, baseRev: head, changes: map[string]*txnChange{}, changesLoaded: true}}
		for _, c := range root.Children {
			if c.XMLName.Local == "rev-prop" {
				if pn := c.attr("name"); pn != "" {
					act.revProps[pn] = strings.TrimSpace(c.Content)
				}
			}
		}
		s.mu.Lock()
		s.activities[uuid] = act
		s.txns[act.txn.id] = act.txn
		s.mu.Unlock()
		s.persistActivity(repo, uuid)
		w.Header().Set("Location", s.absURL(r, name, "/!svn/txn/"+act.txn.id))
		w.Header().Set("SVN-Txn-Name", act.txn.id)
		w.WriteHeader(http.StatusCreated)
	case "commit-txn":
		txnName := ""
		if c, ok := root.child("txn-name"); ok {
			txnName = strings.TrimSpace(c.Content)
		}
		actID := strings.TrimPrefix(txnName, "txn-")
		if actID == "" || actID == txnName {
			http.Error(w, "commit-txn requires txn-name", http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		_, exists := s.activities[actID]
		s.mu.Unlock()
		if !exists {
			http.Error(w, "transaction not found", http.StatusConflict)
			return
		}
		// Finalize through MERGE: hooks, authz, per-path OOD and the atomic
		// CommitTxn stay in one place.
		mergeBody := `<?xml version="1.0"?><D:merge xmlns:D="DAV:"><D:source><D:href>` +
			s.absURL(r, name, "/!svn/act/"+actID) + `</D:href></D:source></D:merge>`
		mr := r.Clone(r.Context())
		mr.Method = MethodMerge
		mr.Body = io.NopCloser(strings.NewReader(mergeBody))
		mr.ContentLength = int64(len(mergeBody))
		mr.URL.Path = strings.TrimSuffix(s.cfg.BasePath, "/") + "/" + name + "/"
		s.handleMerge(w, mr, resource{typ: resRoot, repo: name})
	default:
		http.Error(w, "unsupported POST: "+root.XMLName.Local, http.StatusNotImplemented)
	}
}
