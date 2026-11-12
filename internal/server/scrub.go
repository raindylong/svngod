package server

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"

	"svngod/internal/fs"
)

// Background scrub: periodically run the on-demand Verifier over revision
// ranges that have not been checked yet, so silent corruption (bit rot,
// partial mirror writes, index damage) surfaces as a metric instead of as a
// client-visible read failure months later.
//
// Design notes:
//   - Incremental: each repo remembers the highest verified revision; a tick
//     verifies only [last+1, min(head, last+chunk)] so a 10M-revision repo is
//     scrubbed in bounded slices, never one unbounded walk.
//   - Bounded work per tick (scrubChunk revisions) keeps the fullReads mode
//     Verify enters short-lived; reads only get slower (whole-object reads),
//     never incorrect.
//   - Head may advance while scrubbing: the next tick simply continues from
//     the recorded watermark, so no revision is skipped.
//   - Corruption is counted per repo (svngod_scrub_corruptions_total) and
//     logged; repair stays a manual svnadmin decision on purpose.

var scrubChunk = uint64(2000) // revisions per repo per tick

// RunScrub starts the background integrity scrub. Call it once (typically
// from main) with a process-lifetime context; it stops with the context.
func (s *Server) RunScrub(ctx context.Context, every time.Duration) {
	if every <= 0 {
		every = 10 * time.Minute
	}
	log.Printf("scrub: background integrity check every %s (chunk %d revisions per repo)", every, scrubChunk)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.scrubOnce(ctx)
		}
	}
}

// scrubOnce advances the watermark of every cached repository by at most one
// chunk. It is safe to run concurrently with traffic; VerifyRange checks
// historical data and never mutates.
type scrubState struct {
	mu       sync.Mutex
	verified map[string]uint64 // repo name -> highest verified revision
}

func (s *Server) scrubOnce(ctx context.Context) {
	s.repoMapMu.Lock()
	repos := make(map[string]*fs.Repo, len(s.repoCache))
	for k, v := range s.repoCache {
		repos[k] = v
	}
	s.repoMapMu.Unlock()
	names := make([]string, 0, len(repos))
	for n := range repos {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		if ctx.Err() != nil {
			return
		}
		s.scrubRepo(ctx, name, repos[name])
	}
}

// scrubRepo advances one repository's watermark by at most scrubChunk
// revisions, counting and logging any corruption found.
func (s *Server) scrubRepo(ctx context.Context, name string, repo *fs.Repo) {
	head, err := repo.Head()
	if err != nil {
		return // unreadable now; try again next tick
	}
	s.scrubMu.Lock()
	last := s.scrub.verified[name]
	s.scrubMu.Unlock()
	if last >= head {
		return
	}
	from := last + 1
	to := from - 1 + scrubChunk
	if to > head {
		to = head
	}
	issues, err := repo.VerifyRangeWith(from, to, true, 2)
	if err != nil {
		// Verify itself failed (e.g. the backend is unreachable): leave the
		// watermark where it was so the range is retried next tick.
		log.Printf("scrub: %s [%d..%d] aborted: %v", name, from, to, err)
		return
	}
	if len(issues) > 0 {
		s.m.scrubIssuesAdd(name, len(issues))
		for _, iss := range issues {
			log.Printf("scrub: corruption in %s: rev=%d path=%q err=%v rebuildable=%v",
				name, iss.Rev, iss.Path, iss.Err, iss.Rebuildable)
		}
	}
	s.scrubMu.Lock()
	if s.scrub.verified == nil {
		s.scrub.verified = map[string]uint64{}
	}
	s.scrub.verified[name] = to
	s.scrubMu.Unlock()
	log.Printf("scrub: %s verified revisions %d..%d clean=%v", name, from, to, len(issues) == 0)
}
