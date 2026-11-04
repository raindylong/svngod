package fs

import (
	"log"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

// Online commits are grouped into batches by a per-repository coordinator.
// Requests that arrive while a batch is being built wait for the same lease
// and HEAD CAS instead of each paying six S3 round trips. Off by default:
// operators enable it with SVNGOD_COMMIT_BATCH=<size> (and optionally
// SVNGOD_COMMIT_WINDOW_MS). A single commit still goes through the normal
// path, so enabling it costs one short collection window per commit.
var (
	commitBatchN atomic.Int64
	commitBatchW atomic.Int64
)

func init() {
	if v := os.Getenv("SVNGOD_COMMIT_BATCH"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 1 {
			commitBatchN.Store(int64(n))
		}
	}
	ms := 2
	if v := os.Getenv("SVNGOD_COMMIT_WINDOW_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			ms = n
		}
	}
	commitBatchW.Store(int64(time.Duration(ms) * time.Millisecond))
}

// ConfigureCommitBatch enables (size > 1) or disables online batching. windowMs
// bounds how long the coordinator waits for more requests once it has one.
func ConfigureCommitBatch(size, windowMs int) {
	if size <= 1 {
		size = 0
	}
	if windowMs < 0 {
		windowMs = 0
	}
	commitBatchN.Store(int64(size))
	commitBatchW.Store(int64(time.Duration(windowMs) * time.Millisecond))
}

func commitBatchEnabled() bool { return commitBatchN.Load() > 1 }

type batchReq struct {
	baseRev uint64
	changes []Change
	props   map[string]string
	res     chan batchRes
}

type batchRes struct {
	rev uint64
	err error
}

type commitBatcher struct {
	repo *Repo
	ch   chan *batchReq
}

func newCommitBatcher(r *Repo) *commitBatcher {
	b := &commitBatcher{repo: r, ch: make(chan *batchReq, 1024)}
	go b.loop()
	return b
}

// submit enqueues a commit and waits for the batch to publish it. If the
// queue is full (a pathological burst), the request falls back to the direct
// path instead of blocking for an unbounded time.
func (b *commitBatcher) submit(baseRev uint64, changes []Change, props map[string]string) (uint64, error) {
	req := &batchReq{baseRev: baseRev, changes: changes, props: props, res: make(chan batchRes, 1)}
	select {
	case b.ch <- req:
	case <-time.After(100 * time.Millisecond):
		return b.repo.commitTxn(baseRev, changes, props)
	}
	res := <-req.res
	return res.rev, res.err
}

func (b *commitBatcher) loop() {
	for {
		first := <-b.ch
		reqs := []*batchReq{first}
		size := int(commitBatchN.Load())
		timer := time.NewTimer(time.Duration(commitBatchW.Load()))
	collect:
		for len(reqs) < size {
			select {
			case q := <-b.ch:
				reqs = append(reqs, q)
			case <-timer.C:
				break collect
			}
		}
		timer.Stop()
		b.run(reqs)
	}
}

func (b *commitBatcher) run(reqs []*batchReq) {
	if debugCommit {
		log.Printf("online batch: %d commits", len(reqs))
	}
	commits := make([]BatchCommit, len(reqs))
	for i, q := range reqs {
		commits[i] = BatchCommit{BaseRev: q.baseRev, Changes: q.changes, RevProps: q.props}
	}
	revs, errs, err := b.repo.CommitBatch(commits)
	for i, q := range reqs {
		e := err
		if e == nil {
			e = errs[i]
		}
		q.res <- batchRes{rev: revs[i], err: e}
	}
}
