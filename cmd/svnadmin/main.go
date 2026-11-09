// Command svnadmin is the migration/administration entry point for svngod.
//
// Usage:
//
//	svnadmin create [--endpoint H:PORT --access-key K --secret-key S --bucket B [--prefix P]] <repo>
//	svnadmin dump   [--output FILE] <repo>
//	svnadmin load   [--input FILE] <repo>
//
// Flags come before the repository name. Without S3 flags the repo lives in a
// local directory at <repo>. With an
// endpoint the repo lives in the S3-compatible object store (e.g. RustFS)
// under --prefix (default the repo name).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"svngod/internal/cdc"
	"svngod/internal/dump"
	"svngod/internal/fs"
	"svngod/internal/fs/store"
)

const (
	defaultShardSize = 4096
	defaultMaxDelta  = 64
)

type options struct {
	endpoint        string
	accessKey       string
	secretKey       string
	bucket          string
	prefix          string
	useSSL          bool
	mirrorFile      string
	allRevs         bool
	texts           bool
	fromRev         uint64
	toRev           uint64
	journalLimit    int
	keepJournal     uint64
	gcGrace         int
	chunkAvg        uint64
	dirBuckets      bool
	deleteWorkers   int
	deleteDryRun    bool
	deleteForce     bool
	deleteNoSweep   bool
	deleteHead      uint64
	deleteBatch     int
	deleteControl   bool

	migrateDirs     string
	migrateChunkAvg uint64
}

// mirrorEntry is one backend in a --mirror-file JSON array (same shape as the
// svngod config "mirrors" list): {"type":"file","root":...} or
// {"type":"s3","endpoint":...,"access":...,"secret":...,"bucket":...,"ssl":...}.
type mirrorEntry struct {
	Type     string `json:"type"`
	Root     string `json:"root"`
	Endpoint string `json:"endpoint"`
	Access   string `json:"access"`
	Secret   string `json:"secret"`
	Bucket   string `json:"bucket"`
	SSL      bool   `json:"ssl"`
}

func (o *options) repoStore(localPath string) (store.Store, string, error) {
	name := filepath.Base(localPath)
	if o.mirrorFile != "" {
		return o.mirrorStore(name)
	}
	if o.endpoint == "" {
		// localPath is <parent>/<repoName>; base is the parent directory so the
		// same store layout is shared with the server (--repo-root <parent>).
		st, err := store.NewFileStore(filepath.Dir(localPath))
		if err != nil {
			return nil, "", err
		}
		return st, "repos/" + name, nil
	}
	// --prefix is the bucket-level public prefix (default "repos"). The S3
	// store's own key prefix becomes <public>/<name>, and CreateRepo is called
	// with repo-root-relative keys (""), so created objects land exactly where
	// a config entry with storage.prefix=<public> and repo name <name> expects.
	root := o.prefix
	if root == "" {
		root = "repos"
	}
	st, err := store.NewS3Store(store.S3Config{
		Endpoint:   o.endpoint,
		AccessKey:  o.accessKey,
		SecretKey:  o.secretKey,
		Bucket:     o.bucket,
		Region:     "us-east-1",
		UseSSL:     o.useSSL,
		RepoPrefix: root,
		BaseRepo:   name,
	})
	if err != nil {
		return nil, "", err
	}
	return st, "", nil
}

// mirrorStore builds a MirrorStore from a --mirror-file JSON array so the repo
// can be created once across a local-primary + S3-standby (or any mix) set.
func (o *options) mirrorStore(name string) (store.Store, string, error) {
	data, err := os.ReadFile(o.mirrorFile)
	if err != nil {
		return nil, "", err
	}
	var entries []mirrorEntry
	var quorum int
	repoPrefix := "repos"
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) > 0 && trimmed[0] == '{' {
		// Object form: {"mirrors":[...],"quorum":2}
		var obj struct {
			Mirrors []mirrorEntry `json:"mirrors"`
			Quorum  int           `json:"quorum"`
			Prefix  string        `json:"prefix"`
		}
		if err := json.Unmarshal(trimmed, &obj); err != nil {
			return nil, "", fmt.Errorf("parse --mirror-file: %v", err)
		}
		entries, quorum = obj.Mirrors, obj.Quorum
		if obj.Prefix != "" {
			repoPrefix = obj.Prefix
		}
	} else if err := json.Unmarshal(data, &entries); err != nil {
		return nil, "", fmt.Errorf("parse --mirror-file: %v", err)
	}
	ms := make([]store.Store, 0, len(entries))
	for _, e := range entries {
		if e.Type == "s3" {
			st, err := store.NewS3Store(store.S3Config{
				Endpoint:  e.Endpoint,
				AccessKey: e.Access,
				SecretKey: e.Secret,
				Bucket:    e.Bucket,
				Region:    "us-east-1",
				UseSSL:    e.SSL,
			})
			if err != nil {
				return nil, "", err
			}
			ms = append(ms, st)
		} else {
			st, err := store.NewFileStore(e.Root)
			if err != nil {
				return nil, "", err
			}
			ms = append(ms, st)
		}
	}
	if len(ms) < 2 {
		return nil, "", fmt.Errorf("--mirror-file requires at least 2 backends (got %d)", len(ms))
	}
	prefix := strings.Trim(repoPrefix, "/") + "/" + name
	// Lazy alignment: admin commands (gc, repair, dump) must not block on a
	// full repository listing at construction; doRepair asks for an explicit
	// pass and the others only touch known keys.
	opts := []store.MirrorOption{store.WithMutableOrder(fs.CompareMutable), store.WithRepoPrefix(prefix), store.WithLazyRepair()}
	if quorum > 0 {
		opts = append(opts, store.WithWriteQuorum(quorum))
	}
	return store.NewMirror(ms, opts...), prefix, nil
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	rest := os.Args[2:]

	var o options
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	fs.StringVar(&o.endpoint, "endpoint", "", "S3 endpoint host:port (empty = local dir)")
	fs.StringVar(&o.accessKey, "access-key", "", "S3 access key")
	fs.StringVar(&o.secretKey, "secret-key", "", "S3 secret key")
	fs.StringVar(&o.bucket, "bucket", "", "S3 bucket")
	fs.StringVar(&o.prefix, "prefix", "", "S3 repo prefix")
	fs.BoolVar(&o.useSSL, "ssl", false, "use https for S3")
	fs.StringVar(&o.mirrorFile, "mirror-file", "", "JSON array of backends to create a mirror store (local-primary + S3-standby etc.)")
	var output, input string
	if cmd == "migrate" {
		fs.StringVar(&o.migrateDirs, "dir-buckets", "", "on|off: page large directories (leave empty to keep)")
		fs.Uint64Var(&o.migrateChunkAvg, "chunk-avg", 0, "new target average chunk size in bytes (0 keeps)")
	}
	if cmd == "delete-repo" {
		fs.IntVar(&o.deleteWorkers, "workers", 16, "concurrent delete batches")
		fs.BoolVar(&o.deleteDryRun, "dry-run", false, "count objects without deleting")
		fs.BoolVar(&o.deleteForce, "force", false, "required: deletes every object of the repository")
		fs.BoolVar(&o.deleteNoSweep, "no-sweep", false, "skip the content-addressed prefix sweep (repositories with only inline texts)")
		fs.Uint64Var(&o.deleteHead, "head", 0, "use this HEAD when the repository metadata is already gone")
		fs.IntVar(&o.deleteBatch, "batch", 1000, "keys per DELETE request (smaller batches are more reliable on some backends)")
		fs.BoolVar(&o.deleteControl, "control-only", false, "delete only the repository's control keys (removes it from service; payload objects remain)")
	}
	if cmd == "verify" {
		fs.BoolVar(&o.allRevs, "all-revs", false, "verify every historical revision, not just HEAD")
		fs.BoolVar(&o.texts, "texts", false, "with --all-revs: read and hash every file text (slow)")
		fs.Uint64Var(&o.fromRev, "from", 1, "first revision for --all-revs")
		fs.Uint64Var(&o.toRev, "to", 0, "last revision for --all-revs (0 = HEAD)")
	}
	if cmd == "journal" {
		fs.IntVar(&o.journalLimit, "n", 20, "number of records to show")
	}
	if cmd == "gc" {
		fs.Uint64Var(&o.keepJournal, "keep-journal", 4096, "number of recent WAL records to keep")
		fs.IntVar(&o.gcGrace, "gc-grace", 3600, "seconds an unreferenced text/chunk must be old before collection (0 disables)")
	}
	shardSize := uint64(defaultShardSize)
	maxDelta := uint64(defaultMaxDelta)
	if cmd == "create" {
		fs.Uint64Var(&shardSize, "shard-size", defaultShardSize, "revisions per shard (packing granularity)")
		fs.Uint64Var(&maxDelta, "max-delta", defaultMaxDelta, "maximum text delta chain length")
		fs.Uint64Var(&o.chunkAvg, "chunk-avg", 1<<20, "target average chunk size in bytes (0 keeps 1 MiB)")
		fs.BoolVar(&o.dirBuckets, "dir-buckets", false, "page directories with more than 1024 children into 16 buckets")
	}
	if cmd == "dump" {
		fs.StringVar(&output, "output", "", "write dump to file (default stdout)")
	}
	if cmd == "load" {
		fs.StringVar(&input, "input", "", "read dump from file (default stdin)")
	}
	if err := fs.Parse(rest); err != nil {
		os.Exit(2)
	}

	if fs.NArg() != 1 {
		usage()
		os.Exit(2)
	}
	repoName := fs.Arg(0)

	switch cmd {
	case "create":
		doCreate(&o, repoName, shardSize, maxDelta)
	case "dump":
		doDump(&o, repoName, output)
	case "load":
		doLoad(&o, repoName, input)
	case "verify":
		doVerify(&o, repoName)
	case "journal":
		doJournal(&o, repoName)
	case "info":
		doInfo(&o, repoName)
	case "repair":
		doRepair(&o, repoName)
	case "delete-repo":
		doDeleteRepo(&o, repoName)
	case "migrate":
		doMigrate(&o, repoName)
	case "gc":
		doGC(&o, repoName)
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `svnadmin - svngod repository administration

usage:
  svnadmin create [s3 flags | --mirror-file FILE] <repo>
  svnadmin dump   [--output FILE] [s3 flags] <repo>
  svnadmin load   [--input FILE] [s3 flags] <repo>
  svnadmin verify [s3 flags] <repo>   # integrity walk; prints corrupted nodes
  svnadmin repair [--mirror-file FILE] <repo>  # synchronously reconcile mirrors
  svnadmin delete-repo -force [s3 flags | --mirror-file FILE] <repo>  # delete every object of a repository
  svnadmin journal [-n N] [s3 flags | --mirror-file FILE] <repo>  # show WAL records
  svnadmin gc     [s3 flags] <repo>   # loose objects, txn bodies, unused texts

s3 flags: --endpoint H:PORT --access-key K --secret-key S --bucket B [--prefix P] [--ssl]
`)
}

func doCreate(o *options, repoName string, shardSize, maxDelta uint64) {
	st, prefix, err := o.repoStore(repoName)
	if err != nil {
		fatal(err)
	}
	repo, err := fs.CreateRepoFull(st, prefix, shardSize, maxDelta, chunkParamsFor(o.chunkAvg), o.dirBuckets)
	if err != nil {
		fatal(err)
	}
	uuid, err := repo.UUID()
	if err != nil {
		fatal(err)
	}
	fmt.Printf("created repository %q uuid=%s\n", repoName, uuid)
}

// chunkParamsFor derives min/max/mask from a target average chunk size.
func chunkParamsFor(avg uint64) cdc.Params {
	if avg == 0 {
		return cdc.DefaultParams()
	}
	bits := 1
	for (uint64(1) << bits) < avg {
		bits++
	}
	mask := (uint64(1) << bits) - 1
	min := avg / 4
	if min < 64<<10 {
		min = 64 << 10
	}
	max := avg * 4
	if max > cdc.AbsoluteMax {
		max = cdc.AbsoluteMax
	}
	return cdc.Params{Min: int(min), Max: int(max), Mask: mask}.Normalize()
}

// doInfo prints the repository format and head without modifying anything.
// doRepair runs one synchronous mirror reconciliation pass. A mirror that
// missed writes while it was unreachable is normally healed by the server's
// background repair; after a bulk load or a process crash that never got to
// run it, this command copies the delta on demand.
func doRepair(o *options, repoName string) {
	st, prefix, err := o.repoStore(repoName)
	if err != nil {
		fatal(err)
	}
	r, err := fs.OpenRepo(st, prefix)
	if err != nil {
		fatal(err)
	}
	if rp, ok := st.(interface{ RepairNow() }); ok {
		rp.RepairNow()
	} else {
		fmt.Println("store is not mirrored; nothing to repair")
	}
	head, err := r.HeadFresh()
	if err != nil {
		fatal(err)
	}
	fmt.Printf("repaired %s head=%d\n", repoName, head)
}

// doDeleteRepo removes every object of a repository. Keys are derived from
// the repository's own layout (control keys, packs, loose revs/revprops,
// journal) instead of listing prefixes: a 625k-revision repository has ~2M
// objects and RustFS pages a prefix listing at seconds per page. Content-
// addressed leftovers are swept afterwards.
func doDeleteRepo(o *options, repoName string) {
	if !o.deleteForce && !o.deleteDryRun {
		fatal(fmt.Errorf("delete-repo removes every object; pass -force (or -dry-run)"))
	}
	st, prefix, err := o.repoStore(repoName)
	if err != nil {
		fatal(err)
	}
	head := o.deleteHead
	shardSz := uint64(4096)
	if o.deleteControl {
		// Control-only: make the repository disappear without touching the
		// (possibly millions of) payload objects. Used when a backend's bulk
		// delete is unreliable; the leftovers can be reclaimed offline.
		keys := store.NewKeys(prefix)
		ctx := context.Background()
		keysList := []string{keys.Current(), keys.Format(), keys.UUID(), keys.CommitLock(), keys.CommitLock() + ".break"}
		if !o.deleteDryRun {
			if err := st.Delete(ctx, keysList); err != nil {
				fatal(err)
			}
		}
		fmt.Printf("delete-repo %q: control keys %s\n", repoName, map[bool]string{true: "counted", false: "deleted"}[o.deleteDryRun])
		return
	}
	if head == 0 {
		repo, err := fs.OpenRepo(st, prefix)
		if err != nil {
			fatal(fmt.Errorf("open %q (use -head N if its metadata is already deleted): %w", repoName, err))
		}
		if head, err = repo.HeadFresh(); err != nil {
			fatal(err)
		}
		if sz := repo.ShardSize(); sz != 0 {
			shardSz = sz
		}
	}
	keys := store.NewKeys(prefix)
	lastShard := head / shardSz
	sub := func(name string) string {
		if prefix == "" {
			return name
		}
		return strings.Trim(prefix, "/") + "/" + name
	}

	batchSize := o.deleteBatch
	if batchSize <= 0 || batchSize > 1000 {
		batchSize = 1000
	}
	batches := make(chan []string, 64)
	go func() {
		defer close(batches)
		cur := make([]string, 0, batchSize)
		emit := func(k string) {
			cur = append(cur, k)
			if len(cur) == batchSize {
				batches <- cur
				cur = nil
			}
		}
		for shard := uint64(0); shard <= lastShard; shard++ {
			emit(keys.PackedRev(shard))
			emit(keys.PackedIdx(shard))
			emit(keys.PackedDone(shard))
			emit(keys.PackedRevProps(shard))
			first, last := shard*shardSz, shard*shardSz+shardSz-1
			if last > head {
				last = head
			}
			for rev := first; rev <= last; rev++ {
				emit(keys.LooseRev(shard, rev))
				emit(keys.LooseRevProps(shard, rev))
			}
		}
		for rev := uint64(1); rev <= head; rev++ {
			emit(keys.Journal(rev))
		}
		// Control keys last: a killed deletion leaves the repository openable,
		// so the command can be re-run for the remaining objects.
		for _, k := range []string{keys.Current(), keys.Format(), keys.UUID(),
			keys.CommitLock(), keys.CommitLock() + ".break"} {
			emit(k)
		}
		if len(cur) > 0 {
			batches <- cur
		}
	}()

	var deleted int64
	var firstErr error
	var mu sync.Mutex
	var wg sync.WaitGroup
	workers := o.deleteWorkers
	if workers <= 0 {
		workers = 16
	}
	ctx := context.Background()
	start := time.Now()
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := range batches {
				if o.deleteDryRun {
					atomic.AddInt64(&deleted, int64(len(b)))
					continue
				}
				var err error
				for attempt := 0; attempt < 3; attempt++ {
					c, cancel := context.WithTimeout(ctx, 90*time.Second)
					err = st.Delete(c, b)
					cancel()
					if err == nil {
						break
					}
				}
				if err != nil {
					log.Printf("delete batch of %d (first=%s) failed after retries: %v", len(b), b[0], err)
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					continue
				}
				atomic.AddInt64(&deleted, int64(len(b)))
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				n := atomic.LoadInt64(&deleted)
				rate := float64(n) / time.Since(start).Seconds()
				fmt.Printf("delete: %d objects, %.1f/s\n", n, rate)
			}
		}
	}()
	wg.Wait()
	close(done)
	if firstErr != nil {
		fatal(fmt.Errorf("delete: %w", firstErr))
	}
	if !o.deleteDryRun && !o.deleteNoSweep {
		for _, p := range []string{"texts/", "chunks/", "dirs/", "txn/", "dav-activities/", "dav-txns/", "bundles/", "locks/"} {
			if err := store.DeletePrefix(ctx, st, sub(strings.TrimSuffix(p, "/"))); err != nil {
				fatal(fmt.Errorf("sweep %s: %w", p, err))
			}
		}
	}
	fmt.Printf("delete-repo %q: head=%d, %d objects %s in %s\n", repoName, head, deleted,
		map[bool]string{true: "counted", false: "deleted"}[o.deleteDryRun], time.Since(start).Round(time.Second))
}

func doInfo(o *options, repoName string) {
	st, prefix, err := o.repoStore(repoName)
	if err != nil {
		fatal(err)
	}
	b, err := st.Get(context.Background(), store.NewKeys(prefix).Format(), nil)
	if err != nil {
		fatal(err)
	}
	f, err := fs.UnmarshalFormat(b)
	if err != nil {
		fatal(err)
	}
	repo, err := fs.OpenRepo(st, prefix)
	if err != nil {
		fatal(err)
	}
	head, err := repo.HeadFresh()
	if err != nil {
		fatal(err)
	}
	uuid, _ := repo.UUID()
	fmt.Printf("repo: %s\nformat-version: %d\nshard-size: %d\nmax-delta-chain: %d\n",
		repoName, f.Version, f.ShardSize, f.MaxDeltaChain)
	fmt.Printf("chunk-min: %d\nchunk-max: %d\nchunk-mask: %d\ndir-buckets: %v\nhead: %d\nuuid: %s\n",
		f.ChunkMin, f.ChunkMax, f.ChunkMask, f.DirBuckets, head, uuid)
}

// doMigrate changes opt-in format switches. Every replica must be restarted
// afterwards: servers cache the format when they open a repository.
func doMigrate(o *options, repoName string) {
	if o.migrateDirs == "" && o.migrateChunkAvg == 0 {
		fatal(fmt.Errorf("nothing to change: use --dir-buckets on|off and/or --chunk-avg N"))
	}
	st, prefix, err := o.repoStore(repoName)
	if err != nil {
		fatal(err)
	}
	f, err := fs.UpdateFormat(st, prefix, func(f *fs.Format) error {
		switch strings.ToLower(o.migrateDirs) {
		case "":
		case "on", "true", "1":
			f.DirBuckets = true
		case "off", "false", "0":
			f.DirBuckets = false
		default:
			return fmt.Errorf("invalid --dir-buckets %q (use on or off)", o.migrateDirs)
		}
		if o.migrateChunkAvg > 0 {
			cp := chunkParamsFor(o.migrateChunkAvg)
			f.ChunkMin = uint64(cp.Min)
			f.ChunkMax = uint64(cp.Max)
			f.ChunkMask = cp.Mask
		}
		return nil
	})
	if err != nil {
		fatal(err)
	}
	fmt.Printf("migrated %q: dir-buckets=%v chunk-min=%d chunk-max=%d chunk-mask=%d\n",
		repoName, f.DirBuckets, f.ChunkMin, f.ChunkMax, f.ChunkMask)
	fmt.Println("restart running servers and replicas to pick up the new format")
}

// doGC removes loose revision objects that have been superseded by a shard
// pack. Auto-packing only writes the pack; without this the loose copies stay
// forever and storage doubles.
func doGC(o *options, repoName string) {
	st, prefix, err := o.repoStore(repoName)
	if err != nil {
		fatal(err)
	}
	repo, err := fs.OpenRepo(st, prefix)
	if err != nil {
		fatal(err)
	}
	n, err := repo.CollectLoose(context.Background())
	if err != nil {
		fatal(err)
	}
	tn, err := repo.CollectTxnDataGrace(context.Background(), time.Duration(o.gcGrace)*time.Second)
	if err != nil {
		fatal(err)
	}
	tx, err := repo.CollectTextsGrace(context.Background(), time.Duration(o.gcGrace)*time.Second)
	if err != nil {
		fatal(err)
	}
	jn, err := repo.TrimJournal(o.keepJournal)
	if err != nil {
		fatal(err)
	}
	fmt.Printf("gc: removed %d loose objects, %d orphaned txn bodies and %d unreferenced texts from %q (journal: %d trimmed)\n",
		n, tn, tx, repoName, jn)
}

func doJournal(o *options, repoName string) {
	st, prefix, err := o.repoStore(repoName)
	if err != nil {
		fatal(err)
	}
	repo, err := fs.OpenRepo(st, prefix)
	if err != nil {
		fatal(err)
	}
	recs, err := repo.JournalTail(o.journalLimit)
	if err != nil {
		fatal(err)
	}
	if len(recs) == 0 {
		fmt.Printf("journal: no records\n")
		return
	}
	for _, rec := range recs {
		fmt.Printf("rev %d parent %d quorum %d lease %s created %d sha %s log %q\n",
			rec.Rev, rec.Parent, rec.Quorum, rec.Lease, rec.Created, rec.RevBlockSHA[:12], rec.Log)
	}
}

func doVerify(o *options, repoName string) {
	st, prefix, err := o.repoStore(repoName)
	if err != nil {
		fatal(err)
	}
	repo, err := fs.OpenRepo(st, prefix)
	if err != nil {
		fatal(err)
	}
	head, err := repo.Head()
	if err != nil {
		fatal(err)
	}
	var issues []fs.VerifyIssue
	if o.allRevs {
		from, to := o.fromRev, o.toRev
		if from == 0 {
			from = 1
		}
		if to == 0 || to > head {
			to = head
		}
		issues, err = repo.VerifyRange(from, to, o.texts)
		scope := fmt.Sprintf("revs %d..%d (texts=%v)", from, to, o.texts)
		if err != nil {
			fatal(err)
		}
		if len(issues) == 0 {
			fmt.Printf("verify: repository %q %s: no integrity issues\n", repoName, scope)
			return
		}
		fmt.Printf("verify: repository %q %s: %d integrity issues\n", repoName, scope, len(issues))
	} else {
		issues, err = repo.Verify(head)
		if err != nil {
			fatal(err)
		}
		if len(issues) == 0 {
			fmt.Printf("verify: repository %q at rev %d: no integrity issues\n", repoName, head)
			return
		}
		fmt.Printf("verify: repository %q at rev %d: %d integrity issues\n", repoName, head, len(issues))
	}
	for _, is := range issues {
		fmt.Printf("  rev=%d path=%s err=%v rebuildable=%v\n", is.Rev, is.Path, is.Err, is.Rebuildable)
	}
}

func doDump(o *options, repoName, output string) {
	st, prefix, err := o.repoStore(repoName)
	if err != nil {
		fatal(err)
	}
	repo, err := fs.OpenRepo(st, prefix)
	if err != nil {
		fatal(err)
	}
	var w io.Writer = os.Stdout
	var f *os.File
	if output != "" {
		f, err = os.Create(output)
		if err != nil {
			fatal(err)
		}
		defer f.Close()
		w = f
	}
	if err := dump.Dump(repo, w); err != nil {
		fatal(err)
	}
	if f != nil {
		fmt.Fprintf(os.Stderr, "dumped %s to %s\n", repoName, output)
	}
}

func doLoad(o *options, repoName, input string) {
	st, prefix, err := o.repoStore(repoName)
	if err != nil {
		fatal(err)
	}
	// create if it does not exist yet
	if _, err := fs.OpenRepo(st, prefix); err != nil {
		if _, cerr := fs.CreateRepo(st, prefix, defaultShardSize, defaultMaxDelta); cerr != nil {
			fatal(cerr)
		}
	}
	repo, err := fs.OpenRepo(st, prefix)
	if err != nil {
		fatal(err)
	}
	var r io.Reader = os.Stdin
	if input != "" {
		f, err := os.Open(input)
		if err != nil {
			fatal(err)
		}
		defer f.Close()
		r = f
	}
	if err := dump.Load(repo, r); err != nil {
		fatal(err)
	}
	head, _ := repo.Head()
	fmt.Printf("loaded %s, head=%d\n", repoName, head)
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "svnadmin: %v\n", err)
	os.Exit(1)
}
