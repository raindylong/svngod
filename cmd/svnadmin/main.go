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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
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
	verifyWorkers   int
	textWorkers     int
	progress        bool
	loadFrom        uint64
	loadTo          uint64
	rebuildIndex    bool
	verifyMembers   bool
	verifyTextfile  string
	repairVerify    bool
	repairMembers   bool
	repairMirror    bool
	repairIndexOnly bool
	repairProps     bool
	replicaQuiet    bool
	replicaTextfile string
	replicaCounts   bool
	keepJournal     uint64
	gcGrace         int
	gcTexts         bool
	gcCompact       bool
	gcDryRun        bool
	chunkAvg        uint64
	dirBuckets      bool
	segments        bool
	segmentTarget   uint64

	deleteWorkers int
	deleteDryRun  bool
	deleteForce   bool
	deleteNoSweep bool
	deleteHead    uint64
	deleteBatch   int
	deleteControl bool

	migrateSegments string
	migratePurge    bool
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
	entries, quorum, repoPrefix, err := readMirrorFile(o.mirrorFile)
	if err != nil {
		return nil, "", err
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
	fs.BoolVar(&o.rebuildIndex, "rebuild-index", false, "repair: rebuild segment shard indexes from segment objects")
	var output, input string
	if cmd == "migrate" {
		fs.StringVar(&o.migrateSegments, "segments", "", "on: rewrite revisions into segment objects (format v2)")
		fs.BoolVar(&o.migratePurge, "purge", false, "with --segments on: delete superseded v1 objects after switching")
		fs.StringVar(&o.migrateDirs, "dir-buckets", "", "on|off: page large directories (leave empty to keep)")
		fs.Uint64Var(&o.migrateChunkAvg, "chunk-avg", 0, "new target average chunk size in bytes (0 keeps)")
	}
	if cmd == "create" {
		fs.BoolVar(&o.segments, "segments", false, "store revisions in batched segment objects (format v2)")
		fs.Uint64Var(&o.segmentTarget, "segment-target", 0, "compaction target revisions per segment (0 = shard size)")
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
	if cmd == "replicas" {
		fs.BoolVar(&o.verifyMembers, "verify", false, "also walk every revision on each member")
		fs.BoolVar(&o.repairMembers, "repair", false, "run every repair action before comparing")
		fs.BoolVar(&o.repairMirror, "repair-mirror", false, "only copy missing objects between members")
		fs.BoolVar(&o.repairIndexOnly, "repair-index", false, "only rebuild each member's shard indexes")
		fs.BoolVar(&o.repairProps, "repair-revprops", false, "only drop dead revision-property overrides")
		fs.BoolVar(&o.replicaQuiet, "quiet", false, "only print differences and the final verdict")
		fs.StringVar(&o.replicaTextfile, "textfile", "", "also write these metrics as a Prometheus textfile")
		fs.BoolVar(&o.replicaCounts, "counts", false, "also count content objects per member (slow on large repositories)")
	}
	if cmd == "verify" {
		fs.BoolVar(&o.allRevs, "all-revs", false, "verify every historical revision, not just HEAD")
		fs.BoolVar(&o.texts, "texts", false, "with --all-revs: read and hash every file text (slow)")
		fs.Uint64Var(&o.fromRev, "from", 1, "first revision for --all-revs")
		fs.Uint64Var(&o.toRev, "to", 0, "last revision for --all-revs (0 = HEAD)")
		fs.IntVar(&o.verifyWorkers, "workers", 1, "parallel workers for --all-revs")
		fs.BoolVar(&o.rebuildIndex, "repair-index", false, "with --all-revs: rebuild shard indexes from segments before checking")
		fs.IntVar(&o.textWorkers, "text-workers", 4, "concurrent text checks inside one revision with --texts")
		fs.BoolVar(&o.progress, "progress", false, "print progress to stderr every 5s")
		fs.StringVar(&o.verifyTextfile, "textfile", "", "write the verify result as a Prometheus textfile")
	}
	if cmd == "journal" {
		fs.IntVar(&o.journalLimit, "n", 20, "number of records to show")
	}
	if cmd == "gc" {
		fs.Uint64Var(&o.keepJournal, "keep-journal", 4096, "number of recent WAL records to keep")
		fs.IntVar(&o.gcGrace, "gc-grace", 3600, "seconds an unreferenced text/chunk must be old before collection (0 disables)")
		fs.BoolVar(&o.gcTexts, "texts", false, "scan every revision to collect unreferenced texts (slow: O(all revisions))")
		fs.BoolVar(&o.gcCompact, "compact", true, "merge segment shards whose index has more than one entry")
		fs.BoolVar(&o.gcDryRun, "dry-run", false, "only report what would be removed")
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
		fs.BoolVar(&o.progress, "progress", false, "print progress to stderr every 5s")
		fs.StringVar(&input, "input", "", "read dump from file (default stdin)")
		fs.Uint64Var(&o.loadFrom, "from", 0, "first revision number to apply (earlier ones must already exist)")
		fs.Uint64Var(&o.loadTo, "to", 0, "last revision number to apply (0 = all)")
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
	case "replicas":
		doReplicas(&o, repoName, o.verifyMembers, o.repairMembers)
	default:
		usage()
		os.Exit(2)
	}
}

// readMirrorFile parses a --mirror-file (array or {"mirrors":[...],"quorum":N,"prefix":P}).
func readMirrorFile(path string) ([]mirrorEntry, int, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, "", err
	}
	var entries []mirrorEntry
	var quorum int
	repoPrefix := "repos"
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) > 0 && trimmed[0] == '{' {
		var obj struct {
			Mirrors []mirrorEntry `json:"mirrors"`
			Quorum  int           `json:"quorum"`
			Prefix  string        `json:"prefix"`
		}
		if err := json.Unmarshal(trimmed, &obj); err != nil {
			return nil, 0, "", fmt.Errorf("parse --mirror-file: %v", err)
		}
		entries, quorum = obj.Mirrors, obj.Quorum
		if obj.Prefix != "" {
			repoPrefix = obj.Prefix
		}
	} else if err := json.Unmarshal(data, &entries); err != nil {
		return nil, 0, "", fmt.Errorf("parse --mirror-file: %v", err)
	}
	return entries, quorum, repoPrefix, nil
}

// memberStore opens one mirror member as an independent view of the repository
// (keys relative to the bucket/root, repository prefix applied by the caller).
func memberStore(e mirrorEntry) (store.Store, error) {
	if e.Type == "s3" {
		return store.NewS3Store(store.S3Config{
			Endpoint: e.Endpoint, AccessKey: e.Access, SecretKey: e.Secret,
			Bucket: e.Bucket, Region: "us-east-1", UseSSL: e.SSL,
		})
	}
	return store.NewFileStore(e.Root)
}

// memberStat is one mirror member's reported state.
type memberStat struct {
	index  int
	label  string
	head   uint64
	digest string
	issues int
	ok     bool
	// counts is populated only with -counts; nil means "not measured".
	counts map[string]contentCount
}

// contentCount is how many objects of one kind a member holds, and their size.
type contentCount struct {
	objects int64
	bytes   int64
}

// doReplicas reports each mirror member's HEAD, UUID and shard-index digest.
// Any difference means a replica is behind or damaged; -verify additionally
// walks every revision on each member.
func doReplicas(o *options, repoName string, verify, repair bool) {
	quiet := o.replicaQuiet
	if o.mirrorFile == "" {
		fatal(errors.New("replicas requires --mirror-file"))
	}
	entries, _, repoPrefix, err := readMirrorFile(o.mirrorFile)
	if err != nil {
		fatal(err)
	}
	keyPrefix := strings.Trim(repoPrefix, "/") + "/" + repoName
	repairMirror, repairIndex, repairProps := false, false, false
	switch {
	case o.repairMirror || o.repairIndexOnly || o.repairProps:
		// Specific actions were requested: run exactly those.
		repairMirror, repairIndex, repairProps = o.repairMirror, o.repairIndexOnly, o.repairProps
	case repair:
		repairMirror, repairIndex, repairProps = true, true, true
	}
	if repairMirror {
		if ms, _, merr := o.mirrorStore(repoName); merr == nil {
			if rp, ok := ms.(interface{ RepairNow() }); ok {
				if !quiet {
					fmt.Println("replicas: running mirror repair before the comparison")
				}
				rp.RepairNow()
			}
			closeStore(ms)
		}
	}
	if repairIndex || repairProps {
		// Divergent (present but different) shard indexes are converged by
		// rebuilding each member's index from *its own* immutable segments.
		// Copying one member's index onto another could propagate a stale
		// mapping after a failover; a rebuild can only produce what the
		// member's own objects say, and equal objects yield equal digests.
		for i, e := range entries {
			st, serr := memberStore(e)
			if serr != nil {
				continue
			}
			r, oerr := fs.OpenRepo(st, keyPrefix)
			if oerr == nil {
				if repairIndex {
					if n, rerr := r.RepairSegmentIndexes(context.Background()); rerr != nil {
						fmt.Printf("replicas: member[%d] index rebuild: %v\n", i, rerr)
					} else if n > 0 && !quiet {
						fmt.Printf("replicas: member[%d] rebuilt %d shard index(es)\n", i, n)
					}
				}
				if repairProps {
					if d, derr := r.RepairRevPropsOverrides(context.Background()); derr != nil {
						fmt.Printf("replicas: member[%d] revprop repair: %v\n", i, derr)
					} else if d > 0 && !quiet {
						fmt.Printf("replicas: member[%d] dropped %d dead revprop override(s)\n", i, d)
					}
				}
			}
			closeStore(st)
		}
	}
	var firstHead uint64
	var firstDigest, firstUUID string
	drift := false
	var stats []memberStat
	for i, e := range entries {
		st, serr := memberStore(e)
		if serr != nil {
			fmt.Printf("member[%d] %s: %v\n", i, e.Endpoint+e.Root, serr)
			drift = true
			continue
		}
		r, oerr := fs.OpenRepo(st, keyPrefix)
		if oerr != nil {
			fmt.Printf("member[%d] %s: %v\n", i, e.Endpoint+e.Root, oerr)
			drift = true
			closeStore(st)
			continue
		}
		head, herr := r.HeadFresh()
		uuid, _ := r.UUID()
		digest, derr := r.ReplicaDigest()
		if herr != nil || derr != nil {
			fmt.Printf("member[%d]: head err=%v digest err=%v\n", i, herr, derr)
			drift = true
			closeStore(st)
			continue
		}
		var counts map[string]contentCount
		if o.replicaCounts {
			c, cerr := countContentObjects(context.Background(), st, keyPrefix)
			counts = c
			if cerr != nil {
				// A failed listing is reported but not treated as drift: S3
				// throttling or a transient network error must not page anyone.
				fmt.Printf("member[%d] counts: %v\n", i, cerr)
			}
		}
		if !quiet {
			fmt.Printf("member[%d] %s head=%d uuid=%s index=%s\n", i, e.Endpoint+e.Root, head, uuid, short(digest))
			if counts != nil {
				fmt.Printf("member[%d] content texts=%d/%s chunks=%d/%s dirs=%d/%s\n", i,
					counts["texts"].objects, mib(counts["texts"].bytes),
					counts["chunks"].objects, mib(counts["chunks"].bytes),
					counts["dirs"].objects, mib(counts["dirs"].bytes))
			}
		}
		stats = append(stats, memberStat{index: i, label: e.Endpoint + e.Root, head: head, digest: digest, ok: true, counts: counts})
		if i == 0 {
			firstHead, firstDigest, firstUUID = head, digest, uuid
		} else if head != firstHead || digest != firstDigest || uuid != firstUUID {
			drift = true
		}
		if verify {
			issues, verr := r.VerifyRangeWith(1, head, false, 4)
			if verr != nil || len(issues) > 0 {
				stats[len(stats)-1].issues = len(issues)
				if verr != nil {
					stats[len(stats)-1].issues = -1
				}
				fmt.Printf("member[%d] verify: %d issue(s) err=%v\n", i, len(issues), verr)
				for _, is := range issues {
					fmt.Printf("  rev=%d %v\n", is.Rev, is.Err)
				}
				drift = true
			} else if !quiet {
				fmt.Printf("member[%d] verify: no issues\n", i)
			}
		}
		closeStore(st)
	}
	if o.replicaCounts {
		reportContentCountDiffs(stats)
	}
	if o.replicaTextfile != "" {
		writeReplicaMetrics(o.replicaTextfile, repoName, stats, drift, o.replicaCounts)
	}
	if drift {
		fmt.Println("replicas: differences:")
		reportDriftDiffs(entries, keyPrefix)
		fmt.Println("replicas: DRIFT")
		os.Exit(1)
	}
	fmt.Println("replicas: consistent")
}

// contentKinds are the content-addressed object families whose coverage can
// differ between members even when HEAD and segment digests agree: a
// commit-quorum write can leave one standby without a text/chunk blob.
var contentKinds = []string{"texts", "chunks", "dirs"}

// countContentObjects lists the content-addressed object families of one
// member. It is O(objects) in backend requests, hence opt-in via -counts.
func countContentObjects(ctx context.Context, st store.Store, prefix string) (map[string]contentCount, error) {
	out := make(map[string]contentCount, len(contentKinds))
	for _, kind := range contentKinds {
		p := strings.TrimPrefix(prefix, "/")
		if p != "" {
			p += "/"
		}
		p += kind + "/"
		var c contentCount
		next := ""
		for {
			objs, cont, err := st.ListPrefix(ctx, p, next, 1000)
			if err != nil {
				return out, fmt.Errorf("%s: %w", kind, err)
			}
			for _, o := range objs {
				c.objects++
				c.bytes += o.Size
			}
			if cont == "" || cont == next {
				break
			}
			next = cont
		}
		out[kind] = c
	}
	return out, nil
}

// reportContentCountDiffs prints members whose content object counts differ.
// A differing count is a warning, not drift: a standby that was down during a
// delete or a failed best-effort mirror write legitimately leaves unreferenced
// orphans on the other member. Missing *referenced* data is what -verify
// proves; equal counts alone never prove absence of that.
func reportContentCountDiffs(stats []memberStat) {
	var differ []string
	for _, kind := range contentKinds {
		first, ok := -1, false
		same := true
		for i := range stats {
			c, measured := stats[i].counts[kind]
			if !measured {
				continue
			}
			if !ok {
				first, ok = i, true
				continue
			}
			if c != stats[first].counts[kind] {
				same = false
			}
		}
		if ok && !same {
			var parts []string
			for i := range stats {
				if c, measured := stats[i].counts[kind]; measured {
					parts = append(parts, fmt.Sprintf("member[%d]=%d/%s", stats[i].index, c.objects, mib(c.bytes)))
				}
			}
			differ = append(differ, "  "+kind+": "+strings.Join(parts, " "))
		}
	}
	if len(differ) > 0 {
		fmt.Println("replicas: content counts differ (unreferenced orphans are harmless; run -verify to rule out missing data):")
		for _, line := range differ {
			fmt.Println(line)
		}
	}
}

func mib(n int64) string {
	return fmt.Sprintf("%.1fMiB", float64(n)/(1<<20))
}

// writeReplicaMetrics writes a Prometheus textfile (node_exporter textfile
// collector layout) so replica drift can alert without parsing logs.
func writeReplicaMetrics(path, repoName string, stats []memberStat, drift, counts bool) {
	var b strings.Builder
	b.WriteString("# HELP svngod_replica_head HEAD reported by one mirror member.\n")
	b.WriteString("# TYPE svngod_replica_head gauge\n")
	for _, st := range stats {
		if !st.ok {
			continue
		}
		fmt.Fprintf(&b, "svngod_replica_head{repo=%q,member=%q} %d\n", repoName, st.label, st.head)
	}
	b.WriteString("# HELP svngod_replica_verify_issues Integrity issues found on one member (-1 = verify failed).\n")
	b.WriteString("# TYPE svngod_replica_verify_issues gauge\n")
	for _, st := range stats {
		if !st.ok {
			continue
		}
		fmt.Fprintf(&b, "svngod_replica_verify_issues{repo=%q,member=%q} %d\n", repoName, st.label, st.issues)
	}
	b.WriteString("# HELP svngod_replica_consistent 1 when every member reports the same HEAD/UUID/index digest.\n")
	b.WriteString("# TYPE svngod_replica_consistent gauge\n")
	consistent := 0
	if !drift {
		consistent = 1
	}
	fmt.Fprintf(&b, "svngod_replica_consistent{repo=%q} %d\n", repoName, consistent)
	if counts {
		b.WriteString("# HELP svngod_replica_content_objects Content-addressed objects held by one member.\n")
		b.WriteString("# TYPE svngod_replica_content_objects gauge\n")
		for _, st := range stats {
			if !st.ok || st.counts == nil {
				continue
			}
			for _, kind := range contentKinds {
				fmt.Fprintf(&b, "svngod_replica_content_objects{repo=%q,member=%q,kind=%q} %d\n",
					repoName, st.label, kind, st.counts[kind].objects)
			}
		}
		b.WriteString("# HELP svngod_replica_content_bytes Bytes held by one member, per content family.\n")
		b.WriteString("# TYPE svngod_replica_content_bytes gauge\n")
		for _, st := range stats {
			if !st.ok || st.counts == nil {
				continue
			}
			for _, kind := range contentKinds {
				fmt.Fprintf(&b, "svngod_replica_content_bytes{repo=%q,member=%q,kind=%q} %d\n",
					repoName, st.label, kind, st.counts[kind].bytes)
			}
		}
	}
	b.WriteString("# HELP svngod_replica_last_run_timestamp Unix time of the last replica check.\n")
	b.WriteString("# TYPE svngod_replica_last_run_timestamp gauge\n")
	fmt.Fprintf(&b, "svngod_replica_last_run_timestamp{repo=%q} %d\n", repoName, time.Now().Unix())
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(b.String()), 0o644); err != nil {
		os.Remove(tmp)
		fmt.Fprintf(os.Stderr, "replicas: write %s: %v\n", tmp, err)
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		fmt.Fprintf(os.Stderr, "replicas: rename %s: %v\n", path, err)
	}
}

func closeStore(st store.Store) {
	if c, ok := st.(interface{ Close() }); ok {
		c.Close()
	}
}

func short(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

func usage() {
	fmt.Fprint(os.Stderr, `svnadmin - svngod repository administration

usage:
  svnadmin create [--segments] [s3 flags | --mirror-file FILE] <repo>
  svnadmin dump   [--output FILE] [s3 flags] <repo>
  svnadmin load   [--input FILE] [--from N] [--to N] [s3 flags] <repo>  # resumable
  svnadmin verify [--all-revs [--workers N] [--texts] [--repair-index]] [s3 flags] <repo>  # integrity walk
  svnadmin repair [--mirror-file FILE] [--rebuild-index] <repo>  # reconcile mirrors / rebuild segment indexes
  svnadmin delete-repo -force [s3 flags | --mirror-file FILE] <repo>  # delete every object of a repository
  svnadmin journal [-n N] [s3 flags | --mirror-file FILE] <repo>  # show WAL records
  svnadmin replicas [-verify] [-counts] [-repair | -repair-mirror|-repair-index|-repair-revprops] --mirror-file FILE <repo>
  svnadmin gc [-texts] [s3 flags] <repo>  # loose objects, txn bodies, segment compaction (-texts: unreferenced texts, slow)

s3 flags: --endpoint H:PORT --access-key K --secret-key S --bucket B [--prefix P] [--ssl]
`)
}

func doCreate(o *options, repoName string, shardSize, maxDelta uint64) {
	st, prefix, err := o.repoStore(repoName)
	if err != nil {
		fatal(err)
	}
	var repo *fs.Repo
	if o.segments {
		repo, err = fs.CreateRepoSegments(st, prefix, shardSize, maxDelta, chunkParamsFor(o.chunkAvg), o.dirBuckets, o.segmentTarget)
	} else {
		repo, err = fs.CreateRepoFull(st, prefix, shardSize, maxDelta, chunkParamsFor(o.chunkAvg), o.dirBuckets)
	}
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
	if o.rebuildIndex {
		n, rerr := r.RepairSegmentIndexes(context.Background())
		if rerr != nil {
			fatal(rerr)
		}
		fmt.Printf("repair: rebuilt %d segment shard index(es) from segment objects\n", n)
		d, derr := r.RepairRevPropsOverrides(context.Background())
		if derr != nil {
			fatal(derr)
		}
		if d > 0 {
			fmt.Printf("repair: dropped %d dead revision-property override(s)\n", d)
		}
	}
	if rp, ok := st.(interface{ RepairNow() }); ok {
		rp.RepairNow()
	} else if !o.rebuildIndex {
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
	fmt.Printf("segments: %v\nsegment-from: %d\nsegment-target: %d\n",
		f.Segments, f.SegmentFrom, f.SegmentTarget)
}

// doMigrate changes opt-in format switches. Every replica must be restarted
// afterwards: servers cache the format when they open a repository.
// doMigrateToSegments rewrites every revision into segment objects and then
// switches the repository format. Run it with writers stopped: HEAD moving
// during the pass aborts the migration (the repository stays v1).
func doMigrateToSegments(o *options, repoName string) {
	st, prefix, err := o.repoStore(repoName)
	if err != nil {
		fatal(err)
	}
	repo, err := fs.OpenRepo(st, prefix)
	if err != nil {
		fatal(err)
	}
	n, err := repo.MigrateToSegments(context.Background(), o.migratePurge)
	if err != nil {
		fatal(err)
	}
	fmt.Printf("migrated %q to segment format: %d revisions%s\n", repoName, n,
		map[bool]string{true: " (v1 objects purged)", false: ""}[o.migratePurge])
	fmt.Println("restart running servers and replicas to pick up the new format")
}

func doMigrate(o *options, repoName string) {
	if o.migrateSegments != "" {
		switch strings.ToLower(o.migrateSegments) {
		case "on", "true", "1":
			doMigrateToSegments(o, repoName)
		case "off", "false", "0":
			fatal(fmt.Errorf("segment format cannot be reverted online; migrate to a v1 repository with dump/load"))
		default:
			fatal(fmt.Errorf("invalid --segments %q (use on)", o.migrateSegments))
		}
		return
	}
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
	ctx := context.Background()
	grace := time.Duration(o.gcGrace) * time.Second
	// Long collections report their phase every 5 seconds; a full sweep of a
	// large repository can run for hours.
	var mu sync.Mutex
	last := time.Now()
	phaseBytes := map[string]int64{}
	progress := fs.GCProgress(func(phase string, done, total int, bytes int64) {
		mu.Lock()
		if bytes > 0 {
			phaseBytes[phase] = bytes
		}
		if time.Since(last) < 5*time.Second {
			mu.Unlock()
			return
		}
		last = time.Now()
		mu.Unlock()
		if total > 0 {
			fmt.Fprintf(os.Stderr, "gc: %s %d/%d\n", phase, done, total)
		} else {
			fmt.Fprintf(os.Stderr, "gc: %s %d\n", phase, done)
		}
	})
	phase := func(name string, fn func() (int, error)) int {
		t := time.Now()
		n, perr := fn()
		if perr != nil {
			fatal(perr)
		}
		fmt.Printf("gc: %-9s %8d  %s\n", name, n, time.Since(t).Round(time.Millisecond))
		return n
	}
	dry := o.gcDryRun
	n := phase("loose", func() (int, error) { return repo.CollectLooseProgress(ctx, dry, progress) })
	tn := phase("txn-data", func() (int, error) {
		if dry {
			return repo.CollectTxnDataDry(ctx, grace)
		}
		return repo.CollectTxnDataGrace(ctx, grace)
	})
	// Collecting unreferenced texts walks every revision, which costs minutes
	// per 100k revisions on object storage. It is a leak-reclamation pass, never
	// needed for correctness, so routine GC skips it and ops opt in with -texts.
	tx := 0
	if o.gcTexts {
		tx = phase("texts", func() (int, error) { return repo.CollectTextsGraceProgress(ctx, grace, dry, progress) })
	}
	jn := phase("journal", func() (int, error) {
		if dry {
			return repo.TrimJournalDry(o.keepJournal)
		}
		return repo.TrimJournal(o.keepJournal)
	})
	merged := 0
	if o.gcCompact {
		merged = phase("segments", func() (int, error) { return repo.CompactSegmentsProgress(ctx, dry, progress) })
	}
	// The segment sweep takes the commit lease while it deletes, so a short age
	// filter is safe and lets one gc run clear the superseded segments that its
	// own compaction just left behind.
	orphans := phase("orphans", func() (int, error) {
		return repo.CollectOrphanSegmentsProgress(ctx, time.Minute, dry, progress)
	})
	if dry {
		var bytes int64
		mu.Lock()
		for _, v := range phaseBytes {
			bytes += v
		}
		mu.Unlock()
		fmt.Printf("gc: %q dry-run (would remove loose=%d txn=%d texts=%d, merge %d shard(s), drop orphans=%d, journal=%d, reclaim ~%s; nothing was deleted)\n",
			repoName, n, tn, tx, merged, orphans, jn, humanBytes(bytes))
		return
	}
	fmt.Printf("gc: %q done (loose=%d txn=%d texts=%d merged=%d orphans=%d journal=%d)\n",
		repoName, n, tn, tx, merged, orphans, jn)
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
		// A version-2 record carries the segment digest (and, for a batch that
		// crossed a shard boundary, one entry per group); version 1 carried the
		// rev-block digest.
		sum := rec.SegmentSHA
		if sum == "" {
			sum = rec.RevBlockSHA
		}
		if len(sum) > 12 {
			sum = sum[:12]
		}
		fmt.Printf("rev %d parent %d quorum %d lease %s created %d sha %s groups %d log %q\n",
			rec.Rev, rec.Parent, rec.Quorum, rec.Lease, rec.Created, sum, len(rec.Groups), rec.Log)
	}
}

func doVerify(o *options, repoName string) {
	begin := time.Now()
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
	if o.rebuildIndex && o.allRevs {
		// Rebuild shard indexes from the immutable segments first: a lost index
		// entry makes revisions unreadable, and verify would otherwise only
		// report it. A rebuild that cannot cover HEAD reports why instead of
		// writing a shorter index.
		if n, rerr := repo.RepairSegmentIndexes(context.Background()); rerr != nil {
			fmt.Printf("verify: index rebuild: %v\n", rerr)
		} else if n > 0 {
			fmt.Printf("verify: rebuilt %d shard index(es) before checking\n", n)
		}
	}
	var issues []fs.VerifyIssue
	scope := fmt.Sprintf("at rev %d", head)
	if o.allRevs {
		from, to := o.fromRev, o.toRev
		if from == 0 {
			from = 1
		}
		if to == 0 || to > head {
			to = head
		}
		fs.SetVerifyTextWorkers(o.textWorkers)
		var progress func(uint64)
		if o.progress {
			total := to - from + 1
			last := time.Now()
			var mu sync.Mutex
			progress = func(done uint64) {
				mu.Lock()
				defer mu.Unlock()
				if time.Since(last) < 5*time.Second || done == 0 {
					return
				}
				last = time.Now()
				fmt.Fprintf(os.Stderr, "verify: %d/%d revisions (%.0f%%)\n", done, total, float64(done)*100/float64(total))
			}
		}
		issues, err = repo.VerifyRangeWithProgress(from, to, o.texts, o.verifyWorkers, progress)
		if err != nil {
			fatal(err)
		}
		scope = fmt.Sprintf("revs %d..%d (texts=%v, workers=%d)", from, to, o.texts, o.verifyWorkers)
	} else {
		issues, err = repo.Verify(head)
		if err != nil {
			fatal(err)
		}
	}
	if o.verifyTextfile != "" {
		writeVerifyMetrics(o.verifyTextfile, repoName, scope, len(issues), time.Since(begin))
	}
	if len(issues) == 0 {
		fmt.Printf("verify: repository %q %s: no integrity issues\n", repoName, scope)
		return
	}
	fmt.Printf("verify: repository %q %s: %d integrity issues\n", repoName, scope, len(issues))
	for _, is := range issues {
		fmt.Printf("  rev=%d path=%s err=%v rebuildable=%v\n", is.Rev, is.Path, is.Err, is.Rebuildable)
	}
	// A verifier's job is to fail: monitoring and cron must be able to act on
	// the exit status instead of parsing log lines.
	os.Exit(1)
}

// writeVerifyMetrics publishes the last verify result as a Prometheus textfile,
// so a scheduled full check shows up on the alerting dashboard (the exit code
// alone is invisible to Prometheus).
func writeVerifyMetrics(path, repoName, scope string, issues int, dur time.Duration) {
	var b strings.Builder
	b.WriteString("# HELP svngod_verify_issues Integrity issues found by the last verify.\n")
	b.WriteString("# TYPE svngod_verify_issues gauge\n")
	fmt.Fprintf(&b, "svngod_verify_issues{repo=%q,scope=%q} %d\n", repoName, scope, issues)
	b.WriteString("# HELP svngod_verify_ok 1 when the last verify found no issues.\n")
	b.WriteString("# TYPE svngod_verify_ok gauge\n")
	ok := 0
	if issues == 0 {
		ok = 1
	}
	fmt.Fprintf(&b, "svngod_verify_ok{repo=%q} %d\n", repoName, ok)
	b.WriteString("# HELP svngod_verify_duration_seconds Duration of the last verify.\n")
	b.WriteString("# TYPE svngod_verify_duration_seconds gauge\n")
	fmt.Fprintf(&b, "svngod_verify_duration_seconds{repo=%q} %.3f\n", repoName, dur.Seconds())
	b.WriteString("# HELP svngod_verify_last_run_timestamp Unix time of the last verify.\n")
	b.WriteString("# TYPE svngod_verify_last_run_timestamp gauge\n")
	fmt.Fprintf(&b, "svngod_verify_last_run_timestamp{repo=%q} %d\n", repoName, time.Now().Unix())
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(b.String()), 0o644); err != nil {
		os.Remove(tmp)
		fmt.Fprintf(os.Stderr, "verify: write %s: %v\n", tmp, err)
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		fmt.Fprintf(os.Stderr, "verify: rename %s: %v\n", path, err)
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
	var loadProgress func(uint64)
	if o.progress {
		start := time.Now()
		last := time.Now()
		var mu sync.Mutex
		loadProgress = func(rev uint64) {
			mu.Lock()
			defer mu.Unlock()
			if time.Since(last) < 5*time.Second {
				return
			}
			last = time.Now()
			fmt.Fprintf(os.Stderr, "load: revision %d applied (%s)\n", rev, time.Since(start).Round(time.Second))
		}
	}
	if err := dump.LoadRangeProgress(repo, r, o.loadFrom, o.loadTo, loadProgress); err != nil {
		fatal(err)
	}
	head, _ := repo.Head()
	fmt.Printf("loaded %s, head=%d\n", repoName, head)
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "svnadmin: %v\n", err)
	os.Exit(1)
}

// reportDriftDiffs lists the concrete objects that differ between members, so
// an operator can tell which side lost or corrupted one. Segment objects are
// compared by size (content comparison would download every segment from every
// member); indexes and revprop-override objects are small and compared by hash.
func reportDriftDiffs(entries []mirrorEntry, keyPrefix string) {
	type view struct {
		label string
		st    store.Store
		sizes map[string]int64
	}
	var views []view
	for _, e := range entries {
		st, err := memberStore(e)
		if err != nil {
			continue
		}
		v := view{label: e.Endpoint + e.Root, st: st, sizes: map[string]int64{}}
		for _, p := range []string{keyPrefix + "/segs/", keyPrefix + "/seg-revprops/"} {
			next := ""
			for {
				objs, cont, lerr := st.ListPrefix(context.Background(), p, next, 1000)
				if lerr != nil {
					break
				}
				for _, o := range objs {
					v.sizes[o.Key] = o.Size
				}
				if cont == "" || cont == next {
					break
				}
				next = cont
			}
		}
		views = append(views, v)
	}
	defer func() {
		for _, v := range views {
			closeStore(v.st)
		}
	}()
	if len(views) < 2 {
		return
	}
	set := map[string]bool{}
	for _, v := range views {
		for k := range v.sizes {
			set[k] = true
		}
	}
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	shown := 0
	for _, k := range keys {
		var missing []string
		var sizes []int64
		for _, v := range views {
			if sz, ok := v.sizes[k]; ok {
				sizes = append(sizes, sz)
			} else {
				missing = append(missing, v.label)
			}
		}
		if len(missing) > 0 {
			fmt.Printf("  %s: missing on %s\n", k, strings.Join(missing, ", "))
		} else {
			sizeDiff := false
			for _, sz := range sizes[1:] {
				if sz != sizes[0] {
					sizeDiff = true
				}
			}
			if sizeDiff {
				fmt.Printf("  %s: size differs %v\n", k, sizes)
			} else if strings.HasSuffix(k, ".idx") || strings.Contains(k, "/seg-revprops/") {
				var sums []string
				diff := false
				for _, v := range views {
					b, gerr := v.st.Get(context.Background(), k, nil)
					if gerr != nil {
						sums = append(sums, "unreadable")
						diff = true
						continue
					}
					sum := sha256.Sum256(b)
					sums = append(sums, hex.EncodeToString(sum[:8]))
				}
				for _, sm := range sums[1:] {
					if sm != sums[0] {
						diff = true
					}
				}
				if diff {
					fmt.Printf("  %s: content differs %v\n", k, sums)
				} else {
					continue
				}
			} else {
				continue
			}
		}
		shown++
		if shown >= 40 {
			fmt.Println("  … (further differences omitted)")
			break
		}
	}
}

// humanBytes formats a byte count for operator-facing output.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
