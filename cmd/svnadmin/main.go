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
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"svngod/internal/dump"
	"svngod/internal/fs"
	"svngod/internal/fs/store"
)

const (
	defaultShardSize = 4096
	defaultMaxDelta  = 64
)

type options struct {
	endpoint   string
	accessKey  string
	secretKey  string
	bucket     string
	prefix     string
	useSSL     bool
	mirrorFile string
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
	if err := json.Unmarshal(data, &entries); err != nil {
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
	return store.NewMirror(ms), "repos/" + name, nil
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
	shardSize := uint64(defaultShardSize)
	maxDelta := uint64(defaultMaxDelta)
	if cmd == "create" {
		fs.Uint64Var(&shardSize, "shard-size", defaultShardSize, "revisions per shard (packing granularity)")
		fs.Uint64Var(&maxDelta, "max-delta", defaultMaxDelta, "maximum text delta chain length")
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
  svnadmin gc     [s3 flags] <repo>   # delete loose objects of packed shards

s3 flags: --endpoint H:PORT --access-key K --secret-key S --bucket B [--prefix P] [--ssl]
`)
}

func doCreate(o *options, repoName string, shardSize, maxDelta uint64) {
	st, prefix, err := o.repoStore(repoName)
	if err != nil {
		fatal(err)
	}
	repo, err := fs.CreateRepo(st, prefix, shardSize, maxDelta)
	if err != nil {
		fatal(err)
	}
	uuid, err := repo.UUID()
	if err != nil {
		fatal(err)
	}
	fmt.Printf("created repository %q uuid=%s\n", repoName, uuid)
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
	tn, err := repo.CollectTxnData(context.Background())
	if err != nil {
		fatal(err)
	}
	fmt.Printf("gc: removed %d loose objects and %d orphaned txn bodies from %q\n", n, tn, repoName)
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
	issues, err := repo.Verify(head)
	if err != nil {
		fatal(err)
	}
	if len(issues) == 0 {
		fmt.Printf("verify: repository %q at rev %d: no integrity issues\n", repoName, head)
		return
	}
	fmt.Printf("verify: repository %q at rev %d: %d integrity issues\n", repoName, head, len(issues))
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
