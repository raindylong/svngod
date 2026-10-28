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
	"flag"
	"fmt"
	"path/filepath"
	"io"
	"os"

	"svngod/internal/dump"
	"svngod/internal/fs"
	"svngod/internal/fs/store"
)

const (
	defaultShardSize = 4096
	defaultMaxDelta  = 64
)

type options struct {
	endpoint  string
	accessKey string
	secretKey string
	bucket    string
	prefix    string
	useSSL    bool
}

func (o *options) repoStore(localPath string) (store.Store, string, error) {
	name := filepath.Base(localPath)
	if o.endpoint == "" {
		// localPath is <parent>/<repoName>; base is the parent directory so the
		// same store layout is shared with the server (--repo-root <parent>).
		st, err := store.NewFileStore(filepath.Dir(localPath))
		if err != nil {
			return nil, "", err
		}
		return st, "repos/" + name, nil
	}
	st, err := store.NewS3Store(store.S3Config{
		Endpoint:   o.endpoint,
		AccessKey:  o.accessKey,
		SecretKey:  o.secretKey,
		Bucket:     o.bucket,
		Region:     "us-east-1",
		UseSSL:     o.useSSL,
		RepoPrefix: o.prefix,
	})
	if err != nil {
		return nil, "", err
	}
	prefix := o.prefix
	if prefix == "" {
		prefix = "repos/" + name
	}
	return st, prefix, nil
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
	var output, input string
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
		doCreate(&o, repoName)
	case "dump":
		doDump(&o, repoName, output)
	case "load":
		doLoad(&o, repoName, input)
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `svnadmin - svngod repository administration

usage:
  svnadmin create [s3 flags] <repo>
  svnadmin dump   [--output FILE] [s3 flags] <repo>
  svnadmin load   [--input FILE] [s3 flags] <repo>

s3 flags: --endpoint H:PORT --access-key K --secret-key S --bucket B [--prefix P] [--ssl]
`)
}

func doCreate(o *options, repoName string) {
	st, prefix, err := o.repoStore(repoName)
	if err != nil {
		fatal(err)
	}
	repo, err := fs.CreateRepo(st, prefix, defaultShardSize, defaultMaxDelta)
	if err != nil {
		fatal(err)
	}
	uuid, err := repo.UUID()
	if err != nil {
		fatal(err)
	}
	fmt.Printf("created repository %q uuid=%s\n", repoName, uuid)
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
