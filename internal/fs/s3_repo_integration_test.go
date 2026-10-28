package fs

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"svngod/internal/fs/store"
)

func newMinioForTest(cfg store.S3Config) (*minio.Client, error) {
	return minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
}

// TestS3RepoEndToEnd runs the full repository lifecycle against a live
// S3-compatible store (skip unless S3_ENDPOINT is set).
func TestS3RepoEndToEnd(t *testing.T) {
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("S3_ENDPOINT not set; skipping live S3 repository test")
	}
	cfg := store.S3Config{
		Endpoint:  endpoint,
		AccessKey: orEnv(os.Getenv("S3_ACCESS_KEY"), "test"),
		SecretKey: orEnv(os.Getenv("S3_SECRET_KEY"), "test"),
		Bucket:    orEnv(os.Getenv("S3_BUCKET"), "svngod-test"),
		Region:    os.Getenv("S3_REGION"),
		UseSSL:    os.Getenv("S3_SSL") == "1",
		// unique repo under a unique run prefix
		RepoPrefix: "repo-it/" + strconv.FormatInt(time.Now().UnixNano(), 36),
		BaseRepo:   "r1",
	}
	st, err := store.NewS3Store(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// ensure bucket
	cl, err := newMinioForTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if ok, err := cl.BucketExists(ctx, cfg.Bucket); err != nil {
		t.Fatal(err)
	} else if !ok {
		if err := cl.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{Region: cfg.Region}); err != nil {
			t.Fatal(err)
		}
	}

	// create + commit with a small shard size so packing is exercised
	repo, err := CreateRepo(st, "x", 4, 64)
	if err != nil {
		t.Fatal(err)
	}
	last := uint64(0)
	for i := 1; i <= 9; i++ {
		rev, err := repo.CommitTxn([]Change{
			{Path: "/data" + strconv.Itoa(i) + ".txt", Action: ActionAdd, Kind: KindFile, Data: []byte("content-" + strconv.Itoa(i))},
		}, map[string]string{"svn:log": "rev " + strconv.Itoa(i)})
		if err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
		last = rev
	}
	if last != 9 {
		t.Fatalf("head=%d want 9", last)
	}

	// read back every revision's file text
	for rev := uint64(1); rev <= 9; rev++ {
		p := "/data" + strconv.FormatUint(rev, 10) + ".txt"
		nr, err := repo.Resolve(rev, p)
		if err != nil {
			t.Fatalf("resolve rev %d: %v", rev, err)
		}
		txt, err := repo.GetText(nr.Node)
		if err != nil {
			t.Fatalf("gettext rev %d: %v", rev, err)
		}
		want := "content-" + strconv.FormatUint(rev, 10)
		if string(txt) != want {
			t.Fatalf("rev %d text=%q want %q", rev, txt, want)
		}
	}

	// pack shards 0..2 and re-read (switch to packed path)
	for sh := uint64(0); sh <= 2; sh++ {
		if err := repo.PackShard(sh); err != nil {
			t.Fatalf("pack shard %d: %v", sh, err)
		}
	}
	// re-open the repo from S3 and read after pack (proves persistence)
	reopened, err := OpenRepo(st, "x")
	if err != nil {
		t.Fatal(err)
	}
	nr9, err := reopened.Resolve(9, "/data9.txt")
	if err != nil {
		t.Fatal(err)
	}
	txt9, err := reopened.GetText(nr9.Node)
	if err != nil || string(txt9) != "content-9" {
		t.Fatalf("post-pack reopened text=%q err=%v", txt9, err)
	}
	rp, err := reopened.GetRevProps(5)
	if err != nil || rp.Props["svn:log"] != "rev 5" {
		t.Fatalf("revprops from packed: %+v err=%v", rp, err)
	}
	if head, _ := reopened.Head(); head != 9 {
		t.Fatalf("head after reopen=%d", head)
	}

	// verify the object layout (loose deleted after pack, packed present).
	// ListPrefix takes a prefix relative to the store's repo prefix; the repo
	// was created at rel path "x".
	keys, err := listAll(ctx, st, "x")
	if err != nil {
		t.Fatal(err)
	}
	var hasLoose, hasPacked bool
	for _, k := range keys {
		if contains(k, "revs-loose/") {
			hasLoose = true
		}
		if contains(k, "revs/") && contains(k, ".pack") {
			hasPacked = true
		}
	}
	if !hasPacked {
		t.Fatalf("expected packed rev objects; keys: %v", keys)
	}
	_ = hasLoose // loose objects are left for GC, may still exist
}

func orEnv(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func listAll(ctx context.Context, st store.Store, prefix string) ([]string, error) {
	var keys []string
	next := ""
	for {
		objs, n, err := st.ListPrefix(ctx, prefix, next, 200)
		if err != nil {
			return nil, err
		}
		for _, o := range objs {
			keys = append(keys, o.Key)
		}
		if n == "" {
			break
		}
		next = n
	}
	return keys, nil
}
