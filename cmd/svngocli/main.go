// Command svngocli is a Subversion client front-end for svngod repositories.
//
// Every native svn subcommand is passed through to the installed `svn` binary,
// so the full feature set keeps working. On top of that:
//
//   - read-only commands retry transient connection failures;
//   - `commit` verifies whether a failed commit actually landed before it
//     retries, so a retried commit cannot create a duplicate revision;
//   - `upload` uses the negotiated bulk commit channel (HTTPv2 style): one
//     request, one atomic revision for a whole directory tree, with a
//     content-derived token that makes retries idempotent.
//
// usage:
//
//	svngocli <svn arguments...>              passthrough (checkout, update, ...)
//	svngocli upload <dir> <url> [flags]     atomic bulk upload of a tree
//	svngocli commit <svn commit args...>    retry-safe commit
package main

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"
)

var errOutOfDate = errors.New("target changed on the server")

const (
	version        = "0.1.0"
	bundleMagic    = "SVNGB1\n"
	maxFileSize    = 16 << 20
	maxUploadTotal = 512 << 20
)

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}
	switch args[0] {
	case "help", "--help", "-h":
		usage()
	case "version", "--version", "-V":
		fmt.Printf("svngocli %s (svngod bulk commit client)\n", version)
	case "upload":
		os.Exit(cmdUpload(args[1:]))
	case "commit", "ci":
		os.Exit(cmdCommit(args))
	default:
		os.Exit(runSvn(args, isReadOnly(args[0])))
	}
}

func usage() {
	fmt.Print(`svngocli - Subversion client for svngod

  svngocli <svn command> [args...]    run any native svn command (full parity)
  svngocli commit [args...]           svn commit with safe retry/verification
  svngocli upload <dir> <repo-url>    atomic bulk upload of a directory tree
      -m, --message MSG               commit message
      --delete-missing                delete server files absent locally
      --username USER --password PASS credentials
      --quiet                         only print the revision
  svngocli version

Read-only commands (checkout/update/log/...) are retried on transient
connection errors. upload requires an svngod server advertising
X-Svngod-Bundle; otherwise it falls back to 'svn import' for new paths.
`)
}

// --- native svn passthrough ---

var readOnlyCmds = map[string]bool{
	"checkout": true, "co": true, "update": true, "up": true, "export": true,
	"cat": true, "log": true, "ls": true, "list": true, "info": true,
	"status": true, "st": true, "diff": true, "di": true, "blame": true,
	"praise": true, "annotate": true, "proplist": true, "pl": true,
	"propget": true, "pget": true, "help": true,
}

func isReadOnly(cmd string) bool { return readOnlyCmds[cmd] }

func runSvn(args []string, retry bool) int {
	if !retry {
		cmd := exec.Command("svn", args...)
		cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
		if err := cmd.Run(); err != nil {
			return exitCode(err)
		}
		return 0
	}
	backoff := time.Second
	for attempt := 0; attempt < 3; attempt++ {
		var out bytes.Buffer
		cmd := exec.Command("svn", args...)
		cmd.Stdin = os.Stdin
		cmd.Stdout = &out
		cmd.Stderr = &out
		err := cmd.Run()
		if err == nil {
			os.Stdout.Write(out.Bytes())
			return 0
		}
		text := out.String()
		os.Stderr.WriteString(text)
		if !isTransient(text) || attempt == 2 {
			return exitCode(err)
		}
		fmt.Fprintf(os.Stderr, "svngocli: transient failure, retrying in %v\n", backoff)
		time.Sleep(backoff)
		backoff *= 2
	}
	return 1
}

func exitCode(err error) int {
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	return 1
}

func isTransient(out string) bool {
	for _, s := range []string{
		"Connection refused", "Connection reset", "connection reset",
		"timed out", "Operation timed out", "Timeout",
		"E175002", "E170013", "Could not resolve host",
		"502 Bad Gateway", "503 Service", "504 Gateway",
		"unexpected server error 5", "Unexpected server error 5",
	} {
		if strings.Contains(out, s) {
			return true
		}
	}
	return false
}

// --- commit with outcome verification ---

func cmdCommit(args []string) int {
	msg, msgKnown := commitMessage(args)
	svnUser = flagValue(args, "--username")
	svnPass = flagValue(args, "--password")
	wcURL, head := svnURLAndHead(".")
	for attempt := 0; attempt < 3; attempt++ {
		var out bytes.Buffer
		cmd := exec.Command("svn", args...)
		cmd.Stdin = os.Stdin
		cmd.Stdout = &out
		cmd.Stderr = &out
		err := cmd.Run()
		os.Stdout.Write(out.Bytes())
		if err == nil {
			return 0
		}
		if !isTransient(out.String()) {
			return exitCode(err)
		}
		// Before retrying, check whether the "failed" commit actually landed.
		if head > 0 && wcURL != "" {
			if rev, ok := landedCommit(wcURL, head, msg, msgKnown); ok {
				fmt.Fprintf(os.Stderr, "svngocli: commit reached the server despite the error (r%d)\n", rev)
				return 0
			}
		}
		if !msgKnown {
			fmt.Fprintln(os.Stderr, "svngocli: cannot verify the commit outcome (no -m/-F message); not retrying to avoid a duplicate revision")
			return exitCode(err)
		}
		fmt.Fprintf(os.Stderr, "svngocli: commit failed before landing, retrying (attempt %d/3)\n", attempt+2)
		time.Sleep(time.Duration(attempt+1) * time.Second)
	}
	return 1
}

func flagValue(args []string, name string) string {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == name {
			return args[i+1]
		}
		if strings.HasPrefix(args[i], name+"=") {
			return strings.TrimPrefix(args[i], name+"=")
		}
	}
	return ""
}

func commitMessage(args []string) (string, bool) {
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-m", "--message":
			if i+1 < len(args) {
				return args[i+1], true
			}
		case "-F", "--file":
			if i+1 < len(args) {
				if b, err := os.ReadFile(args[i+1]); err == nil {
					return string(b), true
				}
			}
		}
	}
	return "", false
}

// landedCommit reports whether a revision with our message appeared after the
// recorded HEAD. Used to decide whether a failed commit must be retried.
func landedCommit(repoURL string, oldHead uint64, msg string, msgKnown bool) (uint64, bool) {
	if !msgKnown {
		return 0, false
	}
	out, err := runSvnOutput([]string{"log", "--xml", "-r", fmt.Sprintf("%d:HEAD", oldHead+1), "-l", "5", repoURL, "--non-interactive"})
	if err != nil {
		return 0, false
	}
	var doc struct {
		Logs []struct {
			Revision string `xml:"revision,attr"`
			Msg      string `xml:"msg"`
		} `xml:"logentry"`
	}
	if xml.Unmarshal(out, &doc) != nil {
		return 0, false
	}
	for _, le := range doc.Logs {
		if strings.TrimSpace(le.Msg) == strings.TrimSpace(msg) && strings.TrimSpace(msg) != "" {
			var rev uint64
			fmt.Sscanf(le.Revision, "%d", &rev)
			return rev, true
		}
	}
	return 0, false
}

func svnURLAndHead(target string) (string, uint64) {
	out, err := runSvnOutput([]string{"info", "--xml", "--non-interactive", target})
	if err != nil {
		return "", 0
	}
	var doc struct {
		Entry struct {
			Rev string `xml:"revision,attr"`
			URL string `xml:"url"`
		} `xml:"entry"`
	}
	if xml.Unmarshal(out, &doc) != nil {
		return "", 0
	}
	var rev uint64
	fmt.Sscanf(doc.Entry.Rev, "%d", &rev)
	return doc.Entry.URL, rev
}

// svnUser/svnPass carry credentials parsed from the invocation, for the
// helper svn calls (info/log/ls) the client makes on the user's behalf.
var (
	svnUser string
	svnPass string
)

func authFlags() []string {
	f := []string{"--non-interactive"}
	if svnUser != "" {
		f = append(f, "--username", svnUser, "--password", svnPass, "--no-auth-cache")
	}
	return f
}

func runSvnOutput(args []string) ([]byte, error) {
	args = append(args, authFlags()...)
	cmd := exec.Command("svn", args...)
	var errb bytes.Buffer
	cmd.Stderr = &errb
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("svn %s: %v: %s", strings.Join(args, " "), err, errb.String())
	}
	return out, nil
}

// --- bulk upload (svngod bundle channel) ---

type bundleChange struct {
	Path    string            `json:"path"`
	Action  string            `json:"action"`
	Kind    string            `json:"kind"`
	BaseRev uint64            `json:"baseRev,omitempty"`
	Props   map[string]string `json:"props,omitempty"`
	TextLen int64             `json:"textLen,omitempty"`
}

type bundleHeader struct {
	Token   string         `json:"token"`
	BaseRev uint64         `json:"baseRev"`
	Message string         `json:"message"`
	Changes []bundleChange `json:"changes"`
}

type uploadFile struct {
	local    string // absolute path on disk
	repoPath string // "/dir/file"
	size     int64
	sum      [32]byte
	props    map[string]string // binary files get svn:mime-type like svn import
}

func cmdUpload(args []string) int {
	var (
		msg     string
		user    string
		pass    string
		delMiss bool
		quiet   bool
		pos     []string
	)
	// Flags may appear before or after the two positionals.
	for i := 0; i < len(args); i++ {
		a := args[i]
		val := func() (string, bool) {
			if eq := strings.IndexByte(a, '='); eq >= 0 {
				return a[eq+1:], true
			}
			if i+1 < len(args) {
				i++
				return args[i], true
			}
			return "", false
		}
		switch {
		case a == "-m" || a == "--message" || strings.HasPrefix(a, "--message=") || strings.HasPrefix(a, "-m="):
			if v, ok := val(); ok {
				msg = v
			}
		case a == "--username" || strings.HasPrefix(a, "--username="):
			if v, ok := val(); ok {
				user = v
			}
		case a == "--password" || strings.HasPrefix(a, "--password="):
			if v, ok := val(); ok {
				pass = v
			}
		case a == "--delete-missing":
			delMiss = true
		case a == "--quiet" || a == "-q":
			quiet = true
		case strings.HasPrefix(a, "-"):
			fmt.Fprintf(os.Stderr, "svngocli: unknown upload flag %s\n", a)
			return 2
		default:
			pos = append(pos, a)
		}
	}
	if len(pos) != 2 {
		fmt.Fprintln(os.Stderr, "usage: svngocli upload <dir> <repo-url> [-m MSG] [--delete-missing]")
		return 2
	}
	localDir, targetURL := pos[0], pos[1]
	svnUser, svnPass = user, pass
	info, err := os.Stat(localDir)
	if err != nil || !info.IsDir() {
		fmt.Fprintf(os.Stderr, "svngocli: %s is not a directory\n", localDir)
		return 1
	}

	repoRoot, targetPath, err := resolveRepoRoot(targetURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "svngocli: %v\n", err)
		return 1
	}
	head, bundleOK, err := probeBundle(repoRoot, user, pass)
	if err != nil {
		fmt.Fprintf(os.Stderr, "svngocli: %v\n", err)
		return 1
	}
	if !bundleOK {
		// Fall back to svn import when the target does not exist yet.
		if _, err := runSvnOutput([]string{"ls", targetURL}); err != nil {
			fmt.Fprintln(os.Stderr, "svngocli: server has no bulk commit channel; falling back to svn import")
			ia := []string{"import", "-m", msg, localDir, targetURL}
			ia = append(ia, authFlags()...)
			return runSvn(ia, false)
		}
		fmt.Fprintln(os.Stderr, "svngocli: server has no bulk commit channel (upgrade svngod or use svn commit)")
		return 1
	}

	// Scan the tree, hashing every file. The token is content derived, so
	// re-running the same upload is a no-op instead of a second revision.
	var files []uploadFile
	var total int64
	digest := sha256.New()
	fmt.Fprintf(digest, "%s\x00%s\x00%s\x00", repoRoot, targetPath, msg)
	err = filepath.WalkDir(localDir, func(p string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			if d.Name() == ".svn" {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink %s is not supported by bulk upload; use svn import", p)
		}
		if !d.Type().IsRegular() {
			return nil
		}
		fi, ferr := d.Info()
		if ferr != nil {
			return ferr
		}
		if fi.Size() > maxFileSize {
			return fmt.Errorf("%s is %d bytes (limit %d); use svn commit for large files", p, fi.Size(), maxFileSize)
		}
		total += fi.Size()
		if total > maxUploadTotal {
			return fmt.Errorf("upload exceeds %d bytes; split it or use svn commit", maxUploadTotal)
		}
		rel, rerr := filepath.Rel(localDir, p)
		if rerr != nil {
			return rerr
		}
		sum, serr := hashFile(p)
		if serr != nil {
			return serr
		}
		rp := joinRepoPath(targetPath, filepath.ToSlash(rel))
		fmt.Fprintf(digest, "%s\x00%d\x00%x\x00", rp, fi.Size(), sum)
		files = append(files, uploadFile{local: p, repoPath: rp, size: fi.Size(), sum: sum, props: sniffProps(p)})
		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "svngocli: %v\n", err)
		return 1
	}

	changes := make([]bundleChange, 0, len(files))
	for _, f := range files {
		changes = append(changes, bundleChange{Path: f.repoPath, Action: "put", Kind: "file", BaseRev: head, TextLen: f.size, Props: f.props})
	}
	if delMiss {
		gone, gerr := missingOnServer(targetURL, targetPath, files, user, pass)
		if gerr != nil {
			fmt.Fprintf(os.Stderr, "svngocli: list server tree: %v\n", gerr)
			return 1
		}
		for _, rp := range gone {
			changes = append(changes, bundleChange{Path: rp, Action: "delete", Kind: "file", BaseRev: head})
			fmt.Fprintf(digest, "del\x00%s\x00", rp)
		}
	}
	token := hex.EncodeToString(digest.Sum(nil))

	body, err := buildBundleFile(bundleHeader{Token: token, BaseRev: head, Message: msg, Changes: changes}, files)
	if err != nil {
		fmt.Fprintf(os.Stderr, "svngocli: %v\n", err)
		return 1
	}
	defer os.Remove(body)
	rev, replayed, err := postBundle(repoRoot, body, user, pass)
	if err != nil {
		fmt.Fprintf(os.Stderr, "svngocli: upload failed: %v\n", err)
		return 1
	}
	if replayed {
		if !quiet {
			fmt.Printf("Already committed as r%d (identical content); nothing to do.\n", rev)
		} else {
			fmt.Printf("%d\n", rev)
		}
		return 0
	}
	if quiet {
		fmt.Printf("%d\n", rev)
	} else {
		fmt.Printf("Committed revision %d (%d files).\n", rev, len(files))
	}
	return 0
}

// sniffProps mirrors svn import's binary detection: a NUL byte in the first
// few KiB marks the file application/octet-stream.
func sniffProps(p string) map[string]string {
	f, err := os.Open(p)
	if err != nil {
		return nil
	}
	defer f.Close()
	buf := make([]byte, 8000)
	n, _ := f.Read(buf)
	if bytes.IndexByte(buf[:n], 0) >= 0 {
		return map[string]string{"svn:mime-type": "application/octet-stream"}
	}
	return nil
}

func hashFile(p string) ([32]byte, error) {
	f, err := os.Open(p)
	if err != nil {
		return [32]byte{}, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return [32]byte{}, err
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out, nil
}

func joinRepoPath(base, rel string) string {
	p := path.Join("/", strings.TrimSuffix(base, "/"), rel)
	return path.Clean(p)
}

// resolveRepoRoot returns the repository root URL and the target path inside
// the repository for a possibly-not-yet-existing URL.
func resolveRepoRoot(targetURL string) (string, string, error) {
	u := strings.TrimSuffix(targetURL, "/")
	for {
		out, err := runSvnOutput([]string{"info", "--show-item", "repos-root-url", u})
		if err == nil {
			root := strings.TrimSpace(string(out))
			return root, repoPathOf(targetURL, root), nil
		}
		parent := parentURL(u)
		if parent == u || parent == "" {
			return "", "", fmt.Errorf("cannot determine repository root for %s", targetURL)
		}
		u = parent
	}
}

func repoPathOf(target, root string) string {
	t := strings.TrimSuffix(target, "/")
	r := strings.TrimSuffix(root, "/")
	if t == r {
		return "/"
	}
	if strings.HasPrefix(t, r+"/") {
		return path.Clean("/" + strings.TrimPrefix(t, r))
	}
	return "/"
}

func parentURL(u string) string {
	parsed, err := url.Parse(u)
	if err != nil {
		return ""
	}
	parsed.Path = path.Dir(strings.TrimSuffix(parsed.Path, "/"))
	if strings.HasSuffix(parsed.Path, "/.") {
		return ""
	}
	return strings.TrimSuffix(parsed.String(), "/")
}

// probeBundle negotiates the bulk channel and returns the repository HEAD.
func probeBundle(repoRoot, user, pass string) (uint64, bool, error) {
	req, err := http.NewRequest("OPTIONS", strings.TrimSuffix(repoRoot, "/")+"/", nil)
	if err != nil {
		return 0, false, err
	}
	setAuth(req, user, pass)
	req.Header.Set("User-Agent", "svngocli/"+version)
	resp, err := httpClient().Do(req)
	if err != nil {
		return 0, false, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode == http.StatusUnauthorized {
		return 0, false, fmt.Errorf("authentication required (use --username/--password)")
	}
	var head uint64
	fmt.Sscanf(resp.Header.Get("SVN-Youngest-Rev"), "%d", &head)
	ok := resp.Header.Get("X-Svngod-Bundle") == "1"
	if head == 0 {
		if out, err := runSvnOutput([]string{"info", "--show-item", "revision", "--non-interactive", repoRoot + "@HEAD"}); err == nil {
			fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &head)
		}
	}
	return head, ok, nil
}

// missingOnServer lists the server tree under target and returns repo paths
// that have no local counterpart.
func missingOnServer(targetURL, targetPath string, files []uploadFile, user, pass string) ([]string, error) {
	out, err := runSvnOutput([]string{"ls", "--xml", "-R", targetURL})
	if err != nil {
		// A missing target simply has nothing to delete.
		return nil, nil
	}
	var doc struct {
		Entries []struct {
			Kind string `xml:"kind,attr"`
			Name string `xml:"name"`
		} `xml:"list>entry"`
	}
	if err := xml.Unmarshal(out, &doc); err != nil {
		return nil, err
	}
	have := make(map[string]bool, len(files))
	for _, f := range files {
		have[f.repoPath] = true
	}
	var gone []string
	for _, e := range doc.Entries {
		if e.Kind == "dir" {
			continue
		}
		rp := joinRepoPath(targetPath, e.Name)
		if !have[rp] {
			gone = append(gone, rp)
		}
	}
	return gone, nil
}

// buildBundleFile writes the framed bundle to a temp file. The file is kept
// (seekable) so the POST can be retried without rebuilding.
func buildBundleFile(hdr bundleHeader, files []uploadFile) (string, error) {
	tmp, err := os.CreateTemp("", "svngocli-bundle-")
	if err != nil {
		return "", err
	}
	cleanup := func() {
		tmp.Close()
		os.Remove(tmp.Name())
	}
	hj, err := json.Marshal(hdr)
	if err != nil {
		cleanup()
		return "", err
	}
	var sz [8]byte
	binary.BigEndian.PutUint64(sz[:], uint64(len(hj)))
	if _, err := tmp.WriteString(bundleMagic); err != nil {
		cleanup()
		return "", err
	}
	if _, err := tmp.Write(sz[:]); err != nil {
		cleanup()
		return "", err
	}
	if _, err := tmp.Write(hj); err != nil {
		cleanup()
		return "", err
	}
	for _, f := range files {
		in, oerr := os.Open(f.local)
		if oerr != nil {
			cleanup()
			return "", oerr
		}
		n, cerr := io.CopyN(tmp, in, f.size)
		in.Close()
		if cerr != nil || n != f.size {
			cleanup()
			return "", fmt.Errorf("read %s: copied %d/%d (%v)", f.local, n, f.size, cerr)
		}
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return "", err
	}
	return tmp.Name(), nil
}

// postBundle sends the file with retries; the token makes repeats idempotent.
func postBundle(repoRoot, bodyPath, user, pass string) (uint64, bool, error) {
	endpoint := strings.TrimSuffix(repoRoot, "/") + "/!svngod/bundle"
	backoff := time.Second
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		f, err := os.Open(bodyPath)
		if err != nil {
			return 0, false, err
		}
		fi, _ := f.Stat()
		req, rerr := http.NewRequest("POST", endpoint, f)
		if rerr != nil {
			f.Close()
			return 0, false, rerr
		}
		req.ContentLength = fi.Size()
		req.Header.Set("Content-Type", "application/x-svngod-bundle")
		req.Header.Set("User-Agent", "svngocli/"+version)
		setAuth(req, user, pass)
		resp, derr := httpClient().Do(req)
		f.Close()
		if derr != nil {
			lastErr = derr
		} else {
			payload, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
			var out struct {
				Revision uint64 `json:"revision"`
				Replayed bool   `json:"replayed"`
				Error    *struct {
					Code    string `json:"code"`
					Message string `json:"message"`
				} `json:"error"`
			}
			_ = json.Unmarshal(payload, &out) // success or structured error
			if resp.StatusCode == 200 && out.Revision > 0 {
				return out.Revision, out.Replayed, nil
			}
			if out.Error != nil {
				lastErr = fmt.Errorf("%s: %s", out.Error.Code, out.Error.Message)
				if out.Error.Code == "out-of-date" {
					// A concurrent commit changed a path in this tree. Retrying
					// the same bundle cannot succeed; the user re-runs it.
					return 0, false, fmt.Errorf("%w (re-run after updating the target)", errOutOfDate)
				}
			} else {
				lastErr = fmt.Errorf("server returned %d: %s", resp.StatusCode, strings.TrimSpace(string(payload)))
			}
			if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != http.StatusConflict {
				return 0, false, lastErr // auth/permission/bad request: do not retry
			}
		}
		if attempt < 4 {
			fmt.Fprintf(os.Stderr, "svngocli: upload attempt %d failed (%v); retrying in %v\n", attempt+1, lastErr, backoff)
			time.Sleep(backoff)
			backoff *= 2
		}
	}
	return 0, false, lastErr
}

func setAuth(req *http.Request, user, pass string) {
	if user != "" {
		req.SetBasicAuth(user, pass)
	}
}

func httpClient() *http.Client {
	return &http.Client{
		Timeout: 0,
		Transport: &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			TLSClientConfig:     &tls.Config{},
			MaxIdleConnsPerHost: 4,
			IdleConnTimeout:     90 * time.Second,
		},
	}
}
