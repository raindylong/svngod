package store

import (
	"fmt"
	"strings"
)

// Keys builds object keys for a single repository. All keys are rooted under
// a repo prefix. Numeric parts are zero-padded so ListPrefix enumeration is
// lexicographically ordered.
type Keys struct {
	Prefix string // e.g. "repos/alpha"
}

func NewKeys(prefix string) *Keys { return &Keys{Prefix: prefix} }

func (k *Keys) p(parts ...string) string {
	s := k.Prefix
	for _, p := range parts {
		s += "/" + p
	}
	return s
}

// Format returns the storage-format marker key.
func (k *Keys) Format() string { return k.p("format") }

// UUID returns the repository uuid key.
func (k *Keys) UUID() string { return k.p("uuid") }

// Current returns the HEAD pointer key (CAS target).
func (k *Keys) Current() string { return k.p("current") }

func padShard(s uint64) string { return fmt.Sprintf("%010d", s) }
func padRev(r uint64) string   { return fmt.Sprintf("%012d", r) }

// PackedRev returns the packed revision-shard pack key.
func (k *Keys) PackedRev(shard uint64) string { return k.p("revs", padShard(shard)+".pack") }

// PackedIdx returns the packed revision-shard index key.
func (k *Keys) PackedIdx(shard uint64) string { return k.p("revs", padShard(shard)+".idx") }

// PackedDone returns the shard pack-completion marker key.
func (k *Keys) PackedDone(shard uint64) string { return k.p("revs", padShard(shard)+".done") }

// PackedRevProps returns the packed revision-properties key.
func (k *Keys) PackedRevProps(shard uint64) string { return k.p("revprops", padShard(shard)+".pack") }

// Segment names a batched revision segment (format v2).
func (k *Keys) Segment(shard, first, count uint64) string {
	return k.p("segs", padShard(shard), fmt.Sprintf("%012d-%012d.seg", first, count))
}

// Dir returns a listing prefix for the given key parts. Listing returns keys
// relative to the store prefix (no leading slash), while Keys methods keep the
// leading slash of an empty repository prefix, so GC must compare both sides in
// this form or it will treat every live object as unreferenced.
func (k *Keys) ListingPrefix(parts ...string) string {
	return strings.TrimPrefix(k.p(parts...), "/") + "/"
}

// SegmentDir is the listing prefix of one shard's segment objects.
func (k *Keys) SegmentDir(shard uint64) string {
	return k.p("segs", padShard(shard)) + "/"
}

// SegRevPropsOverride holds mutable revision properties for a revision whose
// committed copy lives inside an immutable segment. Written only after a
// revprop edit; absent for every other revision.
func (k *Keys) SegRevPropsOverride(shard, rev uint64) string {
	return k.p("seg-revprops", padShard(shard), padRev(rev))
}

// SegRevPropsIndex lists the revisions of one shard that have an override, so
// the read path needs no extra GET for the (overwhelmingly common) revisions
// whose properties only exist in the segment.
func (k *Keys) SegRevPropsIndex(shard uint64) string {
	return k.p("seg-revprops", padShard(shard)+".idx")
}

// SegmentIndex names the per-shard segment index (format v2).
func (k *Keys) SegmentIndex(shard uint64) string {
	return k.p("segs", padShard(shard)+".idx")
}

// LooseRevDir lists one shard's loose revision-block objects.
func (k *Keys) LooseRevDir(shard uint64) string {
	return k.p("revs-loose", padShard(shard)) + "/"
}

// LooseRevPropsDir lists one shard's loose revision-property objects.
func (k *Keys) LooseRevPropsDir(shard uint64) string {
	return k.p("revprops-loose", padShard(shard)) + "/"
}

// LooseRev returns the loose (active-shard) revision-block key.
func (k *Keys) LooseRev(shard, rev uint64) string {
	return k.p("revs-loose", padShard(shard), padRev(rev))
}

// LooseRevProps returns the loose revision-properties key.
func (k *Keys) LooseRevProps(shard, rev uint64) string {
	return k.p("revprops-loose", padShard(shard), padRev(rev))
}

// Txn returns the transaction staging prefix.
func (k *Keys) TxnPrefix(txid string) string { return k.p("txn", txid) + "/" }

// Text returns the external file-text object key for a content reference.
func (k *Keys) Text(ref string) string { return k.p("texts", ref) }

// Chunk returns the key of one content-defined chunk (hex sha256).
func (k *Keys) Chunk(hash string) string { return k.p("chunks", hash) }

// Dir returns the key of a large-directory table or bucket object.
func (k *Keys) Dir(hash string) string { return k.p("dirs", hash) }

// Journal returns the write-ahead commit record key for one revision.
func (k *Keys) Journal(rev uint64) string { return k.p("journal", padRev(rev)) }

// JournalPrefix is the common prefix of all journal records.
func (k *Keys) JournalPrefix() string { return k.p("journal") + "/" }

// CommitLock returns the commit-lock object key.
func (k *Keys) CommitLock() string { return k.p("locks", "commit") }

// Activity returns the DeltaV activity mapping key.
func (k *Keys) Activity(uuid string) string { return k.p("dav-activities", uuid) }
