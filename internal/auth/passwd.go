package auth

import (
	"crypto/sha1"
	"crypto/sha512"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
)

// VerifyPassword checks a submitted password against a stored password record.
// Stored formats (svnpasswd-style):
//
//	plaintext
//	{SHA}<base64 sha1>
//	{CRYPT}$6$<salt>$<sha512-crypt hash>   (and $5$ sha256 / $1$ md5 fallback rejected)
//
// The comparison is constant-time for digest forms where possible.
func VerifyPassword(stored, submitted string) bool {
	if strings.HasPrefix(stored, "{SHA}") {
		sum := sha1.Sum([]byte(submitted))
		got := base64.StdEncoding.EncodeToString(sum[:])
		return constantEqual(got, stored[len("{SHA}"):])
	}
	if strings.HasPrefix(stored, "{CRYPT}") {
		c := stored[len("{CRYPT}"):]
		if strings.HasPrefix(c, "$6$") {
			return verifySha512Crypt(c, submitted)
		}
		// {CRYPT}$1$/$5$/DES unsupported in this build — never succeed silently.
		return false
	}
	return constantEqual(stored, submitted)
}

func constantEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var v byte
	for i := range a {
		v |= a[i] ^ b[i]
	}
	return v == 0
}

// --- SHA-512 crypt ($6$), glibc-compatible ---

const b64t = "./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

func b64From24(b2, b1, b0 byte, n int) string {
	// glibc __b64_from_24bit: emit chars from LOW bits upward (w & 0x3f, then >>6).
	w := uint32(b2)<<16 | uint32(b1)<<8 | uint32(b0)
	var sb strings.Builder
	for i := 0; i < n; i++ {
		sb.WriteByte(b64t[w&0x3f])
		w >>= 6
	}
	return sb.String()
}

// verifySha512Crypt verifies a submitted password against a $6$ record.
func verifySha512Crypt(record, submitted string) bool {
	// record: $6$<salt-spec>$<hash>, salt-spec = [rounds=N$]<salt>
	rest := strings.TrimPrefix(record, "$6$")
	sep := strings.LastIndex(rest, "$")
	if sep < 0 {
		return false
	}
	saltSpec := rest[:sep]
	wantHash := rest[sep+1:]

	rounds := 5000
	salt := saltSpec
	if strings.HasPrefix(saltSpec, "rounds=") {
		idx := strings.IndexByte(saltSpec, '$')
		if idx < 0 {
			return false
		}
		r, err := strconv.Atoi(saltSpec[len("rounds="):idx])
		if err != nil || r < 1000 || r > 999999999 {
			return false
		}
		rounds = r
		salt = saltSpec[idx+1:]
	}
	if len(salt) > 16 {
		salt = salt[:16]
	}

	gotHash := sha512Crypt(submitted, salt, rounds)
	got := gotHash[strings.LastIndex(gotHash, "$")+1:]
	return constantEqual(got, wantHash)
}

// sha512Crypt implements the glibc SHA-512 crypt algorithm, verified against
// the reference vector ("Hello world!" / "saltstring") and openssl/python.
func sha512Crypt(password, salt string, rounds int) string {
	p := []byte(password)
	s := []byte(salt)

	// Alternate sum: sha512(p + s + p)
	alt := sha512.New()
	alt.Write(p)
	alt.Write(s)
	alt.Write(p)
	altSum := alt.Sum(nil)

	// Intermediate sum: sha512(p + s + cycle(altSum, len(p)) + per-bit blocks).
	// glibc adds alt_result cyclically to cover key_len bytes total
	// (while(cnt>64) cnt-=64 {+64} then +cnt).
	intm := sha512.New()
	intm.Write(p)
	intm.Write(s)
	intm.Write(cycle(altSum, len(p)))
	for cnt := int64(len(p)); cnt > 0; cnt >>= 1 {
		if cnt&1 != 0 {
			intm.Write(altSum)
		} else {
			intm.Write(p)
		}
	}
	cur := intm.Sum(nil) // alt_result (running digest)

	// P sequence: digest of (p repeated len(p) times), cycled to len(p) bytes.
	t := sha512.New()
	for i := 0; i < len(p); i++ {
		t.Write(p)
	}
	pSeq := cycle(t.Sum(nil), len(p))

	// S sequence: digest of (s repeated (16 + cur[0]) times), cycled to len(s).
	t2 := sha512.New()
	for i := 0; i < 16+int(cur[0]); i++ {
		t2.Write(s)
	}
	sSeq := cycle(t2.Sum(nil), len(s))

	// Iterated rounds using P and S.
	for i := 0; i < rounds; i++ {
		h := sha512.New()
		if i&1 != 0 {
			h.Write(pSeq)
		} else {
			h.Write(cur)
		}
		if i%3 != 0 {
			h.Write(sSeq)
		}
		if i%7 != 0 {
			h.Write(pSeq)
		}
		if i&1 != 0 {
			h.Write(cur)
		} else {
			h.Write(pSeq)
		}
		cur = h.Sum(nil)
	}

	// Constant-time-ish final assembly from 64-byte digest d[0..63].
	d := cur
	var sb strings.Builder
	sb.WriteString("$6$")
	sb.WriteString(salt)
	sb.WriteByte('$')
	blk := func(a, b, c int) { sb.WriteString(b64From24(d[a], d[b], d[c], 4)) }
	blk(0, 21, 42)
	blk(22, 43, 1)
	blk(44, 2, 23)
	blk(3, 24, 45)
	blk(25, 46, 4)
	blk(47, 5, 26)
	blk(6, 27, 48)
	blk(28, 49, 7)
	blk(50, 8, 29)
	blk(9, 30, 51)
	blk(31, 52, 10)
	blk(53, 11, 32)
	blk(12, 33, 54)
	blk(34, 55, 13)
	blk(56, 14, 35)
	blk(15, 36, 57)
	blk(37, 58, 16)
	blk(59, 17, 38)
	blk(18, 39, 60)
	blk(40, 61, 19)
	blk(62, 20, 41)
	// Final two characters: b64_from_24bit(0, 0, d[63], 2).
	c := uint32(d[63])
	sb.WriteByte(b64t[c&0x3f])
	sb.WriteByte(b64t[(c>>6)&0x3f])
	return sb.String() // full "$6$<salt>$<hash>"
}

var _ = errors.New // placeholder to keep import if unused paths change

// cycle repeats a digest cyclically to exactly n bytes (glibc mempcpy from
// the 64-byte digest, truncated to the target length).
func cycle(d []byte, n int) []byte {
	out := make([]byte, n)
	for i := 0; i < n; i++ {
		out[i] = d[i%len(d)]
	}
	return out
}
