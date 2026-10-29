package main

import (
	"fmt"
	"os"

	"svngod/internal/svndiff"
)

func ru(b []byte, p *int) uint64 {
	// svn__decode_uint: little-endian base-128; stop at first byte with high bit clear.
	var temp uint64
	for *p < len(b) {
		c := b[*p]
		*p++
		if c < 0x80 {
			return (temp << 7) | uint64(c)
		}
		temp = (temp << 7) | uint64(c&0x7f)
	}
	return 0
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	// 1) svnserve's stored v2 delta
	analyzeSvnserve()
	// 2) svngod's captured HTTP v1 delta, decoded with the fixed semantics
	analyzeBadDelta()
}

func analyzeSvnserve() {
	r, _ := os.ReadFile("/home/user/svngod/serveside/repo/db/revs/0/3")
	idx := 0
	for i := 0; i+3 < len(r); i++ {
		if r[i] == 'S' && r[i+1] == 'V' && r[i+2] == 'N' && r[i+3] == 2 {
			idx = i
			break
		}
	}
	d := r[idx:]
	p := 4
	so := ru(d, &p)
	sl := ru(d, &p)
	tl := ru(d, &p)
	il := ru(d, &p)
	nl := ru(d, &p)
	fmt.Printf("[svnserve v2] win: so=%d sl=%d tl=%d il=%d nl=%d ins_at=%d\n", so, sl, tl, il, nl, p)
	out, err := svndiff.Apply(nil, d)
	fmt.Printf("[svnserve v2] Apply err=%v outlen=%d\n", err, len(out))
}

func analyzeBadDelta() {
	d, _ := os.ReadFile("/home/user/svngod/bad_delta_dump.bin")
	if len(d) < 4 {
		fmt.Printf("[bad_delta] short\n")
		return
	}
	p := 4
	so := ru(d, &p)
	sl := ru(d, &p)
	tl := ru(d, &p)
	il := ru(d, &p)
	nl := ru(d, &p)
	fmt.Printf("[bad_delta v%d] len=%d win: so=%d sl=%d tl=%d il=%d nl=%d ins_at=%d\n",
		d[3], len(d), so, sl, tl, il, nl, p)
	out, err := svndiff.Apply(nil, d)
	fmt.Printf("[bad_delta v%d] Apply err=%v outlen=%d outHead=%x\n",
		d[3], err, len(out), out[:min(16, len(out))])
}
