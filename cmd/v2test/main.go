package main

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"os"

	"github.com/pierrec/lz4/v4"
	"svngod/internal/svndiff"
)

func main() {
	d, _ := os.ReadFile("/home/user/svngod/bad_delta_dump.bin")
	fmt.Printf("version=%d len=%d head=%x\n", d[3], len(d), d[:12])
	// window header: so=0 sl=0 tl=390 il=80 nl=5 (parse by hand from known capture)
	ins := d[10 : 10+80]
	fmt.Printf("ins[0:24]=%x\n", ins[:24])
	dst := make([]byte, 1<<16)
	n, err := lz4.UncompressBlock(ins, dst)
	fmt.Printf("lz4 ins: n=%d err=%v out=%x\n", n, err, dst[:n])
	z, zerr := zlib.NewReader(bytes.NewReader(ins))
	if zerr == nil {
		zo, _ := io.ReadAll(z)
		fmt.Printf("zlib ins: len=%d out=%x\n", len(zo), zo[:24])
	} else {
		fmt.Printf("zlib ins err=%v\n", zerr)
	// data section: nl=5, starts at offset 90
	data := d[90 : 90+5]
	fmt.Printf("data[0:5]=%x\n", data)
	dd := make([]byte, 1<<16)
	dn, derr := lz4.UncompressBlock(data, dd)
	fmt.Printf("lz4 data: n=%d err=%v out=%x\n", dn, derr, dd[:dn])
	fmt.Printf("body[8:24]=%x\n", d[8:24])
	fmt.Printf("body[80:100]=%x\n", d[80:100])
	fmt.Printf("body[90:120]=%x\n", d[90:120])
	// decode ins section as raw opcodes (v0-style), count new/target
	ru := func(b []byte, p *int) uint64 {
		c := b[*p]
		*p++
		if c&0x80 == 0 {
			return uint64(c)
		}
		if c&0xc0 == 0x80 {
			v := uint64(c&0x7f)<<7 | uint64(b[*p]&0x7f)
			*p += 1
			return v
		}
		if c&0xe0 == 0xc0 {
			v := uint64(c&0x3f)<<14 | uint64(b[*p]&0x7f)<<7 | uint64(b[*p+1]&0x7f)
			*p += 2
			return v
		}
		v := uint64(c&0x1f)<<21 | uint64(b[*p]&0x7f)<<14 | uint64(b[*p+1]&0x7f)<<7 | uint64(b[*p+2]&0x7f)
		*p += 3
		return v
	}
	insb := d[10 : 10+80]
	p := 0
	npos := uint64(0)
	tpos := uint64(0)
	for p < len(insb) {
		c := insb[p]
		p++
		act := (c >> 6) & 3
		ln := uint64(c & 0x3f)
		if ln == 0 {
			ln = ru(insb, &p)
		}
		var off uint64
		if act != 2 {
			off = ru(insb, &p)
		}
		fmt.Printf("op%d: act=%d len=%d off=%d\n", p-1, act, ln, off)
		if act == 2 {
			npos += ln
		}
		tpos += ln
	}
	fmt.Printf("ins: npos(new total)=%d tpos=%d\n", npos, tpos)
	}
	out, err := svndiff.Apply(nil, d)
	oh := ""
	if len(out) >= 12 {
		oh = fmt.Sprintf("%x", out[:12])
	}
	fmt.Printf("Apply err=%v outlen=%d outHead=%s\n", err, len(out), oh)
}
