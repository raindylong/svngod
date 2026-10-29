package main

// deltatest: 对比 svn 客户端 dump 与 svngod Encode 的 svndiff window 结构
import (
	"bytes"
	"fmt"
	"math/rand"
	"os"

	"svngod/internal/svndiff"
)

func readUint(b []byte) (uint64, int, error) {
	if len(b) == 0 {
		return 0, 0, fmt.Errorf("eof")
	}
	c := b[0]
	var v uint64
	var n int
	switch {
	case c&0x80 == 0:
		v = uint64(c)
		n = 1
	case c&0xc0 == 0x80:
		if len(b) < 2 {
			return 0, 0, fmt.Errorf("short")
		}
		v = uint64(c&0x7f)<<7 | uint64(b[1]&0x7f)
		n = 2
	case c&0xe0 == 0xc0:
		if len(b) < 3 {
			return 0, 0, fmt.Errorf("short")
		}
		v = uint64(c&0x3f)<<14 | uint64(b[1]&0x7f)<<7 | uint64(b[2]&0x7f)
		n = 3
	case c&0xf0 == 0xe0:
		if len(b) < 4 {
			return 0, 0, fmt.Errorf("short")
		}
		v = uint64(c&0x1f)<<21 | uint64(b[1]&0x7f)<<14 | uint64(b[2]&0x7f)<<7 | uint64(b[3]&0x7f)
		n = 4
	default:
		if len(b) < 5 {
			return 0, 0, fmt.Errorf("short")
		}
		v = uint64(c&0x0f)<<28 | uint64(b[1]&0x7f)<<21 | uint64(b[2]&0x7f)<<14 | uint64(b[3]&0x7f)<<7 | uint64(b[4]&0x7f)
		n = 5
	}
	return v, n, nil
}

func windows(d []byte, name string, limit int) {
	p := 4
	w := 0
	for p < len(d) && w < limit {
		w++
		so, n, e := readUint(d[p:])
		if e != nil {
			fmt.Printf("%s w[%d] hdr eof p=%d\n", name, w, p)
			return
		}
		p += n
		sl, n, _ := readUint(d[p:])
		p += n
		tl, n, _ := readUint(d[p:])
		p += n
		il, n, _ := readUint(d[p:])
		p += n
		dl, n, _ := readUint(d[p:])
		p += n
		fmt.Printf("%s w[%d] so=%d sl=%d tl=%d il=%d dl=%d\n", name, w, so, sl, tl, il, dl)
		if int(il)+int(dl) > len(d)-p {
			fmt.Printf("%s w[%d] mis-parse (il+dl %d > remaining %d)\n", name, w, il+dl, len(d)-p)
			return
		}
		p += int(il) + int(dl)
	}
}

func main() {
	d, _ := os.ReadFile("/home/user/svngod/bad_delta_dump.bin")
	fmt.Printf("dump len=%d Apply err=%v\n", len(d), func() error {
		_, e := svndiff.Apply(nil, d)
		return e
	}())
	windows(d, "dump", 8)

	rng := rand.New(rand.NewSource(99))
	data := make([]byte, 50000)
	rng.Read(data)
	enc, _ := svndiff.Encode(nil, data)
	fmt.Printf("svngod-enc len=%d Apply-ok=%v\n", len(enc), func() bool {
		dec, e := svndiff.Apply(nil, enc)
		return e == nil && bytes.Equal(dec, data)
	}())
	windows(enc, "enc", 8)
}
