package main

// svn 参考解码器：严格按 svndiff.c（write_handler + decode_window + count_and_verify）
// 验证真实 svn 客户端 delta（bad_delta_dump.bin）能否按 svn 语义解码。
import (
	"fmt"
	"os"
)

// ru: svn__decode_uint 前导长度编码
func ru(b []byte, p *int) (uint64, error) {
	if *p >= len(b) {
		return 0, fmt.Errorf("eof")
	}
	c := b[*p]
	*p++
	switch {
	case c&0x80 == 0:
		return uint64(c), nil
	case c&0xc0 == 0x80:
		if *p >= len(b) {
			return 0, fmt.Errorf("short")
		}
		v := uint64(c&0x7f)<<7 | uint64(b[*p]&0x7f)
		*p++
		return v, nil
	case c&0xe0 == 0xc0:
		if *p+1 >= len(b) {
			return 0, fmt.Errorf("short")
		}
		v := uint64(c&0x3f)<<14 | uint64(b[*p]&0x7f)<<7 | uint64(b[*p+1]&0x7f)
		*p += 2
		return v, nil
	case c&0xf0 == 0xe0:
		if *p+2 >= len(b) {
			return 0, fmt.Errorf("short")
		}
		v := uint64(c&0x1f)<<21 | uint64(b[*p]&0x7f)<<14 | uint64(b[*p+1]&0x7f)<<7 | uint64(b[*p+2]&0x7f)
		*p += 3
		return v, nil
	default:
		if *p+3 >= len(b) {
			return 0, fmt.Errorf("short")
		}
		v := uint64(c&0x0f)<<28 | uint64(b[*p]&0x7f)<<21 | uint64(b[*p+1]&0x7f)<<14 | uint64(b[*p+2]&0x7f)<<7 | uint64(b[*p+3]&0x7f)
		*p += 4
		return v, nil
	}
}

func main() {
	d, _ := os.ReadFile("/home/user/svngod/bad_delta_dump.bin")
	p := 4
	window := 0
	for p < len(d) {
		window++
		start := p
		so, _ := ru(d, &p)
		sl, _ := ru(d, &p)
		tl, _ := ru(d, &p)
		il, _ := ru(d, &p)
		nl, _ := ru(d, &p)
		if p+int(il)+int(nl) > len(d) {
			fmt.Printf("w%d: il+nl OOB so=%d sl=%d tl=%d il=%d nl=%d hdrLen=%d\n", window, so, sl, tl, il, nl, p-start)
			return
		}
		ins := d[p : p+int(il)]
		p += int(il) + int(nl)
		_ = p
		fmt.Printf("w%d: so=%d sl=%d tl=%d il=%d nl=%d hdrLen=%d\n", window, so, sl, tl, il, nl, p-start-(int(il)+int(nl)))

		var tview []byte
		_ = tview
		ip := 0
		npos := 0
		tpos := 0
		for ip < len(ins) {
			c := ins[ip]
			ip++
			action := (c >> 6) & 0x3
			if action == 3 {
				fmt.Printf("  w%d insn invalid action=3 byte=0x%x\n", window, c)
				return
			}
			length := uint64(c & 0x3f)
			if length == 0 {
				var err error
				if length, err = ru(ins, &ip); err != nil {
					fmt.Printf("  w%d insn length ext fail: %v\n", window, err)
					return
				}
			}
			if length == 0 {
				fmt.Printf("  w%d insn length zero byte=0x%x\n", window, c)
				return
			}
			var offset uint64
			if action != 2 {
				var err error
				if offset, err = ru(ins, &ip); err != nil {
					fmt.Printf("  w%d insn offset fail: %v\n", window, err)
					return
				}
			}
			switch action {
			case 0: // source
				if offset > uint64(sl) || length > uint64(sl)-offset {
					fmt.Printf("  w%d src OOB off=%d len=%d sl=%d\n", window, offset, length, sl)
					return
				}
			case 1: // target: 绝对位置
				if offset >= uint64(tpos) {
					fmt.Printf("  w%d tgt OOB offset=%d >= tpos=%d byte=0x%x\n", window, offset, tpos, c)
					return
				}
			case 2: // new
				if length > uint64(nl)-uint64(npos) {
					fmt.Printf("  w%d NEW OOB npos=%d len=%d nl=%d byte=0x%x\n", window, npos, length, nl, c)
					return
				}
				npos += int(length)
			}
			tpos += int(length)
		}
		if tpos != int(tl) {
			fmt.Printf("  w%d tpos=%d != tl=%d\n", window, tpos, tl)
			return
		}
		if npos != int(nl) {
			fmt.Printf("  w%d npos=%d != nl=%d\n", window, npos, nl)
			return
		}
	}
	fmt.Printf("REFERENCE DECODE OK windows=%d\n", window)
}
