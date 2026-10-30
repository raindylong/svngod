package server

import (
	"sort"
	"strconv"
	"strings"
)

// mergeRange is one svn:mergeinfo range. Non-inheritable ranges (marked with
// '*') apply to the node itself but are not inherited by its children.
type mergeRange struct {
	start, end int64
	inherit    bool
}

// parseMergeinfo parses an svn:mergeinfo property value:
//
//	/trunk:1-3,7
//	/branches/x:4-6*
func parseMergeinfo(s string) map[string][]mergeRange {
	out := map[string][]mergeRange{}
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		i := strings.LastIndex(line, ":")
		if i <= 0 {
			continue
		}
		path := strings.TrimSpace(line[:i])
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		for _, part := range strings.Split(line[i+1:], ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			inherit := true
			if strings.HasSuffix(part, "*") {
				inherit = false
				part = strings.TrimSuffix(part, "*")
			}
			var start, end int64
			if j := strings.IndexByte(part, '-'); j >= 0 {
				a, err1 := strconv.ParseInt(strings.TrimSpace(part[:j]), 10, 64)
				b, err2 := strconv.ParseInt(strings.TrimSpace(part[j+1:]), 10, 64)
				if err1 != nil || err2 != nil {
					continue
				}
				start, end = a, b
			} else {
				n, err := strconv.ParseInt(part, 10, 64)
				if err != nil {
					continue
				}
				start, end = n, n
			}
			if start > end {
				start, end = end, start
			}
			out[path] = append(out[path], mergeRange{start: start, end: end, inherit: inherit})
		}
	}
	return out
}

func formatMergeinfo(m map[string][]mergeRange) string {
	if len(m) == 0 {
		return ""
	}
	paths := make([]string, 0, len(m))
	for p := range m {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	var lines []string
	for _, p := range paths {
		ranges := mergeRanges(m[p])
		var parts []string
		for _, r := range ranges {
			s := strconv.FormatInt(r.start, 10)
			if r.end != r.start {
				s += "-" + strconv.FormatInt(r.end, 10)
			}
			if !r.inherit {
				s += "*"
			}
			parts = append(parts, s)
		}
		if len(parts) > 0 {
			lines = append(lines, p+":"+strings.Join(parts, ","))
		}
	}
	return strings.Join(lines, "\n")
}

// mergeRanges sorts, merges overlapping ranges and drops zero-length entries.
func mergeRanges(in []mergeRange) []mergeRange {
	if len(in) == 0 {
		return nil
	}
	byInherit := map[bool][]mergeRange{}
	for _, r := range in {
		if r.start < 0 || r.end < r.start {
			continue
		}
		byInherit[r.inherit] = append(byInherit[r.inherit], r)
	}
	var out []mergeRange
	for _, inherit := range []bool{true, false} {
		rs := byInherit[inherit]
		sort.Slice(rs, func(i, j int) bool {
			if rs[i].start == rs[j].start {
				return rs[i].end < rs[j].end
			}
			return rs[i].start < rs[j].start
		})
		for _, r := range rs {
			if n := len(out); n > 0 && out[n-1].inherit == inherit && r.start <= out[n-1].end+1 {
				if r.end > out[n-1].end {
					out[n-1].end = r.end
				}
				continue
			}
			out = append(out, r)
		}
	}
	return out
}

// translateMergeinfo moves mergeinfo from an ancestor to one of its children:
// every source path gets the child's relative suffix appended.
func translateMergeinfo(m map[string][]mergeRange, rel string) map[string][]mergeRange {
	rel = strings.Trim(rel, "/")
	if rel == "" {
		return m
	}
	out := make(map[string][]mergeRange, len(m))
	for p, ranges := range m {
		np := strings.TrimSuffix(p, "/") + "/" + rel
		out[np] = append(out[np], ranges...)
	}
	return out
}

// unionMergeinfo merges src into dst.
func unionMergeinfo(dst, src map[string][]mergeRange) map[string][]mergeRange {
	if dst == nil {
		dst = map[string][]mergeRange{}
	}
	for p, ranges := range src {
		dst[p] = append(dst[p], ranges...)
	}
	return dst
}

// inheritableOnly drops non-inheritable ranges (they do not propagate).
func inheritableOnly(m map[string][]mergeRange) map[string][]mergeRange {
	out := map[string][]mergeRange{}
	for p, ranges := range m {
		for _, r := range ranges {
			if r.inherit {
				out[p] = append(out[p], r)
			}
		}
	}
	return out
}
