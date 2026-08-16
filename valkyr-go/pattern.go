package valkyr

import (
	"strings"
	"unicode/utf8"
)

type patternToken struct{ kind, text string }

func tokenize(pattern string) []patternToken {
	var out []patternToken
	for i := 0; i < len(pattern); {
		tail := pattern[i:]
		if tail[0] == '*' {
			out = append(out, patternToken{"wildcard", ""})
			i++
			continue
		}
		start := 0
		if strings.HasPrefix(tail, "${") {
			start = 2
		} else if strings.HasPrefix(tail, "{") {
			start = 1
		}
		if start > 0 {
			if end := strings.IndexByte(tail, '}'); end > start {
				out = append(out, patternToken{"capture", tail[start:end]})
				i += end + 1
				continue
			}
		}
		end := len(tail)
		for _, ch := range []string{"*", "{", "${"} {
			if p := strings.Index(tail, ch); p > 0 && p < end {
				end = p
			}
		}
		out = append(out, patternToken{"literal", tail[:end]})
		i += end
	}
	return out
}
func matchPattern(tokens []patternToken, value string) bool {
	memo := map[[2]int]bool{}
	seen := map[[2]int]bool{}
	var f func(int, int) bool
	f = func(t, i int) bool {
		k := [2]int{t, i}
		if seen[k] {
			return memo[k]
		}
		seen[k] = true
		var ok bool
		if t == len(tokens) {
			ok = i == len(value)
		} else {
			tok := tokens[t]
			switch tok.kind {
			case "literal":
				ok = strings.HasPrefix(value[i:], tok.text) && f(t+1, i+len(tok.text))
			case "wildcard":
				for j := i; j <= len(value); j++ {
					if !utf8.ValidString(value[i:j]) {
						continue
					}
					if f(t+1, j) {
						ok = true
						break
					}
				}
			case "capture":
				for j := i + 1; j <= len(value); j++ {
					if f(t+1, j) {
						ok = true
						break
					}
				}
			}
		}
		memo[k] = ok
		return ok
	}
	return f(0, 0)
}
func match(pattern, value string) bool { return matchPattern(tokenize(pattern), value) }
func namespaceMatch(pattern, namespace string) bool {
	if match(pattern, namespace) {
		return true
	}
	return !strings.ContainsAny(pattern, "*{") && strings.HasPrefix(namespace, pattern+"::")
}
func overlap(a, b string) bool {
	if a == "*" || b == "*" || a == b {
		return true
	}
	if match(a, b) || match(b, a) {
		return true
	}
	if strings.HasSuffix(a, "*") && strings.HasPrefix(b, strings.TrimSuffix(a, "*")) {
		return true
	}
	return strings.HasSuffix(b, "*") && strings.HasPrefix(a, strings.TrimSuffix(b, "*"))
}
