// Package sort compares migration versions in natural order.
package sort

import (
	"unicode"
)

// Compare treats digit runs as numbers, so "2" sorts before "10".
func Compare(a, b string) int {
	ia, ib := 0, 0

	for ia < len(a) && ib < len(b) {
		numA, nextA := extractNumber(a, ia)
		numB, nextB := extractNumber(b, ib)

		if nextA > ia && nextB > ib {
			if numA != numB {
				return sign(numA - numB)
			}
			ia, ib = nextA, nextB
			continue
		}

		strA, nextA := extractString(a, ia)
		strB, nextB := extractString(b, ib)

		if strA != strB {
			if strA < strB {
				return -1
			}
			return 1
		}

		ia, ib = nextA, nextB
	}

	return sign(len(a) - len(b))
}

func extractNumber(s string, i int) (int, int) {
	if i >= len(s) || !unicode.IsDigit(rune(s[i])) {
		return 0, i
	}

	num := 0
	for i < len(s) && unicode.IsDigit(rune(s[i])) {
		num = num*10 + int(s[i]-'0')
		i++
	}

	return num, i
}

func extractString(s string, i int) (string, int) {
	start := i
	for i < len(s) && !unicode.IsDigit(rune(s[i])) {
		i++
	}
	return s[start:i], i
}

func sign(n int) int {
	if n < 0 {
		return -1
	}
	if n > 0 {
		return 1
	}
	return 0
}
