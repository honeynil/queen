// Package sort provides natural sorting for migration versions.
// Natural sorting treats numeric parts as numbers, not strings.
// For example: "1" < "2" < "10" < "100"
package sort

import (
	"unicode"
)

// Compare compares two version strings using natural sort order.
// Returns:
//   -1 if a < b
//    0 if a == b
//   +1 if a > b
//
// Examples:
//   Compare("1", "2") = -1
//   Compare("10", "2") = 1
//   Compare("v1", "v10") = -1
//   Compare("user_001", "user_002") = -1
func Compare(a, b string) int {
	ia, ib := 0, 0

	for ia < len(a) && ib < len(b) {
		// Extract numeric parts
		numA, nextA := extractNumber(a, ia)
		numB, nextB := extractNumber(b, ib)

		// If both have numbers, compare numerically
		if nextA > ia && nextB > ib {
			if numA != numB {
				return sign(numA - numB)
			}
			ia, ib = nextA, nextB
			continue
		}

		// Extract string parts
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

	// If one string is a prefix of the other, shorter comes first
	return sign(len(a) - len(b))
}

// extractNumber extracts a number from the string starting at position i.
// Returns the numeric value and the position after the number.
// If no number is found, returns (0, i).
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

// extractString extracts a non-numeric string starting at position i.
// Returns the string and the position after it.
// If no string is found, returns ("", i).
func extractString(s string, i int) (string, int) {
	start := i
	for i < len(s) && !unicode.IsDigit(rune(s[i])) {
		i++
	}
	return s[start:i], i
}

// sign returns -1, 0, or 1 based on the sign of n.
func sign(n int) int {
	if n < 0 {
		return -1
	}
	if n > 0 {
		return 1
	}
	return 0
}
