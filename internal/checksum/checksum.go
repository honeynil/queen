// Package checksum provides checksum calculation for migrations.
package checksum

import (
	"crypto/sha256"
	"fmt"
	"strings"
)

// Calculate hashes migration SQL after whitespace normalization.
func Calculate(content ...string) string {
	h := sha256.New()

	for _, c := range content {
		normalized := normalizeWhitespace(c)
		h.Write([]byte(normalized))
	}

	return fmt.Sprintf("%x", h.Sum(nil))
}

func normalizeWhitespace(s string) string {
	lines := strings.Split(s, "\n")
	result := make([]string, 0, len(lines))

	prevEmpty := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		if trimmed == "" {
			if !prevEmpty {
				result = append(result, "")
				prevEmpty = true
			}
			continue
		}
		prevEmpty = false
		result = append(result, trimmed)
	}

	return strings.TrimSpace(strings.Join(result, "\n"))
}
