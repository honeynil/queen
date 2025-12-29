// Package checksum provides checksum calculation for migrations.
package checksum

import (
	"crypto/sha256"
	"fmt"
)

// Calculate computes a SHA-256 checksum of the given migration content.
// It concatenates all provided strings and returns the hex-encoded hash.
func Calculate(content ...string) string {
	h := sha256.New()

	for _, c := range content {
		h.Write([]byte(c))
	}

	return fmt.Sprintf("%x", h.Sum(nil))
}
