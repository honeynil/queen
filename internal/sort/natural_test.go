package sort

import "testing"

func TestCompare(t *testing.T) {
	tests := []struct {
		name string
		a    string
		b    string
		want int
	}{
		// Simple numeric comparisons
		{"1 < 2", "1", "2", -1},
		{"2 > 1", "2", "1", 1},
		{"1 == 1", "1", "1", 0},

		// Natural sort: numbers treated numerically
		{"1 < 10", "1", "10", -1},
		{"2 < 10", "2", "10", -1},
		{"10 > 2", "10", "2", 1},
		{"10 < 100", "10", "100", -1},

		// Zero-padded numbers
		{"001 < 002", "001", "002", -1},
		{"001 < 010", "001", "010", -1},
		{"010 < 100", "010", "100", -1},

		// Mixed alphanumeric
		{"v1 < v2", "v1", "v2", -1},
		{"v1 < v10", "v1", "v10", -1},
		{"v2 < v10", "v2", "v10", -1},

		// Prefixed versions
		{"user_001 < user_002", "user_001", "user_002", -1},
		{"user_001 < user_010", "user_001", "user_010", -1},
		{"post_001 < user_001", "post_001", "user_001", -1}, // alphabetical prefix

		// Suffixes
		{"001 < 001a", "001", "001a", -1},
		{"001a < 001b", "001a", "001b", -1},
		{"001a < 002", "001a", "002", -1},

		// Feature branches
		{"001_feat_a < 001_feat_b", "001_feat_a", "001_feat_b", -1},
		{"001_feat_a < 002", "001_feat_a", "002", -1},

		// Equal strings
		{"user_001 == user_001", "user_001", "user_001", 0},

		// Length differences
		{"abc < abcd", "abc", "abcd", -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Compare(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("Compare(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestExtractNumber(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		i        int
		wantNum  int
		wantNext int
	}{
		{"simple number", "123", 0, 123, 3},
		{"number in middle", "abc123def", 3, 123, 6},
		{"no number", "abc", 0, 0, 0},
		{"number at end", "abc123", 3, 123, 6},
		{"zero", "0", 0, 0, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotNum, gotNext := extractNumber(tt.s, tt.i)
			if gotNum != tt.wantNum || gotNext != tt.wantNext {
				t.Errorf("extractNumber(%q, %d) = (%d, %d), want (%d, %d)",
					tt.s, tt.i, gotNum, gotNext, tt.wantNum, tt.wantNext)
			}
		})
	}
}

func TestExtractString(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		i        int
		wantStr  string
		wantNext int
	}{
		{"simple string", "abc", 0, "abc", 3},
		{"string before number", "abc123", 0, "abc", 3},
		{"empty at number", "123", 0, "", 0},
		{"string in middle", "abc123def", 0, "abc", 3},
		{"underscore", "user_", 0, "user_", 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStr, gotNext := extractString(tt.s, tt.i)
			if gotStr != tt.wantStr || gotNext != tt.wantNext {
				t.Errorf("extractString(%q, %d) = (%q, %d), want (%q, %d)",
					tt.s, tt.i, gotStr, gotNext, tt.wantStr, tt.wantNext)
			}
		})
	}
}
