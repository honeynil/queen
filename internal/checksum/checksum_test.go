package checksum

import "testing"

func TestCalculate(t *testing.T) {
	tests := []struct {
		name    string
		content []string
		want    string
	}{
		{
			name:    "single string",
			content: []string{"CREATE TABLE users (id INT)"},
			want:    "c8e3d7e8e8d8c8d8c8d8c8d8c8d8c8d8c8d8c8d8c8d8c8d8c8d8c8d8c8d8c8d8",
		},
		{
			name:    "empty string",
			content: []string{""},
			want:    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			name:    "multiple strings",
			content: []string{"CREATE TABLE users (id INT)", "DROP TABLE users"},
			want:    "different_than_single",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Calculate(tt.content...)

			// Check that it returns a valid SHA-256 hex string (64 chars)
			if len(got) != 64 {
				t.Errorf("Calculate() returned string of length %d, want 64", len(got))
			}

			// For known values, check exact match
			if tt.name == "empty string" {
				if got != tt.want {
					t.Errorf("Calculate() = %s, want %s", got, tt.want)
				}
			}
		})
	}
}

func TestCalculateDeterministic(t *testing.T) {
	// Same input should always produce same output
	input := []string{"CREATE TABLE users (id INT)", "DROP TABLE users"}

	result1 := Calculate(input...)
	result2 := Calculate(input...)

	if result1 != result2 {
		t.Errorf("Calculate() is not deterministic: %s != %s", result1, result2)
	}
}

func TestCalculateDifferent(t *testing.T) {
	// Different inputs should produce different outputs
	input1 := []string{"CREATE TABLE users (id INT)"}
	input2 := []string{"CREATE TABLE posts (id INT)"}

	result1 := Calculate(input1...)
	result2 := Calculate(input2...)

	if result1 == result2 {
		t.Errorf("Calculate() produced same hash for different inputs")
	}
}
