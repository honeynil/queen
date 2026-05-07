package queen

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadQueenIgnore(t *testing.T) {
	t.Parallel()

	t.Run("returns empty ignore when file doesn't exist", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()
		path := filepath.Join(tempDir, ".queenignore")

		qi, err := LoadQueenIgnoreFrom(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if qi == nil {
			t.Fatal("expected QueenIgnore instance, got nil")
		}

		if len(qi.ignored) != 0 {
			t.Errorf("expected empty ignore map, got %d entries", len(qi.ignored))
		}
	})

	t.Run("loads ignore file with versions and reasons", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()
		path := filepath.Join(tempDir, ".queenignore")

		content := `# Test ignore file
001 # skipped migration
002 # another skip

# Comment line
003
`
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}

		qi, err := LoadQueenIgnoreFrom(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(qi.ignored) != 3 {
			t.Fatalf("expected 3 ignored versions, got %d", len(qi.ignored))
		}

		if !qi.IsIgnored("001") {
			t.Error("001 should be ignored")
		}

		if reason := qi.GetReason("001"); reason != "skipped migration" {
			t.Errorf("reason for 001 = %q, want %q", reason, "skipped migration")
		}

		if !qi.IsIgnored("002") {
			t.Error("002 should be ignored")
		}

		if !qi.IsIgnored("003") {
			t.Error("003 should be ignored")
		}

		if qi.GetReason("003") != "" {
			t.Errorf("003 should have empty reason, got %q", qi.GetReason("003"))
		}
	})

	t.Run("handles empty lines and comments", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()
		path := filepath.Join(tempDir, ".queenignore")

		content := `
# Comment
001

# Another comment


002 # reason
`
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}

		qi, err := LoadQueenIgnoreFrom(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(qi.ignored) != 2 {
			t.Fatalf("expected 2 ignored versions, got %d", len(qi.ignored))
		}
	})
}

func TestIsIgnored(t *testing.T) {
	t.Parallel()

	qi := &QueenIgnore{
		ignored: map[string]*IgnoredGap{
			"001": {Version: "001"},
			"002": {Version: "002"},
		},
	}

	if !qi.IsIgnored("001") {
		t.Error("001 should be ignored")
	}

	if !qi.IsIgnored("002") {
		t.Error("002 should be ignored")
	}

	if qi.IsIgnored("003") {
		t.Error("003 should not be ignored")
	}
}

func TestGetReason(t *testing.T) {
	t.Parallel()

	qi := &QueenIgnore{
		ignored: map[string]*IgnoredGap{
			"001": {Version: "001", Reason: "test reason"},
			"002": {Version: "002", Reason: ""},
		},
	}

	if reason := qi.GetReason("001"); reason != "test reason" {
		t.Errorf("reason = %q, want %q", reason, "test reason")
	}

	if reason := qi.GetReason("002"); reason != "" {
		t.Errorf("reason = %q, want empty", reason)
	}

	if reason := qi.GetReason("003"); reason != "" {
		t.Errorf("reason for non-existent version = %q, want empty", reason)
	}
}

func TestAddIgnore(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, ".queenignore")

	qi := &QueenIgnore{
		filePath: path,
		ignored:  make(map[string]*IgnoredGap),
	}

	err := qi.AddIgnore("001", "test reason", "test user")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !qi.IsIgnored("001") {
		t.Error("001 should be ignored after adding")
	}

	gap := qi.ignored["001"]
	if gap.Reason != "test reason" {
		t.Errorf("reason = %q, want %q", gap.Reason, "test reason")
	}
	if gap.IgnoredBy != "test user" {
		t.Errorf("ignoredBy = %q, want %q", gap.IgnoredBy, "test user")
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("queenignore file should exist after AddIgnore")
	}
}

func TestRemoveIgnore(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, ".queenignore")

	qi := &QueenIgnore{
		filePath: path,
		ignored: map[string]*IgnoredGap{
			"001": {Version: "001", Reason: "test"},
		},
	}

	err := qi.RemoveIgnore("001")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if qi.IsIgnored("001") {
		t.Error("001 should not be ignored after removal")
	}
}

func TestSave(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, ".queenignore")

	qi := &QueenIgnore{
		filePath: path,
		ignored: map[string]*IgnoredGap{
			"001": {Version: "001", Reason: "skipped"},
			"002": {Version: "002", Reason: ""},
		},
	}

	err := qi.Save()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Read the file back
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	contentStr := string(content)
	if !contains(contentStr, "001") {
		t.Error("file should contain version 001")
	}
	if !contains(contentStr, "002") {
		t.Error("file should contain version 002")
	}
	if !contains(contentStr, "skipped") {
		t.Error("file should contain reason 'skipped'")
	}
}

func TestListIgnored(t *testing.T) {
	t.Parallel()

	qi := &QueenIgnore{
		ignored: map[string]*IgnoredGap{
			"001": {Version: "001", Reason: "test1"},
			"002": {Version: "002", Reason: "test2"},
		},
	}

	list := qi.ListIgnored()

	if len(list) != 2 {
		t.Fatalf("expected 2 ignored gaps, got %d", len(list))
	}

	found := make(map[string]bool)
	for _, gap := range list {
		found[gap.Version] = true
	}

	if !found["001"] || !found["002"] {
		t.Error("list should contain both 001 and 002")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 || findSubstring(s, substr))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
