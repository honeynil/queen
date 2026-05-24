package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yaop-labs/queen"
)

func TestIsValidMigrationName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"valid lowercase", "create_users", true},
		{"valid with numbers", "add_column_v2", true},
		{"valid numbers only", "001", true},
		{"valid single word", "init", true},
		{"invalid uppercase", "Create_Users", false},
		{"invalid spaces", "create users", false},
		{"invalid dashes", "create-users", false},
		{"invalid special chars", "create@users", false},
		{"invalid dots", "create.users", false},
		{"empty string", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := queen.IsValidMigrationName(tt.input)
			if got != tt.want {
				t.Errorf("isValidMigrationName(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func testFindNextVersion(t *testing.T, setupFiles []string, expectedVersion string) {
	tempDir := t.TempDir()
	oldWd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chdir(oldWd) }()

	if err := os.Chdir(tempDir); err != nil {
		t.Fatal(err)
	}

	if len(setupFiles) > 0 {
		if err := os.MkdirAll("migrations", 0755); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = os.RemoveAll("migrations") }()

		for _, f := range setupFiles {
			if err := os.WriteFile(filepath.Join("migrations", f), []byte("package migrations"), 0644); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Create app with default config (no .queen.yaml)
	app := &App{
		config: &Config{},
	}

	version, err := app.findNextVersion()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if version != expectedVersion {
		t.Errorf("findNextVersion() = %q, want %q", version, expectedVersion)
	}
}

func TestFindNextVersion(t *testing.T) {
	t.Run("no migrations directory", func(t *testing.T) {
		testFindNextVersion(t, nil, "001")
	})

	t.Run("empty migrations directory", func(t *testing.T) {
		testFindNextVersion(t, []string{}, "001")
	})

	t.Run("with existing migrations", func(t *testing.T) {
		testFindNextVersion(t, []string{
			"001_create_users.go",
			"002_add_email.go",
			"005_add_index.go",
		}, "006")
	})

	t.Run("ignores non-go files", func(t *testing.T) {
		testFindNextVersion(t, []string{
			"001_create_users.go",
			"002_readme.md",
			"003_notes.txt",
		}, "002")
	})

	t.Run("handles malformed filenames", func(t *testing.T) {
		testFindNextVersion(t, []string{
			"001_create_users.go",
			"register.go",
			"utils.go",
			"abc_migration.go",
		}, "002")
	})
}

func TestMigrationVariableName(t *testing.T) {
	tests := []struct {
		version string
		name    string
		want    string
	}{
		{"001", "create_users", "Migration001CreateUsers"},
		{"002", "add_email", "Migration002AddEmail"},
		{"010", "add_user_profile", "Migration010AddUserProfile"},
		{"100", "init", "Migration100Init"},
		{"001", "a_b_c", "Migration001ABC"},
	}

	for _, tt := range tests {
		t.Run(tt.version+"_"+tt.name, func(t *testing.T) {
			got := migrationVariableName(tt.version, tt.name)
			if got != tt.want {
				t.Errorf("migrationVariableName(%q, %q) = %q, want %q", tt.version, tt.name, got, tt.want)
			}
		})
	}
}

func TestToPascalCase(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"create_users", "CreateUsers"},
		{"add_email", "AddEmail"},
		{"init", "Init"},
		{"a_b_c", "ABC"},
		{"", ""},
		{"single", "Single"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := toPascalCase(tt.input)
			if got != tt.want {
				t.Errorf("toPascalCase(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestGenerateSQLTemplate(t *testing.T) {
	template := generateSQLTemplate("001", "create_users", "Migration001CreateUsers")

	// Check required parts
	checks := []string{
		"package migrations",
		`import "github.com/yaop-labs/queen"`,
		"Migration001CreateUsers",
		`Version: "001"`,
		`Name:    "create_users"`,
		"UpSQL:",
		"DownSQL:",
	}

	for _, check := range checks {
		if !strings.Contains(template, check) {
			t.Errorf("SQL template missing %q", check)
		}
	}

	// Should not have ManualChecksum (that's for Go migrations)
	if strings.Contains(template, "ManualChecksum") {
		t.Error("SQL template should not contain ManualChecksum")
	}
}

func TestGenerateGoTemplate(t *testing.T) {
	template := generateGoTemplate("001", "migrate_data", "Migration001MigrateData")

	// Check required parts
	checks := []string{
		"package migrations",
		`"context"`,
		`"database/sql"`,
		`"github.com/yaop-labs/queen"`,
		"Migration001MigrateData",
		`Version:        "001"`,
		`Name:           "migrate_data"`,
		`ManualChecksum: "v1"`,
		"UpFunc:",
		"DownFunc:",
		"func up001MigrateData(ctx context.Context, tx *sql.Tx) error",
		"func down001MigrateData(ctx context.Context, tx *sql.Tx) error",
	}

	for _, check := range checks {
		if !strings.Contains(template, check) {
			t.Errorf("Go template missing %q", check)
		}
	}

	// Should have ManualChecksum
	if !strings.Contains(template, "ManualChecksum") {
		t.Error("Go template should contain ManualChecksum")
	}
}
