package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseGooseSQL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		wantUp   string
		wantDown string
	}{
		{
			name: "standard goose migration",
			input: `-- +goose Up
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL
);

-- +goose Down
DROP TABLE users;`,
			wantUp:   "CREATE TABLE users (\n    id SERIAL PRIMARY KEY,\n    email VARCHAR(255) NOT NULL\n);",
			wantDown: "DROP TABLE users;",
		},
		{
			name: "up only, no down",
			input: `-- +goose Up
ALTER TABLE users ADD COLUMN name VARCHAR(255);`,
			wantUp:   "ALTER TABLE users ADD COLUMN name VARCHAR(255);",
			wantDown: "",
		},
		{
			name: "multiple statements in up",
			input: `-- +goose Up
CREATE TABLE posts (id SERIAL PRIMARY KEY, title TEXT);
CREATE INDEX idx_posts_title ON posts(title);

-- +goose Down
DROP INDEX idx_posts_title;
DROP TABLE posts;`,
			wantUp:   "CREATE TABLE posts (id SERIAL PRIMARY KEY, title TEXT);\nCREATE INDEX idx_posts_title ON posts(title);",
			wantDown: "DROP INDEX idx_posts_title;\nDROP TABLE posts;",
		},
		{
			name:     "empty file",
			input:    "",
			wantUp:   "",
			wantDown: "",
		},
		{
			name: "comments before directives",
			input: `-- This is a comment
-- Another comment

-- +goose Up
CREATE TABLE accounts (id INT);

-- +goose Down
DROP TABLE accounts;`,
			wantUp:   "CREATE TABLE accounts (id INT);",
			wantDown: "DROP TABLE accounts;",
		},
		{
			name: "goose StatementBegin and StatementEnd markers",
			input: `-- +goose Up
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- +goose Down
DROP FUNCTION IF EXISTS update_timestamp();`,
			wantUp:   "-- +goose StatementBegin\nCREATE OR REPLACE FUNCTION update_timestamp()\nRETURNS TRIGGER AS $$\nBEGIN\n    NEW.updated_at = NOW();\n    RETURN NEW;\nEND;\n$$ LANGUAGE plpgsql;\n-- +goose StatementEnd",
			wantDown: "DROP FUNCTION IF EXISTS update_timestamp();",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotUp, gotDown := parseGooseSQL(tt.input)
			if gotUp != tt.wantUp {
				t.Errorf("upSQL mismatch\ngot:  %q\nwant: %q", gotUp, tt.wantUp)
			}
			if gotDown != tt.wantDown {
				t.Errorf("downSQL mismatch\ngot:  %q\nwant: %q", gotDown, tt.wantDown)
			}
		})
	}
}

func TestGenerateQueenMigrationFile(t *testing.T) {
	t.Parallel()

	result := generateQueenMigrationFile("001", "create_users", "Register001create_users",
		"CREATE TABLE users (id SERIAL PRIMARY KEY);",
		"DROP TABLE users;",
	)

	checks := []struct {
		name    string
		contain string
	}{
		{"package declaration", "package migrations"},
		{"queen import", `"github.com/honeynil/queen"`},
		{"function name", "func Register001create_users(q *queen.Queen)"},
		{"version", `Version: "001"`},
		{"name", `Name:    "create_users"`},
		{"up SQL", "CREATE TABLE users (id SERIAL PRIMARY KEY);"},
		{"down SQL", "DROP TABLE users;"},
		{"MustAdd call", "q.MustAdd(queen.M{"},
	}

	for _, c := range checks {
		if !strings.Contains(result, c.contain) {
			t.Errorf("generated file missing %s: %q not found in output", c.name, c.contain)
		}
	}
}

func TestGenerateQueenMigrationFile_BacktickEscaping(t *testing.T) {
	t.Parallel()

	result := generateQueenMigrationFile("001", "add_json", "Register001add_json",
		"ALTER TABLE users ADD COLUMN meta JSON DEFAULT '`{}`';",
		"ALTER TABLE users DROP COLUMN meta;",
	)

	// Backticks in SQL should be escaped for Go raw string literals
	if strings.Count(result, "` + \"`\" + `") < 1 {
		t.Error("backticks in SQL should be escaped in generated Go code")
	}
}

func TestGenerateRegistrationFile(t *testing.T) {
	t.Parallel()

	calls := "\tRegister001create_users(q)\n\tRegister002add_posts(q)"
	result := generateRegistrationFile(calls)

	checks := []string{
		"package migrations",
		`"github.com/honeynil/queen"`,
		"func Register(q *queen.Queen)",
		"Register001create_users(q)",
		"Register002add_posts(q)",
	}

	for _, c := range checks {
		if !strings.Contains(result, c) {
			t.Errorf("registration file missing: %q", c)
		}
	}
}

func TestImportFromGoose_EndToEnd(t *testing.T) {
	t.Parallel()

	sourceDir := t.TempDir()
	outputDir := filepath.Join(t.TempDir(), "queen_migrations")

	gooseFiles := map[string]string{
		"001_create_users.sql": `-- +goose Up
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- +goose Down
DROP TABLE users;`,

		"002_create_posts.sql": `-- +goose Up
CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    title TEXT NOT NULL,
    body TEXT
);

CREATE INDEX idx_posts_user_id ON posts(user_id);

-- +goose Down
DROP INDEX idx_posts_user_id;
DROP TABLE posts;`,

		"003_add_users_name.sql": `-- +goose Up
ALTER TABLE users ADD COLUMN name VARCHAR(255);

-- +goose Down
ALTER TABLE users DROP COLUMN name;`,
	}

	for name, content := range gooseFiles {
		if err := os.WriteFile(filepath.Join(sourceDir, name), []byte(content), 0644); err != nil {
			t.Fatalf("failed to create test file %s: %v", name, err)
		}
	}

	err := importFromGoose(sourceDir, outputDir, false)
	if err != nil {
		t.Fatalf("importFromGoose() error = %v", err)
	}

	expectedFiles := []string{
		"migrations.go",
		"001_create_users.go",
		"002_create_posts.go",
		"003_add_users_name.go",
	}

	for _, name := range expectedFiles {
		path := filepath.Join(outputDir, name)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("expected file %s was not created", name)
			continue
		}

		content, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("failed to read %s: %v", name, err)
			continue
		}

		if !strings.Contains(string(content), "package migrations") {
			t.Errorf("%s missing package declaration", name)
		}
	}

	usersContent, _ := os.ReadFile(filepath.Join(outputDir, "001_create_users.go"))
	usersStr := string(usersContent)

	if !strings.Contains(usersStr, "CREATE TABLE users") {
		t.Error("001_create_users.go missing up SQL")
	}
	if !strings.Contains(usersStr, "DROP TABLE users") {
		t.Error("001_create_users.go missing down SQL")
	}
	if !strings.Contains(usersStr, `Version: "001"`) {
		t.Error("001_create_users.go missing version")
	}

	regContent, _ := os.ReadFile(filepath.Join(outputDir, "migrations.go"))
	regStr := string(regContent)

	if !strings.Contains(regStr, "func Register(q *queen.Queen)") {
		t.Error("migrations.go missing Register function")
	}

	if strings.Count(regStr, "(q)") < 3 {
		t.Errorf("migrations.go should have 3 registration calls, got %d", strings.Count(regStr, "(q)"))
	}
}

func TestImportFromGoose_DryRun(t *testing.T) {
	t.Parallel()

	sourceDir := t.TempDir()
	outputDir := filepath.Join(t.TempDir(), "should_not_exist")

	content := `-- +goose Up
CREATE TABLE test (id INT);
-- +goose Down
DROP TABLE test;`
	if err := os.WriteFile(filepath.Join(sourceDir, "001_test.sql"), []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	err := importFromGoose(sourceDir, outputDir, true)
	if err != nil {
		t.Fatalf("dry-run should not error, got: %v", err)
	}

	if _, err := os.Stat(outputDir); !os.IsNotExist(err) {
		t.Error("dry-run should not create output directory")
	}
}

func TestImportFromGoose_NoFiles(t *testing.T) {
	t.Parallel()

	emptyDir := t.TempDir()
	err := importFromGoose(emptyDir, t.TempDir(), false)

	if err == nil {
		t.Fatal("expected error for empty source directory")
	}
	if !strings.Contains(err.Error(), "no goose migration files found") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestImportFromGoose_InvalidFileName(t *testing.T) {
	t.Parallel()

	sourceDir := t.TempDir()

	if err := os.WriteFile(filepath.Join(sourceDir, "invalid.sql"), []byte("-- +goose Up\nSELECT 1;"), 0644); err != nil {
		t.Fatal(err)
	}

	err := importFromGoose(sourceDir, t.TempDir(), false)
	if err == nil {
		t.Fatal("expected error when all files are invalid")
	}
	if !strings.Contains(err.Error(), "no valid goose migrations found") {
		t.Errorf("unexpected error: %v", err)
	}
}
