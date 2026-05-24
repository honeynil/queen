package cli

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/yaop-labs/queen"
	"github.com/yaop-labs/queen/drivers/sqlite"
)

func TestBaselineWorkflowRecordsPendingMigrationsInSQLite(t *testing.T) {
	ctx := context.Background()
	q, closeDB := newSQLiteQueen(t)
	defer closeDB()

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL:   "CREATE TABLE users (id INTEGER PRIMARY KEY);",
		DownSQL: "DROP TABLE users;",
	})
	q.MustAdd(queen.M{
		Version: "002",
		Name:    "create_posts",
		UpSQL:   "CREATE TABLE posts (id INTEGER PRIMARY KEY);",
		DownSQL: "DROP TABLE posts;",
	})

	app := &App{config: &Config{}}
	if err := app.runBaseline(ctx, q, baselineOptions{name: "existing_schema", at: "001"}); err != nil {
		t.Fatalf("runBaseline returned error: %v", err)
	}

	statuses, err := q.Status(ctx)
	if err != nil {
		t.Fatalf("Status returned error: %v", err)
	}
	if statuses[0].Status != queen.StatusApplied {
		t.Fatalf("migration 001 status=%s, want applied", statuses[0].Status)
	}
	if statuses[1].Status != queen.StatusPending {
		t.Fatalf("migration 002 status=%s, want pending", statuses[1].Status)
	}

	applied, err := q.Driver().GetApplied(ctx)
	if err != nil {
		t.Fatalf("GetApplied returned error: %v", err)
	}
	if len(applied) != 1 || applied[0].Action != "baseline" {
		t.Fatalf("applied=%+v, want one baseline record", applied)
	}
}

func TestSquashWorkflowCreatesSQLMigrationFileWithSQLiteQueen(t *testing.T) {
	q, closeDB := newSQLiteQueen(t)
	defer closeDB()

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL:   "CREATE TABLE users (id INTEGER PRIMARY KEY);",
		DownSQL: "DROP TABLE users;",
	})
	q.MustAdd(queen.M{
		Version: "002",
		Name:    "create_posts",
		UpSQL:   "CREATE TABLE posts (id INTEGER PRIMARY KEY);",
		DownSQL: "DROP TABLE posts;",
	})

	restore := chdir(t, t.TempDir())
	defer restore()

	if err := os.MkdirAll("migrations", 0755); err != nil {
		t.Fatalf("MkdirAll migrations: %v", err)
	}
	for _, name := range []string{"001_create_users.go", "002_create_posts.go"} {
		if err := os.WriteFile(filepath.Join("migrations", name), []byte("package migrations\n"), 0644); err != nil {
			t.Fatalf("WriteFile %s: %v", name, err)
		}
	}

	app := &App{config: &Config{}}
	err := app.runSquash(q, squashOptions{
		versions: []string{"001", "002"},
		into:     "squashed_schema",
	})
	if err != nil {
		t.Fatalf("runSquash returned error: %v", err)
	}

	content, err := os.ReadFile(filepath.Join("migrations", "003_squashed_schema.go"))
	if err != nil {
		t.Fatalf("ReadFile squashed migration: %v", err)
	}
	got := string(content)
	for _, want := range []string{
		`Version: "003"`,
		`Name:    "squashed_schema"`,
		"-- 001 create_users",
		"-- 002 create_posts",
		"CREATE TABLE users",
		"DROP TABLE posts",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("squashed migration missing %q:\n%s", want, got)
		}
	}
}

func newSQLiteQueen(t *testing.T) (*queen.Queen, func()) {
	t.Helper()

	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "queen.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	q := queen.New(sqlite.New(db))
	return q, func() {
		_ = q.Close()
	}
}

func chdir(t *testing.T, dir string) func() {
	t.Helper()

	previous, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("Chdir: %v", err)
	}
	return func() {
		if err := os.Chdir(previous); err != nil {
			t.Fatalf("restore working directory: %v", err)
		}
	}
}
