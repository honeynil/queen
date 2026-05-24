package tap

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestRunExplainSQLite(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(context.Background(), "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	result, err := RunExplain(context.Background(), db, "sqlite", ExplainOnly, "SELECT email FROM users WHERE id = ?", []any{1})
	if err != nil {
		t.Fatalf("RunExplain: %v", err)
	}
	if result.Mode != ExplainOnly {
		t.Fatalf("mode=%q, want %q", result.Mode, ExplainOnly)
	}
	if result.Plan == "" {
		t.Fatal("expected non-empty plan")
	}
	if !strings.Contains(strings.ToUpper(result.Plan), "SEARCH") && !strings.Contains(strings.ToUpper(result.Plan), "SCAN") {
		t.Fatalf("unexpected plan: %q", result.Plan)
	}
}
