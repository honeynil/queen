//go:build integration

package integration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/honeynil/queen"
)

// TestMigration is a simple test migration
var TestMigration001 = queen.M{
	Version: "001",
	Name:    "create_users",
	UpSQL: `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			email VARCHAR(255) UNIQUE NOT NULL
		)
	`,
	DownSQL: `DROP TABLE users`,
}

var TestMigration002 = queen.M{
	Version: "002",
	Name:    "create_posts",
	UpSQL: `
		CREATE TABLE posts (
			id INTEGER PRIMARY KEY,
			user_id INTEGER NOT NULL,
			title VARCHAR(255) NOT NULL,
			content TEXT
		)
	`,
	DownSQL: `DROP TABLE posts`,
}

var TestMigration003 = queen.M{
	Version: "003",
	Name:    "add_timestamps",
	UpSQL: `
		ALTER TABLE users ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
		ALTER TABLE posts ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
	`,
	DownSQL: `
		-- Note: SQLite doesn't support DROP COLUMN easily
	`,
}

type NopLogger struct{}

func (NopLogger) Printf(string, ...any) {}

func CleanupDB(t *testing.T, db *sql.DB, tableName string) {
	t.Helper()

	tables := []string{"users", "posts", "comments", tableName}
	for _, table := range tables {
		_, _ = db.Exec("DROP TABLE IF EXISTS " + table)
	}
}

// WaitForDB waits for database to be ready
func WaitForDB(t *testing.T, db *sql.DB, timeout time.Duration) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatal("timeout waiting for database to be ready")
		case <-ticker.C:
			if err := db.PingContext(ctx); err == nil {
				return
			}
		}
	}
}

// TableExists checks if a table exists in the database
func TableExists(t *testing.T, db *sql.DB, tableName string) bool {
	t.Helper()

	// Try a simple COUNT query which works on all databases
	query := "SELECT COUNT(*) FROM " + tableName
	_, err := db.Exec(query)
	return err == nil
}

// CountRows returns the number of rows in a table
func CountRows(t *testing.T, db *sql.DB, tableName string) int {
	t.Helper()

	var count int
	query := "SELECT COUNT(*) FROM " + tableName
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		t.Fatalf("failed to count rows in %s: %v", tableName, err)
	}
	return count
}
