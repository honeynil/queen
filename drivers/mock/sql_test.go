package mock

import (
	"context"
	"database/sql"
	"testing"

	"github.com/yaop-labs/queen"
)

// TestMockDriver_SQLMigrations tests that SQL migrations work with the mock driver.
func TestMockDriver_SQLMigrations(t *testing.T) {
	driver := New()
	defer func() { _ = driver.Close() }()

	q := queen.New(driver)

	// Add SQL migration
	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL:   `CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)`,
		DownSQL: `DROP TABLE users`,
	})

	q.MustAdd(queen.M{
		Version: "002",
		Name:    "add_users_name",
		UpSQL:   `ALTER TABLE users ADD COLUMN name TEXT`,
		DownSQL: `ALTER TABLE users DROP COLUMN name`,
	})

	ctx := context.Background()

	// Test Up
	if err := q.Up(ctx); err != nil {
		t.Fatalf("Up failed: %v", err)
	}

	// Verify migrations were applied
	if count := driver.AppliedCount(); count != 2 {
		t.Errorf("AppliedCount = %d, want 2", count)
	}

	// Test Down
	if err := q.Down(ctx, 1); err != nil {
		t.Fatalf("Down failed: %v", err)
	}

	// Verify one migration was rolled back
	if count := driver.AppliedCount(); count != 1 {
		t.Errorf("AppliedCount after Down = %d, want 1", count)
	}
}

// TestMockDriver_MixedMigrations tests mixing SQL and Go function migrations.
func TestMockDriver_MixedMigrations(t *testing.T) {
	driver := New()
	defer func() { _ = driver.Close() }()

	q := queen.New(driver)

	// SQL migration
	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_table",
		UpSQL:   `CREATE TABLE test (id INTEGER)`,
		DownSQL: `DROP TABLE test`,
	})

	// Go function migration
	q.MustAdd(queen.M{
		Version:        "002",
		Name:           "insert_data",
		ManualChecksum: "v1",
		UpFunc: func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, "INSERT INTO test (id) VALUES (1)")
			return err
		},
		DownFunc: func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, "DELETE FROM test WHERE id = 1")
			return err
		},
	})

	ctx := context.Background()

	// Apply all migrations
	if err := q.Up(ctx); err != nil {
		t.Fatalf("Up failed: %v", err)
	}

	if count := driver.AppliedCount(); count != 2 {
		t.Errorf("AppliedCount = %d, want 2", count)
	}
}
