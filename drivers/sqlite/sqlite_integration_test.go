//go:build integration

package sqlite_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/honeynil/queen"
	"github.com/honeynil/queen/drivers/sqlite"
	helpers "github.com/honeynil/queen/tests/integration"
)

func setupSQLite(t *testing.T) (*sql.DB, func()) {
	t.Helper()

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("failed to connect to sqlite: %v", err)
	}

	cleanup := func() {
		_ = db.Close()
		_ = os.Remove(dbPath)
	}

	return db, cleanup
}

func TestSQLiteIntegration_BasicMigration(t *testing.T) {
	db, cleanup := setupSQLite(t)
	defer cleanup()

	ctx := context.Background()
	driver := sqlite.New(db)
	q := queen.New(driver)

	q.MustAdd(helpers.TestMigration001)

	err := q.Up(ctx)
	if err != nil {
		t.Fatalf("failed to apply migration: %v", err)
	}

	if !helpers.TableExists(t, db, "users") {
		t.Error("users table should exist after migration")
	}

	statuses, err := q.Status(ctx)
	if err != nil {
		t.Fatalf("failed to get status: %v", err)
	}

	if len(statuses) != 1 {
		t.Fatalf("expected 1 migration status, got %d", len(statuses))
	}

	err = q.Down(ctx, 1)
	if err != nil {
		t.Fatalf("failed to rollback migration: %v", err)
	}

	if helpers.TableExists(t, db, "users") {
		t.Error("users table should not exist after rollback")
	}
}

func TestSQLiteIntegration_MultipleMigrations(t *testing.T) {
	db, cleanup := setupSQLite(t)
	defer cleanup()

	ctx := context.Background()
	driver := sqlite.New(db)
	q := queen.New(driver)

	q.MustAdd(helpers.TestMigration001)
	q.MustAdd(helpers.TestMigration002)

	err := q.Up(ctx)
	if err != nil {
		t.Fatalf("failed to apply migrations: %v", err)
	}

	if !helpers.TableExists(t, db, "users") {
		t.Error("users table should exist")
	}
	if !helpers.TableExists(t, db, "posts") {
		t.Error("posts table should exist")
	}

	statuses, err := q.Status(ctx)
	if err != nil {
		t.Fatalf("failed to get status: %v", err)
	}

	if len(statuses) != 2 {
		t.Fatalf("expected 2 migration statuses, got %d", len(statuses))
	}

	err = q.Reset(ctx)
	if err != nil {
		t.Fatalf("failed to reset: %v", err)
	}

	if helpers.TableExists(t, db, "users") {
		t.Error("users table should not exist after reset")
	}
	if helpers.TableExists(t, db, "posts") {
		t.Error("posts table should not exist after reset")
	}
}

func TestSQLiteIntegration_Persistence(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "persistent.db")

	{
		db, err := sql.Open("sqlite3", dbPath)
		if err != nil {
			t.Fatalf("failed to connect to sqlite: %v", err)
		}

		driver := sqlite.New(db)
		q := queen.New(driver)

		q.MustAdd(helpers.TestMigration001)
		q.MustAdd(helpers.TestMigration002)

		err = q.Up(context.Background())
		if err != nil {
			t.Fatalf("failed to apply migrations: %v", err)
		}

		_ = db.Close()
	}

	{
		db, err := sql.Open("sqlite3", dbPath)
		if err != nil {
			t.Fatalf("failed to reconnect to sqlite: %v", err)
		}
		defer func() { _ = db.Close() }()

		driver := sqlite.New(db)
		q := queen.New(driver)

		q.MustAdd(helpers.TestMigration001)
		q.MustAdd(helpers.TestMigration002)

		statuses, err := q.Status(context.Background())
		if err != nil {
			t.Fatalf("failed to get status: %v", err)
		}

		if len(statuses) != 2 {
			t.Fatalf("expected 2 migrations, got %d", len(statuses))
		}

		for _, status := range statuses {
			if status.Status != queen.StatusApplied {
				t.Errorf("migration %s should be applied, got status %v", status.Version, status.Status)
			}
		}

		if !helpers.TableExists(t, db, "users") {
			t.Error("users table should persist across sessions")
		}
		if !helpers.TableExists(t, db, "posts") {
			t.Error("posts table should persist across sessions")
		}
	}
}

func TestSQLiteIntegration_InMemory(t *testing.T) {
	// In-memory database
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to connect to sqlite: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	driver := sqlite.New(db)
	q := queen.New(driver)

	q.MustAdd(helpers.TestMigration001)

	err = q.Up(ctx)
	if err != nil {
		t.Fatalf("failed to apply migration: %v", err)
	}

	if !helpers.TableExists(t, db, "users") {
		t.Error("users table should exist in memory database")
	}

	_, err = db.Exec("INSERT INTO users (id, name, email) VALUES (1, 'Test User', 'test@example.com')")
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	count := helpers.CountRows(t, db, "users")
	if count != 1 {
		t.Errorf("expected 1 row in users table, got %d", count)
	}
}

func TestSQLiteIntegration_TransactionRollback(t *testing.T) {
	db, cleanup := setupSQLite(t)
	defer cleanup()

	ctx := context.Background()
	driver := sqlite.New(db)
	q := queen.New(driver)

	q.MustAdd(helpers.TestMigration001)

	q.MustAdd(queen.M{
		Version: "002",
		Name:    "migration_with_error",
		UpSQL: `
			CREATE TABLE test_table (id INTEGER PRIMARY KEY);
			INSERT INTO test_table VALUES (1);
			-- This will fail - syntax error
			INVALID SQL STATEMENT;
			INSERT INTO test_table VALUES (2);
		`,
		DownSQL: `DROP TABLE IF EXISTS test_table`,
	})

	err := q.UpSteps(ctx, 1)
	if err != nil {
		t.Fatalf("failed to apply first migration: %v", err)
	}

	err = q.UpSteps(ctx, 1)
	if err == nil {
		t.Fatal("expected error when applying migration with invalid SQL")
	}

	if helpers.TableExists(t, db, "test_table") {
		t.Error("test_table should not exist after failed migration (transaction rollback)")
	}

	statuses, err := q.Status(ctx)
	if err != nil {
		t.Fatalf("failed to get status: %v", err)
	}

	appliedCount := 0
	for _, s := range statuses {
		if s.Status == queen.StatusApplied {
			appliedCount++
		}
	}

	if appliedCount != 1 {
		t.Errorf("expected 1 applied migration, got %d", appliedCount)
	}
}

func TestSQLiteIntegration_UpSteps(t *testing.T) {
	db, cleanup := setupSQLite(t)
	defer cleanup()

	ctx := context.Background()
	driver := sqlite.New(db)
	q := queen.New(driver)

	q.MustAdd(helpers.TestMigration001)
	q.MustAdd(helpers.TestMigration002)
	q.MustAdd(queen.M{
		Version: "003",
		Name:    "create_comments",
		UpSQL:   `CREATE TABLE comments (id INTEGER PRIMARY KEY, text TEXT)`,
		DownSQL: `DROP TABLE comments`,
	})

	err := q.UpSteps(ctx, 2)
	if err != nil {
		t.Fatalf("failed to apply 2 migrations: %v", err)
	}

	if !helpers.TableExists(t, db, "users") {
		t.Error("users table should exist")
	}
	if !helpers.TableExists(t, db, "posts") {
		t.Error("posts table should exist")
	}
	if helpers.TableExists(t, db, "comments") {
		t.Error("comments table should not exist yet")
	}

	err = q.UpSteps(ctx, 1)
	if err != nil {
		t.Fatalf("failed to apply remaining migration: %v", err)
	}

	if !helpers.TableExists(t, db, "comments") {
		t.Error("comments table should exist after applying remaining migration")
	}
}

func TestSQLiteIntegration_DownSteps(t *testing.T) {
	db, cleanup := setupSQLite(t)
	defer cleanup()

	ctx := context.Background()
	driver := sqlite.New(db)
	q := queen.New(driver)

	q.MustAdd(helpers.TestMigration001)
	q.MustAdd(helpers.TestMigration002)
	q.MustAdd(queen.M{
		Version: "003",
		Name:    "create_comments",
		UpSQL:   `CREATE TABLE comments (id INTEGER PRIMARY KEY, text TEXT)`,
		DownSQL: `DROP TABLE comments`,
	})

	err := q.Up(ctx)
	if err != nil {
		t.Fatalf("failed to apply migrations: %v", err)
	}

	err = q.Down(ctx, 2)
	if err != nil {
		t.Fatalf("failed to rollback 2 migrations: %v", err)
	}

	if !helpers.TableExists(t, db, "users") {
		t.Error("users table should still exist")
	}
	if helpers.TableExists(t, db, "posts") {
		t.Error("posts table should be rolled back")
	}
	if helpers.TableExists(t, db, "comments") {
		t.Error("comments table should be rolled back")
	}

	statuses, err := q.Status(ctx)
	if err != nil {
		t.Fatalf("failed to get status: %v", err)
	}

	appliedCount := 0
	for _, s := range statuses {
		if s.Status == queen.StatusApplied {
			appliedCount++
		}
	}

	if appliedCount != 1 {
		t.Errorf("expected 1 applied migration, got %d", appliedCount)
	}
}

func TestSQLiteIntegration_ErrorInDownMigration(t *testing.T) {
	db, cleanup := setupSQLite(t)
	defer cleanup()

	ctx := context.Background()
	driver := sqlite.New(db)
	q := queen.New(driver)

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "migration_with_down_error",
		UpSQL:   `CREATE TABLE test_table (id INTEGER PRIMARY KEY)`,
		DownSQL: `INVALID SQL IN DOWN MIGRATION`,
	})

	err := q.Up(ctx)
	if err != nil {
		t.Fatalf("failed to apply migration: %v", err)
	}

	if !helpers.TableExists(t, db, "test_table") {
		t.Fatal("test_table should exist after up migration")
	}

	err = q.Down(ctx, 1)
	if err == nil {
		t.Error("expected error when rolling back with invalid Down SQL")
	}

	if !helpers.TableExists(t, db, "test_table") {
		t.Error("test_table should still exist after failed rollback")
	}
}
