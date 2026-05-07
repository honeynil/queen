//go:build integration

package mssql_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/honeynil/queen"
	"github.com/honeynil/queen/drivers/mssql"
	helpers "github.com/honeynil/queen/tests/integration"
)

func setupMSSQL(t *testing.T) (*sql.DB, func()) {
	t.Helper()

	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "mcr.microsoft.com/mssql/server:2022-latest",
		ExposedPorts: []string{"1433/tcp"},
		Env: map[string]string{
			"ACCEPT_EULA": "Y",
			"SA_PASSWORD": "YourStrong!Passw0rd",
			"MSSQL_PID":   "Express",
		},
		WaitingFor: wait.ForLog("SQL Server is now ready for client connections").
			WithStartupTimeout(120 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Logger:           helpers.NopLogger{},
	})
	if err != nil {
		t.Fatalf("failed to start mssql container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "1433")
	if err != nil {
		t.Fatalf("failed to get container port: %v", err)
	}

	dsn := "sqlserver://sa:YourStrong!Passw0rd@" + host + ":" + port.Port() + "?database=master&encrypt=disable"
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		t.Fatalf("failed to connect to mssql: %v", err)
	}

	helpers.WaitForDB(t, db, 60*time.Second)

	cleanup := func() {
		_ = db.Close()
		_ = container.Terminate(ctx)
	}

	return db, cleanup
}

func TestMSSQLIntegration_BasicMigration(t *testing.T) {
	db, cleanup := setupMSSQL(t)
	defer cleanup()

	ctx := context.Background()
	driver := mssql.New(db)
	q := queen.New(driver)

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL: `
			CREATE TABLE users (
				id INT IDENTITY(1,1) PRIMARY KEY,
				name NVARCHAR(255) NOT NULL,
				email NVARCHAR(255) NOT NULL
			)
		`,
		DownSQL: `DROP TABLE IF EXISTS users`,
	})

	err := q.Up(ctx)
	if err != nil {
		t.Fatalf("failed to apply migration: %v", err)
	}

	if !helpers.TableExists(t, db, "users") {
		t.Error("users table should exist after migration")
	}

	err = q.Down(ctx, 1)
	if err != nil {
		t.Fatalf("failed to rollback migration: %v", err)
	}

	if helpers.TableExists(t, db, "users") {
		t.Error("users table should not exist after rollback")
	}
}

func TestMSSQLIntegration_MultipleMigrations(t *testing.T) {
	db, cleanup := setupMSSQL(t)
	defer cleanup()

	ctx := context.Background()
	driver := mssql.New(db)
	q := queen.New(driver)

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL: `
			CREATE TABLE users (
				id INT IDENTITY(1,1) PRIMARY KEY,
				name NVARCHAR(255)
			)
		`,
		DownSQL: `DROP TABLE IF EXISTS users`,
	})

	q.MustAdd(queen.M{
		Version: "002",
		Name:    "create_posts",
		UpSQL: `
			CREATE TABLE posts (
				id INT IDENTITY(1,1) PRIMARY KEY,
				user_id INT NOT NULL,
				title NVARCHAR(255)
			)
		`,
		DownSQL: `DROP TABLE IF EXISTS posts`,
	})

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

func TestMSSQLIntegration_TransactionRollback(t *testing.T) {
	db, cleanup := setupMSSQL(t)
	defer cleanup()

	ctx := context.Background()
	driver := mssql.New(db)
	q := queen.New(driver)

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL: `
			CREATE TABLE users (
				id INT IDENTITY(1,1) PRIMARY KEY,
				name NVARCHAR(255)
			)
		`,
		DownSQL: `DROP TABLE IF EXISTS users`,
	})

	// Apply first migration
	err := q.Up(ctx)
	if err != nil {
		t.Fatalf("failed to apply first migration: %v", err)
	}

	// Note: MSSQL DDL (CREATE TABLE, DROP TABLE) auto-commits and cannot be rolled back
	// So we test rollback with DML operations instead
	q.MustAdd(queen.M{
		Version: "002",
		Name:    "insert_with_error",
		UpSQL: `
			INSERT INTO users (name) VALUES ('User 1');
			INSERT INTO users (name) VALUES ('User 2');
			-- This will fail - syntax error, should rollback INSERTs
			INVALID SQL STATEMENT HERE;
			INSERT INTO users (name) VALUES ('User 3');
		`,
		DownSQL: `DELETE FROM users WHERE name IN ('User 1', 'User 2', 'User 3')`,
	})

	// Try to apply second migration (should fail and rollback)
	err = q.UpSteps(ctx, 1)
	if err == nil {
		t.Fatal("expected error when applying migration with invalid SQL")
	}

	// Verify that INSERTs were rolled back (no users should be inserted)
	count := helpers.CountRows(t, db, "users")
	if count != 0 {
		t.Errorf("expected 0 rows after failed migration rollback, got %d", count)
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

func TestMSSQLIntegration_UpSteps(t *testing.T) {
	db, cleanup := setupMSSQL(t)
	defer cleanup()

	ctx := context.Background()
	driver := mssql.New(db)
	q := queen.New(driver)

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL:   `CREATE TABLE users (id INT PRIMARY KEY, name NVARCHAR(255))`,
		DownSQL: `DROP TABLE IF EXISTS users`,
	})
	q.MustAdd(queen.M{
		Version: "002",
		Name:    "create_posts",
		UpSQL:   `CREATE TABLE posts (id INT PRIMARY KEY, title NVARCHAR(255))`,
		DownSQL: `DROP TABLE IF EXISTS posts`,
	})
	q.MustAdd(queen.M{
		Version: "003",
		Name:    "create_comments",
		UpSQL:   `CREATE TABLE comments (id INT PRIMARY KEY, text NVARCHAR(MAX))`,
		DownSQL: `DROP TABLE IF EXISTS comments`,
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

func TestMSSQLIntegration_DownSteps(t *testing.T) {
	db, cleanup := setupMSSQL(t)
	defer cleanup()

	ctx := context.Background()
	driver := mssql.New(db)
	q := queen.New(driver)

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL:   `CREATE TABLE users (id INT PRIMARY KEY, name NVARCHAR(255))`,
		DownSQL: `DROP TABLE IF EXISTS users`,
	})
	q.MustAdd(queen.M{
		Version: "002",
		Name:    "create_posts",
		UpSQL:   `CREATE TABLE posts (id INT PRIMARY KEY, title NVARCHAR(255))`,
		DownSQL: `DROP TABLE IF EXISTS posts`,
	})
	q.MustAdd(queen.M{
		Version: "003",
		Name:    "create_comments",
		UpSQL:   `CREATE TABLE comments (id INT PRIMARY KEY, text NVARCHAR(MAX))`,
		DownSQL: `DROP TABLE IF EXISTS comments`,
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
}

func TestMSSQLIntegration_ErrorInDownMigration(t *testing.T) {
	db, cleanup := setupMSSQL(t)
	defer cleanup()

	ctx := context.Background()
	driver := mssql.New(db)
	q := queen.New(driver)

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "migration_with_down_error",
		UpSQL:   `CREATE TABLE test_table (id INT PRIMARY KEY)`,
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
