//go:build integration

package mssql_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/yaop-labs/queen"
	"github.com/yaop-labs/queen/drivers/mssql"
	helpers "github.com/yaop-labs/queen/tests/integration"
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
	competitor := mssql.New(db)

	if err := driver.Lock(ctx, time.Second); err != nil {
		t.Fatalf("failed to acquire application lock: %v", err)
	}
	if err := competitor.Lock(ctx, 20*time.Millisecond); !errors.Is(err, queen.ErrLockTimeout) {
		t.Fatalf("competing Lock() error = %v; want ErrLockTimeout", err)
	}
	if err := driver.Unlock(ctx); err != nil {
		t.Fatalf("failed to release application lock: %v", err)
	}

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

	var action, status, checksum string
	err = db.QueryRowContext(ctx,
		"SELECT action, status, checksum FROM queen_migrations WHERE version = @p1",
		"001",
	).Scan(&action, &status, &checksum)
	if err != nil {
		t.Fatalf("failed to read migration metadata: %v", err)
	}
	if action != "apply" || status != "success" {
		t.Fatalf("migration metadata = action %q, status %q; want apply/success", action, status)
	}

	if _, err := db.ExecContext(ctx,
		"UPDATE queen_migrations SET checksum = @p1 WHERE version = @p2",
		"invalid", "001",
	); err != nil {
		t.Fatalf("failed to introduce checksum drift: %v", err)
	}
	if err := q.Down(ctx, 1); !errors.Is(err, queen.ErrChecksumMismatch) {
		t.Fatalf("Down() with checksum drift error = %v; want ErrChecksumMismatch", err)
	}
	if _, err := db.ExecContext(ctx,
		"UPDATE queen_migrations SET checksum = @p1 WHERE version = @p2",
		checksum, "001",
	); err != nil {
		t.Fatalf("failed to restore checksum: %v", err)
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

	q.MustAdd(queen.M{
		Version: "002",
		Name:    "record_failure_rolls_back_body",
		UpSQL: `
			DROP TABLE queen_migrations;
			CREATE TABLE test_table (id INT PRIMARY KEY);
			INSERT INTO test_table VALUES (1);
		`,
		DownSQL: `DROP TABLE IF EXISTS test_table`,
	})

	err = q.UpSteps(ctx, 1)
	if err == nil {
		t.Fatal("expected migration record failure")
	}

	if helpers.TableExists(t, db, "test_table") {
		t.Error("migration body should roll back when its record cannot be written")
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
