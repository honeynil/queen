//go:build integration

package clickhouse_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/honeynil/queen"
	"github.com/honeynil/queen/drivers/clickhouse"
	helpers "github.com/honeynil/queen/tests/integration"
)

func setupClickHouse(t *testing.T) (*sql.DB, func()) {
	t.Helper()

	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "clickhouse/clickhouse-server:latest",
		ExposedPorts: []string{"9000/tcp", "8123/tcp"},
		Env: map[string]string{
			"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1",
		},
		WaitingFor: wait.ForAll(
			wait.ForHTTP("/ping").WithPort("8123/tcp"),
			wait.ForListeningPort("9000/tcp"),
		).WithDeadline(90 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Logger:           helpers.NopLogger{},
	})
	if err != nil {
		t.Fatalf("failed to start clickhouse container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "9000")
	if err != nil {
		t.Fatalf("failed to get container port: %v", err)
	}

	dsn := "clickhouse://" + host + ":" + port.Port() + "/default"
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		t.Fatalf("failed to connect to clickhouse: %v", err)
	}

	helpers.WaitForDB(t, db, 60*time.Second)

	cleanup := func() {
		_ = db.Close()
		_ = container.Terminate(ctx)
	}

	return db, cleanup
}

func TestClickHouseIntegration_BasicMigration(t *testing.T) {
	db, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	driver, err := clickhouse.New(db)
	if err != nil {
		t.Fatalf("failed to create driver: %v", err)
	}
	q := queen.New(driver)

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL: `
			CREATE TABLE users (
				id UInt64,
				name String,
				email String
			) ENGINE = MergeTree()
			ORDER BY id
		`,
		DownSQL: `DROP TABLE IF EXISTS users`,
	})

	err = q.Up(ctx)
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

func TestClickHouseIntegration_MultipleMigrations(t *testing.T) {
	db, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	driver, err := clickhouse.New(db)
	if err != nil {
		t.Fatalf("failed to create driver: %v", err)
	}
	q := queen.New(driver)

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL: `
			CREATE TABLE users (
				id UInt64,
				name String
			) ENGINE = MergeTree()
			ORDER BY id
		`,
		DownSQL: `DROP TABLE IF EXISTS users`,
	})

	q.MustAdd(queen.M{
		Version: "002",
		Name:    "create_events",
		UpSQL: `
			CREATE TABLE events (
				id UInt64,
				user_id UInt64,
				event_type String
			) ENGINE = MergeTree()
			ORDER BY id
		`,
		DownSQL: `DROP TABLE IF EXISTS events`,
	})

	err = q.Up(ctx)
	if err != nil {
		t.Fatalf("failed to apply migrations: %v", err)
	}

	if !helpers.TableExists(t, db, "users") {
		t.Error("users table should exist")
	}
	if !helpers.TableExists(t, db, "events") {
		t.Error("events table should exist")
	}

	err = q.Reset(ctx)
	if err != nil {
		t.Fatalf("failed to reset: %v", err)
	}

	if helpers.TableExists(t, db, "users") {
		t.Error("users table should not exist after reset")
	}
	if helpers.TableExists(t, db, "events") {
		t.Error("events table should not exist after reset")
	}
}

func TestClickHouseIntegration_TransactionRollback(t *testing.T) {
	db, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	driver, err := clickhouse.New(db)
	if err != nil {
		t.Fatalf("failed to create driver: %v", err)
	}
	q := queen.New(driver)

	// Note: ClickHouse doesn't fully support transactions like traditional RDBMS
	// We test basic error handling instead

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "migration_with_error",
		UpSQL: `
			CREATE TABLE test_table (id UInt64) ENGINE = MergeTree() ORDER BY id;
			-- This will fail - syntax error
			INVALID SQL STATEMENT;
		`,
		DownSQL: `DROP TABLE IF EXISTS test_table`,
	})

	// Try to apply migration (should fail)
	err = q.Up(ctx)
	if err == nil {
		t.Fatal("expected error when applying migration with invalid SQL")
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

	if appliedCount != 0 {
		t.Errorf("expected 0 applied migrations after error, got %d", appliedCount)
	}
}

func TestClickHouseIntegration_UpSteps(t *testing.T) {
	db, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	driver, err := clickhouse.New(db)
	if err != nil {
		t.Fatalf("failed to create driver: %v", err)
	}
	q := queen.New(driver)

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL:   `CREATE TABLE users (id UInt64) ENGINE = MergeTree() ORDER BY id`,
		DownSQL: `DROP TABLE IF EXISTS users`,
	})
	q.MustAdd(queen.M{
		Version: "002",
		Name:    "create_events",
		UpSQL:   `CREATE TABLE events (id UInt64) ENGINE = MergeTree() ORDER BY id`,
		DownSQL: `DROP TABLE IF EXISTS events`,
	})
	q.MustAdd(queen.M{
		Version: "003",
		Name:    "create_logs",
		UpSQL:   `CREATE TABLE logs (id UInt64) ENGINE = MergeTree() ORDER BY id`,
		DownSQL: `DROP TABLE IF EXISTS logs`,
	})

	err = q.UpSteps(ctx, 2)
	if err != nil {
		t.Fatalf("failed to apply 2 migrations: %v", err)
	}

	if !helpers.TableExists(t, db, "users") {
		t.Error("users table should exist")
	}
	if !helpers.TableExists(t, db, "events") {
		t.Error("events table should exist")
	}
	if helpers.TableExists(t, db, "logs") {
		t.Error("logs table should not exist yet")
	}

	err = q.UpSteps(ctx, 1)
	if err != nil {
		t.Fatalf("failed to apply remaining migration: %v", err)
	}

	if !helpers.TableExists(t, db, "logs") {
		t.Error("logs table should exist after applying remaining migration")
	}
}

func TestClickHouseIntegration_DownSteps(t *testing.T) {
	db, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	driver, err := clickhouse.New(db)
	if err != nil {
		t.Fatalf("failed to create driver: %v", err)
	}
	q := queen.New(driver)

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL:   `CREATE TABLE users (id UInt64) ENGINE = MergeTree() ORDER BY id`,
		DownSQL: `DROP TABLE IF EXISTS users`,
	})
	q.MustAdd(queen.M{
		Version: "002",
		Name:    "create_events",
		UpSQL:   `CREATE TABLE events (id UInt64) ENGINE = MergeTree() ORDER BY id`,
		DownSQL: `DROP TABLE IF EXISTS events`,
	})
	q.MustAdd(queen.M{
		Version: "003",
		Name:    "create_logs",
		UpSQL:   `CREATE TABLE logs (id UInt64) ENGINE = MergeTree() ORDER BY id`,
		DownSQL: `DROP TABLE IF EXISTS logs`,
	})

	err = q.Up(ctx)
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
	if helpers.TableExists(t, db, "events") {
		t.Error("events table should be rolled back")
	}
	if helpers.TableExists(t, db, "logs") {
		t.Error("logs table should be rolled back")
	}
}

func TestClickHouseIntegration_ErrorInDownMigration(t *testing.T) {
	db, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	driver, err := clickhouse.New(db)
	if err != nil {
		t.Fatalf("failed to create driver: %v", err)
	}
	q := queen.New(driver)

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "migration_with_down_error",
		UpSQL:   `CREATE TABLE test_table (id UInt64) ENGINE = MergeTree() ORDER BY id`,
		DownSQL: `INVALID SQL IN DOWN MIGRATION`,
	})

	err = q.Up(ctx)
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

	// Table should still exist (rollback failed but didn't panic)
	if !helpers.TableExists(t, db, "test_table") {
		t.Error("test_table should still exist after failed rollback")
	}
}
