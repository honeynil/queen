//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/honeynil/queen"
	"github.com/honeynil/queen/drivers/postgres"
	helpers "github.com/honeynil/queen/tests/integration"
)

func setupPostgres(t *testing.T) (*sql.DB, func()) {
	t.Helper()

	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(90 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Logger:           helpers.NopLogger{},
	})
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("failed to get container port: %v", err)
	}

	dsn := "postgres://test:test@" + host + ":" + port.Port() + "/testdb?sslmode=disable"
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}

	cleanup := func() {
		_ = db.Close()
		_ = container.Terminate(ctx)
	}

	return db, cleanup
}

func TestPostgresIntegration_BasicMigration(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()
	driver := postgres.New(db)
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

	if statuses[0].Status != queen.StatusApplied {
		t.Errorf("migration status = %v, want %v", statuses[0].Status, queen.StatusApplied)
	}

	err = q.Down(ctx, 1)
	if err != nil {
		t.Fatalf("failed to rollback migration: %v", err)
	}

	if helpers.TableExists(t, db, "users") {
		t.Error("users table should not exist after rollback")
	}
}

func TestPostgresIntegration_MultipleMigrations(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()
	driver := postgres.New(db)
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

func TestPostgresIntegration_Lock(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()
	driver := postgres.New(db)

	err := driver.Lock(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}

	err = driver.Unlock(ctx)
	if err != nil {
		t.Fatalf("failed to release lock: %v", err)
	}

	err = driver.Lock(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to re-acquire lock: %v", err)
	}

	err = driver.Unlock(ctx)
	if err != nil {
		t.Fatalf("failed to release lock again: %v", err)
	}
}

func TestPostgresIntegration_ConcurrentMigrations(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()

	driver1 := postgres.New(db)
	q1 := queen.New(driver1)
	q1.MustAdd(helpers.TestMigration001)

	driver2 := postgres.New(db)
	q2 := queen.New(driver2)
	q2.MustAdd(helpers.TestMigration001)

	errCh := make(chan error, 2)

	go func() {
		errCh <- q1.Up(ctx)
	}()

	go func() {
		time.Sleep(50 * time.Millisecond)
		errCh <- q2.Up(ctx)
	}()

	err1 := <-errCh
	err2 := <-errCh

	if err1 != nil && err2 != nil {
		t.Fatalf("both migrations failed: err1=%v, err2=%v", err1, err2)
	}

	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM queen_migrations WHERE version = '001'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count migrations: %v", err)
	}

	if count != 1 {
		t.Errorf("migration should be applied exactly once, got %d", count)
	}
}

func TestPostgresIntegration_Validation(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()
	driver := postgres.New(db)
	q := queen.New(driver)

	q.MustAdd(helpers.TestMigration001)
	q.MustAdd(helpers.TestMigration002)

	err := q.UpSteps(ctx, 1)
	if err != nil {
		t.Fatalf("failed to apply first migration: %v", err)
	}

	err = q.Validate(ctx)
	if err != nil {
		t.Errorf("validation should pass: %v", err)
	}

	_, err = db.Exec("UPDATE queen_migrations SET checksum = 'invalid' WHERE version = '001'")
	if err != nil {
		t.Fatalf("failed to modify checksum: %v", err)
	}

	err = q.Validate(ctx)
	if err == nil {
		t.Error("validation should fail with modified checksum")
	}
}

func TestPostgresIntegration_TransactionRollback(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()
	driver := postgres.New(db)
	q := queen.New(driver)

	q.MustAdd(helpers.TestMigration001)

	q.MustAdd(queen.M{
		Version: "002",
		Name:    "migration_with_error",
		UpSQL: `
			CREATE TABLE test_table (id INT PRIMARY KEY);
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

func TestPostgresIntegration_UpSteps(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()
	driver := postgres.New(db)
	q := queen.New(driver)

	q.MustAdd(helpers.TestMigration001)
	q.MustAdd(helpers.TestMigration002)
	q.MustAdd(queen.M{
		Version: "003",
		Name:    "create_comments",
		UpSQL:   `CREATE TABLE comments (id SERIAL PRIMARY KEY, text TEXT)`,
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

func TestPostgresIntegration_DownSteps(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()
	driver := postgres.New(db)
	q := queen.New(driver)

	q.MustAdd(helpers.TestMigration001)
	q.MustAdd(helpers.TestMigration002)
	q.MustAdd(queen.M{
		Version: "003",
		Name:    "create_comments",
		UpSQL:   `CREATE TABLE comments (id SERIAL PRIMARY KEY, text TEXT)`,
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

func TestPostgresIntegration_ErrorInDownMigration(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()
	driver := postgres.New(db)
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
