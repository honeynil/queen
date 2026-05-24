//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/yaop-labs/queen"
	"github.com/yaop-labs/queen/drivers/postgres"
	helpers "github.com/yaop-labs/queen/tests/integration"
)

func setupPostgres(t *testing.T) (*sql.DB, func()) {
	t.Helper()

	db, _, cleanup := setupPostgresWithDSN(t)
	return db, cleanup
}

func setupPostgresWithDSN(t *testing.T) (*sql.DB, string, func()) {
	t.Helper()

	ctx := context.Background()

	if dsn := postgresTestDSN(); dsn != "" {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatalf("failed to connect to postgres from DSN: %v", err)
		}
		helpers.WaitForDB(t, db, 30*time.Second)
		return db, dsn, func() { _ = db.Close() }
	}

	defer func() {
		if r := recover(); r != nil {
			t.Skipf("testcontainers runtime unavailable: %v", r)
		}
	}()

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
		if isContainerRuntimeUnavailable(err) {
			t.Skipf("testcontainers runtime unavailable: %v", err)
		}
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

	return db, dsn, cleanup
}

func postgresTestDSN() string {
	if dsn := os.Getenv("POSTGRESQL_TEST_DSN"); dsn != "" {
		return dsn
	}
	return os.Getenv("DATABASE_URL")
}

func isContainerRuntimeUnavailable(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "Cannot connect to the Docker daemon") ||
		strings.Contains(msg, "checked path: $XDG_RUNTIME_DIR") ||
		strings.Contains(msg, "permission denied while trying to connect to the Docker daemon")
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

func TestPostgresIntegration_ProductionInvariants(t *testing.T) {
	db, dsn, cleanup := setupPostgresWithDSN(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("mixed SQL and function migrations run atomically in documented order", func(t *testing.T) {
		migrationTable := "queen_migrations_mixed_order"
		mustExecPostgres(t, db, `DROP TABLE IF EXISTS mixed_order_log`)
		mustExecPostgres(t, db, `DROP TABLE IF EXISTS `+migrationTable)
		mustExecPostgres(t, db, `CREATE TABLE mixed_order_log (seq SERIAL PRIMARY KEY, label TEXT NOT NULL)`)

		driver := postgres.NewWithTableName(db, migrationTable)
		q := queen.NewWithConfig(driver, &queen.Config{TableName: migrationTable})

		q.MustAdd(queen.M{
			Version: "001",
			Name:    "mixed_order",
			UpSQL:   `INSERT INTO mixed_order_log(label) VALUES ('up_sql')`,
			UpFunc: func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `INSERT INTO mixed_order_log(label) VALUES ('up_func')`)
				return err
			},
			DownFunc: func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `INSERT INTO mixed_order_log(label) VALUES ('down_func')`)
				return err
			},
			DownSQL: `INSERT INTO mixed_order_log(label) VALUES ('down_sql')`,
		})

		if err := q.Up(ctx); err != nil {
			t.Fatalf("Up() failed: %v", err)
		}
		assertPostgresLabels(t, db, []string{"up_sql", "up_func"})

		if err := q.Down(ctx, 1); err != nil {
			t.Fatalf("Down() failed: %v", err)
		}
		assertPostgresLabels(t, db, []string{"up_sql", "up_func", "down_func", "down_sql"})
	})

	t.Run("function failure after SQL rolls back schema and migration record", func(t *testing.T) {
		migrationTable := "queen_migrations_func_failure"
		bodyTable := "func_failure_body"
		mustExecPostgres(t, db, `DROP TABLE IF EXISTS `+bodyTable)
		mustExecPostgres(t, db, `DROP TABLE IF EXISTS `+migrationTable)

		driver := postgres.NewWithTableName(db, migrationTable)
		q := queen.NewWithConfig(driver, &queen.Config{TableName: migrationTable})

		q.MustAdd(queen.M{
			Version: "001",
			Name:    "func_failure_after_sql",
			UpSQL:   `CREATE TABLE func_failure_body (id INT PRIMARY KEY)`,
			UpFunc: func(context.Context, *sql.Tx) error {
				return errors.New("forced function failure")
			},
			DownSQL: `DROP TABLE func_failure_body`,
		})

		if err := q.Up(ctx); err == nil {
			t.Fatal("Up() succeeded, want forced function failure")
		}
		if helpers.TableExists(t, db, bodyTable) {
			t.Fatal("SQL body table exists after failed mixed migration")
		}
		assertPostgresMigrationRecordCount(t, db, migrationTable, "001", 0)
	})

	t.Run("record failure rolls back migration body", func(t *testing.T) {
		migrationTable := "queen_migrations_record_failure"
		bodyTable := "record_failure_body"
		triggerName := "record_failure_insert_trigger"
		functionName := "record_failure_insert_fn"
		mustExecPostgres(t, db, `DROP TABLE IF EXISTS `+bodyTable)
		mustExecPostgres(t, db, `DROP TABLE IF EXISTS `+migrationTable)
		mustExecPostgres(t, db, `DROP FUNCTION IF EXISTS `+functionName+`()`)

		driver := postgres.NewWithTableName(db, migrationTable)
		if err := driver.Init(ctx); err != nil {
			t.Fatalf("driver.Init() failed: %v", err)
		}
		installFailingTrigger(t, db, migrationTable, triggerName, functionName, "INSERT")

		q := queen.NewWithConfig(driver, &queen.Config{TableName: migrationTable})
		q.MustAdd(queen.M{
			Version: "001",
			Name:    "record_failure_rolls_back_body",
			UpSQL:   `CREATE TABLE record_failure_body (id INT PRIMARY KEY)`,
			DownSQL: `DROP TABLE record_failure_body`,
		})

		if err := q.Up(ctx); err == nil {
			t.Fatal("Up() succeeded, want forced record failure")
		}
		if helpers.TableExists(t, db, bodyTable) {
			t.Fatal("migration body table exists after record failure")
		}
		assertPostgresMigrationRecordCount(t, db, migrationTable, "001", 0)
	})

	t.Run("remove failure rolls back down body and keeps migration record", func(t *testing.T) {
		migrationTable := "queen_migrations_remove_failure"
		bodyTable := "remove_failure_body"
		triggerName := "remove_failure_delete_trigger"
		functionName := "remove_failure_delete_fn"
		mustExecPostgres(t, db, `DROP TABLE IF EXISTS `+bodyTable)
		mustExecPostgres(t, db, `DROP TABLE IF EXISTS `+migrationTable)
		mustExecPostgres(t, db, `DROP FUNCTION IF EXISTS `+functionName+`()`)

		driver := postgres.NewWithTableName(db, migrationTable)
		q := queen.NewWithConfig(driver, &queen.Config{TableName: migrationTable})
		q.MustAdd(queen.M{
			Version: "001",
			Name:    "remove_failure_rolls_back_down",
			UpSQL:   `CREATE TABLE remove_failure_body (id INT PRIMARY KEY)`,
			DownSQL: `DROP TABLE remove_failure_body`,
		})

		if err := q.Up(ctx); err != nil {
			t.Fatalf("Up() failed: %v", err)
		}
		installFailingTrigger(t, db, migrationTable, triggerName, functionName, "DELETE")

		if err := q.Down(ctx, 1); err == nil {
			t.Fatal("Down() succeeded, want forced remove failure")
		}
		if !helpers.TableExists(t, db, bodyTable) {
			t.Fatal("down body was committed after remove failure")
		}
		assertPostgresMigrationRecordCount(t, db, migrationTable, "001", 1)
	})

	t.Run("second migrator times out while advisory lock is held", func(t *testing.T) {
		migrationTable := "queen_migrations_lock_timeout"
		mustExecPostgres(t, db, `DROP TABLE IF EXISTS `+migrationTable)

		driver1 := postgres.NewWithTableName(db, migrationTable)
		driver2 := postgres.NewWithTableName(db, migrationTable)

		if err := driver1.Lock(ctx, 5*time.Second); err != nil {
			t.Fatalf("driver1.Lock() failed: %v", err)
		}
		defer func() { _ = driver1.Unlock(ctx) }()

		err := driver2.Lock(ctx, 50*time.Millisecond)
		if !errors.Is(err, queen.ErrLockTimeout) {
			t.Fatalf("driver2.Lock() error = %v, want ErrLockTimeout", err)
		}
	})

	t.Run("status detects checksum drift from changed migration code", func(t *testing.T) {
		migrationTable := "queen_migrations_checksum_drift"
		bodyTable := "checksum_drift_body"
		mustExecPostgres(t, db, `DROP TABLE IF EXISTS `+bodyTable)
		mustExecPostgres(t, db, `DROP TABLE IF EXISTS `+migrationTable)

		q1 := queen.NewWithConfig(
			postgres.NewWithTableName(db, migrationTable),
			&queen.Config{TableName: migrationTable},
		)
		q1.MustAdd(queen.M{
			Version: "001",
			Name:    "checksum_drift",
			UpSQL:   `CREATE TABLE checksum_drift_body (id INT PRIMARY KEY)`,
			DownSQL: `DROP TABLE checksum_drift_body`,
		})
		if err := q1.Up(ctx); err != nil {
			t.Fatalf("initial Up() failed: %v", err)
		}

		q2 := queen.NewWithConfig(
			postgres.NewWithTableName(db, migrationTable),
			&queen.Config{TableName: migrationTable},
		)
		q2.MustAdd(queen.M{
			Version: "001",
			Name:    "checksum_drift",
			UpSQL:   `CREATE TABLE checksum_drift_body (id INT PRIMARY KEY, changed INT)`,
			DownSQL: `DROP TABLE checksum_drift_body`,
		})

		statuses, err := q2.Status(ctx)
		if err != nil {
			t.Fatalf("Status() failed: %v", err)
		}
		if len(statuses) != 1 || statuses[0].Status != queen.StatusModified {
			t.Fatalf("statuses = %+v, want one modified migration", statuses)
		}
		if err := q2.Validate(ctx); !errors.Is(err, queen.ErrChecksumMismatch) {
			t.Fatalf("Validate() error = %v, want ErrChecksumMismatch", err)
		}
	})

	t.Run("Down with zero rolls back exactly latest applied migration", func(t *testing.T) {
		migrationTable := "queen_migrations_down_zero"
		firstTable := "down_zero_first"
		secondTable := "down_zero_second"
		mustExecPostgres(t, db, `DROP TABLE IF EXISTS `+secondTable)
		mustExecPostgres(t, db, `DROP TABLE IF EXISTS `+firstTable)
		mustExecPostgres(t, db, `DROP TABLE IF EXISTS `+migrationTable)

		q := queen.NewWithConfig(
			postgres.NewWithTableName(db, migrationTable),
			&queen.Config{TableName: migrationTable},
		)
		q.MustAdd(queen.M{
			Version: "001",
			Name:    "create_first",
			UpSQL:   `CREATE TABLE down_zero_first (id INT PRIMARY KEY)`,
			DownSQL: `DROP TABLE down_zero_first`,
		})
		q.MustAdd(queen.M{
			Version: "002",
			Name:    "create_second",
			UpSQL:   `CREATE TABLE down_zero_second (id INT PRIMARY KEY)`,
			DownSQL: `DROP TABLE down_zero_second`,
		})

		if err := q.Up(ctx); err != nil {
			t.Fatalf("Up() failed: %v", err)
		}
		if err := q.Down(ctx, 0); err != nil {
			t.Fatalf("Down(ctx, 0) failed: %v", err)
		}

		if !helpers.TableExists(t, db, firstTable) {
			t.Fatal("first migration table was rolled back, want it to stay applied")
		}
		if helpers.TableExists(t, db, secondTable) {
			t.Fatal("latest migration table still exists after Down(ctx, 0)")
		}
		assertPostgresMigrationRecordCount(t, db, migrationTable, "001", 1)
		assertPostgresMigrationRecordCount(t, db, migrationTable, "002", 0)
	})

	t.Run("successful apply persists execution metadata", func(t *testing.T) {
		migrationTable := "queen_migrations_metadata"
		bodyTable := "metadata_body"
		mustExecPostgres(t, db, `DROP TABLE IF EXISTS `+bodyTable)
		mustExecPostgres(t, db, `DROP TABLE IF EXISTS `+migrationTable)

		q := queen.NewWithConfig(
			postgres.NewWithTableName(db, migrationTable),
			&queen.Config{TableName: migrationTable},
		)
		q.MustAdd(queen.M{
			Version: "001",
			Name:    "metadata",
			UpSQL:   `CREATE TABLE metadata_body (id INT PRIMARY KEY)`,
			DownSQL: `DROP TABLE metadata_body`,
		})
		if err := q.Up(ctx); err != nil {
			t.Fatalf("Up() failed: %v", err)
		}

		var action, status string
		var durationMS int64
		if err := db.QueryRow(`
			SELECT action, status, duration_ms
			FROM `+migrationTable+`
			WHERE version = $1
		`, "001").Scan(&action, &status, &durationMS); err != nil {
			t.Fatalf("query migration metadata failed: %v", err)
		}
		if action != "apply" {
			t.Fatalf("action = %q, want apply", action)
		}
		if status != "success" {
			t.Fatalf("status = %q, want success", status)
		}
		if durationMS < 0 {
			t.Fatalf("duration_ms = %d, want non-negative", durationMS)
		}
	})

	t.Run("native pgx pool can be used through postgres driver adapter", func(t *testing.T) {
		migrationTable := "queen_migrations_pgxpool"
		bodyTable := "pgxpool_body"
		mustExecPostgres(t, db, `DROP TABLE IF EXISTS `+bodyTable)
		mustExecPostgres(t, db, `DROP TABLE IF EXISTS `+migrationTable)

		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Fatalf("pgxpool.New() failed: %v", err)
		}
		defer pool.Close()

		q := queen.NewWithConfig(
			postgres.NewFromPoolWithTableName(pool, migrationTable),
			&queen.Config{TableName: migrationTable},
		)
		q.MustAdd(queen.M{
			Version: "001",
			Name:    "pgxpool",
			UpSQL:   `CREATE TABLE pgxpool_body (id INT PRIMARY KEY)`,
			DownSQL: `DROP TABLE pgxpool_body`,
		})

		if err := q.Up(ctx); err != nil {
			t.Fatalf("Up() with pgxpool-backed driver failed: %v", err)
		}
		if !helpers.TableExists(t, db, bodyTable) {
			t.Fatal("migration body table does not exist after pgxpool-backed Up()")
		}
		assertPostgresMigrationRecordCount(t, db, migrationTable, "001", 1)

		if err := q.Close(); err != nil {
			t.Fatalf("q.Close() failed: %v", err)
		}
		if err := pool.Ping(ctx); err != nil {
			t.Fatalf("q.Close() closed or broke caller-owned pgxpool: %v", err)
		}
	})
}

func mustExecPostgres(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()

	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("exec %q failed: %v", query, err)
	}
}

func installFailingTrigger(t *testing.T, db *sql.DB, tableName, triggerName, functionName, operation string) {
	t.Helper()

	mustExecPostgres(t, db, `
		CREATE FUNCTION `+functionName+`() RETURNS trigger
		LANGUAGE plpgsql
		AS $$
		BEGIN
			RAISE EXCEPTION 'forced migration metadata failure';
		END;
		$$
	`)
	mustExecPostgres(t, db, `
		CREATE TRIGGER `+triggerName+`
		BEFORE `+operation+` ON `+tableName+`
		FOR EACH ROW EXECUTE FUNCTION `+functionName+`()
	`)
}

func assertPostgresLabels(t *testing.T, db *sql.DB, want []string) {
	t.Helper()

	rows, err := db.Query(`SELECT label FROM mixed_order_log ORDER BY seq`)
	if err != nil {
		t.Fatalf("query mixed_order_log failed: %v", err)
	}
	defer func() { _ = rows.Close() }()

	got := make([]string, 0, len(want))
	for rows.Next() {
		var label string
		if err := rows.Scan(&label); err != nil {
			t.Fatalf("scan mixed_order_log failed: %v", err)
		}
		got = append(got, label)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("mixed_order_log rows error: %v", err)
	}

	if len(got) != len(want) {
		t.Fatalf("labels = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("labels = %v, want %v", got, want)
		}
	}
}

func assertPostgresMigrationRecordCount(t *testing.T, db *sql.DB, tableName, version string, want int) {
	t.Helper()

	var got int
	if err := db.QueryRow(`SELECT COUNT(*) FROM `+tableName+` WHERE version = $1`, version).Scan(&got); err != nil {
		t.Fatalf("count migration records in %s failed: %v", tableName, err)
	}
	if got != want {
		t.Fatalf("migration record count for %s in %s = %d, want %d", version, tableName, got, want)
	}
}
