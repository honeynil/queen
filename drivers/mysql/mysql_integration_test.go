//go:build integration

package mysql_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/honeynil/queen"
	"github.com/honeynil/queen/drivers/mysql"
	helpers "github.com/honeynil/queen/tests/integration"
)

func setupMySQL(t *testing.T) (*sql.DB, string, func()) {
	t.Helper()

	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "mysql:8.0",
		ExposedPorts: []string{"3306/tcp"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": "test",
			"MYSQL_DATABASE":      "testdb",
		},
		WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").
			WithStartupTimeout(120 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Logger:           helpers.NopLogger{},
	})
	if err != nil {
		t.Fatalf("failed to start mysql container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "3306")
	if err != nil {
		t.Fatalf("failed to get container port: %v", err)
	}

	dsn := "root:test@tcp(" + host + ":" + port.Port() + ")/testdb?parseTime=true&multiStatements=true"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("failed to connect to mysql: %v", err)
	}

	helpers.WaitForDB(t, db, 30*time.Second)

	cleanup := func() {
		_ = db.Close()
		_ = container.Terminate(ctx)
	}

	return db, dsn, cleanup
}

func TestMySQLIntegration_BasicMigration(t *testing.T) {
	db, _, cleanup := setupMySQL(t)
	defer cleanup()

	ctx := context.Background()
	driver := mysql.New(db)
	q := queen.New(driver)

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL: `
			CREATE TABLE users (
				id INT AUTO_INCREMENT PRIMARY KEY,
				name VARCHAR(255) NOT NULL,
				email VARCHAR(255) UNIQUE NOT NULL
			)
		`,
		DownSQL: `DROP TABLE users`,
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

func TestMySQLIntegration_Lock(t *testing.T) {
	db, _, cleanup := setupMySQL(t)
	defer cleanup()

	ctx := context.Background()
	driver := mysql.New(db)

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

func TestMySQLIntegration_ConcurrentLock(t *testing.T) {
	_, dsn, cleanup := setupMySQL(t)
	defer cleanup()

	ctx := context.Background()

	db1, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Skip("cannot create second connection for concurrency test")
	}
	defer db1.Close()

	db2, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Skip("cannot create third connection for concurrency test")
	}
	defer db2.Close()

	driver1 := mysql.New(db1)
	driver2 := mysql.New(db2)

	err = driver1.Lock(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to acquire lock with driver1: %v", err)
	}
	defer driver1.Unlock(ctx)

	err = driver2.Lock(ctx, 1*time.Second)
	if err == nil {
		t.Error("driver2 should fail to acquire lock while driver1 holds it")
		driver2.Unlock(ctx)
	}
}

func TestMySQLIntegration_MultipleUpDown(t *testing.T) {
	db, _, cleanup := setupMySQL(t)
	defer cleanup()

	ctx := context.Background()
	driver := mysql.New(db)
	q := queen.New(driver)

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL: `
			CREATE TABLE users (
				id INT AUTO_INCREMENT PRIMARY KEY,
				name VARCHAR(255) NOT NULL
			)
		`,
		DownSQL: `DROP TABLE users`,
	})

	q.MustAdd(queen.M{
		Version: "002",
		Name:    "create_posts",
		UpSQL: `
			CREATE TABLE posts (
				id INT AUTO_INCREMENT PRIMARY KEY,
				user_id INT NOT NULL,
				title VARCHAR(255) NOT NULL
			)
		`,
		DownSQL: `DROP TABLE posts`,
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

	err = q.Down(ctx, 1)
	if err != nil {
		t.Fatalf("failed to rollback one migration: %v", err)
	}

	if !helpers.TableExists(t, db, "users") {
		t.Error("users table should still exist")
	}
	if helpers.TableExists(t, db, "posts") {
		t.Error("posts table should not exist after rollback")
	}

	err = q.Up(ctx)
	if err != nil {
		t.Fatalf("failed to reapply migration: %v", err)
	}

	if !helpers.TableExists(t, db, "posts") {
		t.Error("posts table should exist after reapplying")
	}
}

func TestMySQLIntegration_TransactionRollback(t *testing.T) {
	db, _, cleanup := setupMySQL(t)
	defer cleanup()

	ctx := context.Background()
	driver := mysql.New(db)
	q := queen.New(driver)

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL:   `CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255))`,
		DownSQL: `DROP TABLE users`,
	})

	err := q.Up(ctx)
	if err != nil {
		t.Fatalf("failed to apply first migration: %v", err)
	}

	// Note: MySQL DDL (CREATE TABLE, DROP TABLE) auto-commits and cannot be rolled back
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

	err = q.UpSteps(ctx, 1)
	if err == nil {
		t.Fatal("expected error when applying migration with invalid SQL")
	}

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

func TestMySQLIntegration_UpSteps(t *testing.T) {
	db, _, cleanup := setupMySQL(t)
	defer cleanup()

	ctx := context.Background()
	driver := mysql.New(db)
	q := queen.New(driver)

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL:   `CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255))`,
		DownSQL: `DROP TABLE users`,
	})
	q.MustAdd(queen.M{
		Version: "002",
		Name:    "create_posts",
		UpSQL:   `CREATE TABLE posts (id INT AUTO_INCREMENT PRIMARY KEY, title VARCHAR(255))`,
		DownSQL: `DROP TABLE posts`,
	})
	q.MustAdd(queen.M{
		Version: "003",
		Name:    "create_comments",
		UpSQL:   `CREATE TABLE comments (id INT AUTO_INCREMENT PRIMARY KEY, text TEXT)`,
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

func TestMySQLIntegration_DownSteps(t *testing.T) {
	db, _, cleanup := setupMySQL(t)
	defer cleanup()

	ctx := context.Background()
	driver := mysql.New(db)
	q := queen.New(driver)

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL:   `CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255))`,
		DownSQL: `DROP TABLE users`,
	})
	q.MustAdd(queen.M{
		Version: "002",
		Name:    "create_posts",
		UpSQL:   `CREATE TABLE posts (id INT AUTO_INCREMENT PRIMARY KEY, title VARCHAR(255))`,
		DownSQL: `DROP TABLE posts`,
	})
	q.MustAdd(queen.M{
		Version: "003",
		Name:    "create_comments",
		UpSQL:   `CREATE TABLE comments (id INT AUTO_INCREMENT PRIMARY KEY, text TEXT)`,
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

func TestMySQLIntegration_ErrorInDownMigration(t *testing.T) {
	db, _, cleanup := setupMySQL(t)
	defer cleanup()

	ctx := context.Background()
	driver := mysql.New(db)
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
