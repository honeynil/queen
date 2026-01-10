package queen_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/honeynil/queen"
	"github.com/honeynil/queen/drivers/mock"
)

// Example demonstrates basic usage of Queen migrations.
func Example() {
	driver := mock.New()
	q := queen.New(driver)
	defer q.Close()

	// Register migrations
	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL:   `CREATE TABLE users (id SERIAL PRIMARY KEY, email VARCHAR(255))`,
		DownSQL: `DROP TABLE users`,
	})

	q.MustAdd(queen.M{
		Version: "002",
		Name:    "add_users_name",
		UpSQL:   `ALTER TABLE users ADD COLUMN name VARCHAR(255)`,
		DownSQL: `ALTER TABLE users DROP COLUMN name`,
	})

	// Apply all pending migrations
	ctx := context.Background()
	if err := q.Up(ctx); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Migrations applied successfully!")
}

// Example_goFunctionMigration demonstrates using Go functions for complex migrations.
func Example_goFunctionMigration() {
	driver := mock.New()
	q := queen.New(driver)
	defer q.Close()

	// SQL migration to create table
	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL:   `CREATE TABLE users (id SERIAL PRIMARY KEY, email VARCHAR(255))`,
		DownSQL: `DROP TABLE users`,
	})

	// Go function migration for complex data transformation
	q.MustAdd(queen.M{
		Version:        "002",
		Name:           "normalize_emails",
		ManualChecksum: "v1", // Track function changes
		UpFunc: func(ctx context.Context, tx *sql.Tx) error {
			rows, err := tx.QueryContext(ctx, "SELECT id, email FROM users")
			if err != nil {
				return err
			}
			defer rows.Close()

			for rows.Next() {
				var id int
				var email string
				if err := rows.Scan(&id, &email); err != nil {
					return err
				}

				normalized := strings.ToLower(strings.TrimSpace(email))

				_, err = tx.ExecContext(ctx,
					"UPDATE users SET email = $1 WHERE id = $2",
					normalized, id)
				if err != nil {
					return err
				}
			}

			return rows.Err()
		},
	})

	q.Up(context.Background())
}

// Example_modularMigrations demonstrates organizing migrations by domain.
func Example_modularMigrations() {
	driver := mock.New()
	q := queen.New(driver)
	defer q.Close()

	// Register migrations from different modules
	registerUserMigrations(q)
	registerPostMigrations(q)

	q.Up(context.Background())
}

func registerUserMigrations(q *queen.Queen) {
	q.MustAdd(queen.M{
		Version: "users_001",
		Name:    "create_users",
		UpSQL:   `CREATE TABLE users (id SERIAL PRIMARY KEY)`,
		DownSQL: `DROP TABLE users`,
	})
}

func registerPostMigrations(q *queen.Queen) {
	q.MustAdd(queen.M{
		Version: "posts_001",
		Name:    "create_posts",
		UpSQL:   `CREATE TABLE posts (id SERIAL PRIMARY KEY)`,
		DownSQL: `DROP TABLE posts`,
	})
}

// Example_testing demonstrates testing migrations.
func Example_testing() {
	// In your test
	testFunc := func(t *testing.T) {
		driver := setupTestDB(t) // Your test DB setup

		q := queen.NewTest(t, driver) // Auto-cleanup on test end

		q.MustAdd(queen.M{
			Version: "001",
			Name:    "create_users",
			UpSQL:   `CREATE TABLE users (id INT)`,
			DownSQL: `DROP TABLE users`,
		})

		// Test both up and down migrations
		q.TestUpDown()
	}

	// Run the test (in real code, use go test)
	t := &testing.T{}
	testFunc(t)
}

// Example_status demonstrates checking migration status.
func Example_status() {
	driver := mock.New()
	q := queen.New(driver)
	defer q.Close()

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL:   `CREATE TABLE users (id INT)`,
		DownSQL: `DROP TABLE users`,
	})

	ctx := context.Background()

	// Check status of all migrations
	statuses, err := q.Status(ctx)
	if err != nil {
		log.Fatal(err)
	}

	for _, s := range statuses {
		fmt.Printf("Version: %s, Name: %s, Status: %s\n",
			s.Version, s.Name, s.Status)
	}
}

// Example_configuration demonstrates custom configuration.
func Example_configuration() {
	driver := mock.New()

	config := &queen.Config{
		TableName:   "custom_migrations", // Custom table name
		LockTimeout: 30 * time.Minute,
		SkipLock:    false, // Enable lock protection
	}

	q := queen.NewWithConfig(driver, config)
	defer q.Close()

	q.Up(context.Background())
}

// ExampleQueen_Up demonstrates applying all pending migrations.
func ExampleQueen_Up() {
	driver := mock.New()
	q := queen.New(driver)
	defer q.Close()

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL:   `CREATE TABLE users (id INT)`,
		DownSQL: `DROP TABLE users`,
	})

	ctx := context.Background()
	if err := q.Up(ctx); err != nil {
		log.Fatal(err)
	}

	fmt.Println("All migrations applied")
}

// ExampleQueen_UpSteps demonstrates applying a specific number of migrations.
func ExampleQueen_UpSteps() {
	driver := mock.New()
	q := queen.New(driver)
	defer q.Close()

	q.MustAdd(queen.M{Version: "001", Name: "migration_1", UpSQL: "..."})
	q.MustAdd(queen.M{Version: "002", Name: "migration_2", UpSQL: "..."})
	q.MustAdd(queen.M{Version: "003", Name: "migration_3", UpSQL: "..."})

	ctx := context.Background()

	// Apply only the next 2 migrations
	if err := q.UpSteps(ctx, 2); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Applied 2 migrations")
}

// ExampleQueen_Down demonstrates rolling back migrations.
func ExampleQueen_Down() {
	driver := mock.New()
	q := queen.New(driver)
	defer q.Close()

	ctx := context.Background()

	// Rollback last migration
	if err := q.Down(ctx, 1); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Rolled back 1 migration")
}

// ExampleQueen_Reset demonstrates rolling back all migrations.
func ExampleQueen_Reset() {
	driver := mock.New()
	q := queen.New(driver)
	defer q.Close()

	ctx := context.Background()
	if err := q.Reset(ctx); err != nil {
		log.Fatal(err)
	}

	fmt.Println("All migrations rolled back")
}

// ExampleQueen_Validate demonstrates validating migrations.
func ExampleQueen_Validate() {
	driver := mock.New()
	q := queen.New(driver)
	defer q.Close()

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL:   `CREATE TABLE users (id INT)`,
	})

	ctx := context.Background()
	if err := q.Validate(ctx); err != nil {
		log.Fatalf("Validation failed: %v", err)
	}

	fmt.Println("All migrations valid")
}

// ExampleNewTest demonstrates using the testing helper.
func ExampleNewTest() {
	testFunc := func(t *testing.T) {
		driver := setupTestDB(t)

		// NewTest automatically cleans up when test ends
		q := queen.NewTest(t, driver)

		q.MustAdd(queen.M{
			Version: "001",
			Name:    "create_users",
			UpSQL:   `CREATE TABLE users (id INT)`,
			DownSQL: `DROP TABLE users`,
		})

		// Test migrations
		q.MustUp()
		q.MustValidate()

		fmt.Println("Test passed")
	}

	t := &testing.T{}
	testFunc(t)
}

// ExampleTestHelper_TestUpDown demonstrates testing up and down migrations.
func ExampleTestHelper_TestUpDown() {
	testFunc := func(t *testing.T) {
		driver := setupTestDB(t)
		q := queen.NewTest(t, driver)

		q.MustAdd(queen.M{
			Version: "001",
			Name:    "create_users",
			UpSQL:   `CREATE TABLE users (id INT)`,
			DownSQL: `DROP TABLE users`,
		})

		// TestUpDown applies all migrations, then rolls them back
		q.TestUpDown()

		fmt.Println("Up and down migrations work correctly")
	}

	t := &testing.T{}
	testFunc(t)
}

func setupTestDB(_ *testing.T) queen.Driver {
	// Your test database setup logic
	// This is just a placeholder for the example
	return mock.New()
}
