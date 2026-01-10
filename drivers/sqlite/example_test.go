//go:build cgo
// +build cgo

package sqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/mattn/go-sqlite3"

	"github.com/honeynil/queen"
	"github.com/honeynil/queen/drivers/sqlite"
)

// Example demonstrates basic usage of the SQLite driver.
func Example() {
	// Connect to SQLite database file
	db, err := sql.Open("sqlite3", "myapp.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create SQLite driver
	driver := sqlite.New(db)

	// Create Queen instance
	q := queen.New(driver)
	defer q.Close()

	// Register migrations
	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users_table",
		UpSQL: `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				email TEXT NOT NULL UNIQUE,
				name TEXT,
				created_at TEXT DEFAULT (datetime('now'))
			)
		`,
		DownSQL: `DROP TABLE users`,
	})

	q.MustAdd(queen.M{
		Version: "002",
		Name:    "add_users_bio",
		UpSQL:   `ALTER TABLE users ADD COLUMN bio TEXT`,
		DownSQL: `ALTER TABLE users DROP COLUMN bio`,
	})

	// Apply all pending migrations
	ctx := context.Background()
	if err := q.Up(ctx); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Migrations applied successfully!")
}

// Example_inMemory demonstrates using an in-memory database for testing.
func Example_inMemory() {
	// Use in-memory database (perfect for testing)
	db, _ := sql.Open("sqlite3", ":memory:")
	defer db.Close()

	driver := sqlite.New(db)
	q := queen.New(driver)
	defer q.Close()

	// Migrations work exactly the same with in-memory databases
	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL:   `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`,
		DownSQL: `DROP TABLE users`,
	})

	ctx := context.Background()
	if err := q.Up(ctx); err != nil {
		log.Fatal(err)
	}

	fmt.Println("In-memory migrations applied!")
}

// Example_walMode demonstrates using WAL mode for better concurrency.
func Example_walMode() {
	// Enable WAL mode for better concurrent read/write performance
	db, _ := sql.Open("sqlite3", "myapp.db?_journal_mode=WAL")
	defer db.Close()

	driver := sqlite.New(db)
	q := queen.New(driver)
	defer q.Close()

	// Your migrations here
}

// Example_fullConnectionString demonstrates a production-ready connection string.
func Example_fullConnectionString() {
	// Recommended connection string for production
	dsn := "myapp.db?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=on&_synchronous=NORMAL"
	db, _ := sql.Open("sqlite3", dsn)
	defer db.Close()

	driver := sqlite.New(db)
	q := queen.New(driver)
	defer q.Close()

	// This configuration provides:
	// - WAL mode: better concurrency
	// - Busy timeout: 5 seconds to wait for locks
	// - Foreign keys: enabled
	// - Synchronous: balanced safety/performance
}

// Example_customTableName demonstrates using a custom table name for migrations.
func Example_customTableName() {
	db, _ := sql.Open("sqlite3", "myapp.db")
	defer db.Close()

	// Use custom table name
	driver := sqlite.NewWithTableName(db, "my_custom_migrations")
	q := queen.New(driver)
	defer q.Close()

	// The migrations will be tracked in "my_custom_migrations" table
	// instead of the default "queen_migrations"
}

// Example_goFunctionMigration demonstrates using Go functions for complex migrations.
func Example_goFunctionMigration() {
	db, _ := sql.Open("sqlite3", "myapp.db")
	defer db.Close()

	driver := sqlite.New(db)
	q := queen.New(driver)
	defer q.Close()

	// Migration using Go function for complex logic
	q.MustAdd(queen.M{
		Version:        "003",
		Name:           "normalize_emails",
		ManualChecksum: "v1", // Important: track function changes!
		UpFunc: func(ctx context.Context, tx *sql.Tx) error {
			// Fetch all users
			rows, err := tx.QueryContext(ctx, "SELECT id, email FROM users")
			if err != nil {
				return err
			}
			defer rows.Close()

			// Normalize each email
			for rows.Next() {
				var id int
				var email string
				if err := rows.Scan(&id, &email); err != nil {
					return err
				}

				// Convert to lowercase
				normalized := normalizeEmail(email)

				// Update the email
				_, err = tx.ExecContext(ctx,
					"UPDATE users SET email = ? WHERE id = ?",
					normalized, id)
				if err != nil {
					return err
				}
			}

			return rows.Err()
		},
		DownFunc: func(ctx context.Context, tx *sql.Tx) error {
			// Rollback is not possible for this migration
			return nil
		},
	})

	ctx := context.Background()
	if err := q.Up(ctx); err != nil {
		log.Fatal(err)
	}
}

// Example_foreignKeys demonstrates handling foreign keys properly.
func Example_foreignKeys() {
	// Enable foreign keys in connection string
	db, _ := sql.Open("sqlite3", "myapp.db?_foreign_keys=on")
	defer db.Close()

	driver := sqlite.New(db)
	q := queen.New(driver)
	defer q.Close()

	// First migration: create parent table
	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL: `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				email TEXT NOT NULL UNIQUE
			)
		`,
		DownSQL: `DROP TABLE users`,
	})

	// Second migration: create child table with foreign key
	q.MustAdd(queen.M{
		Version: "002",
		Name:    "create_posts",
		UpSQL: `
			CREATE TABLE posts (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				user_id INTEGER NOT NULL,
				title TEXT,
				FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
			)
		`,
		// Important: child table must be dropped first
		DownSQL: `DROP TABLE posts`,
	})

	ctx := context.Background()
	if err := q.Up(ctx); err != nil {
		log.Fatal(err)
	}

	// When rolling back, Queen will execute down migrations in reverse order:
	// 1. DROP TABLE posts (child)
	// 2. DROP TABLE users (parent)
	// This ensures foreign key constraints are satisfied
}

// Example_indexes demonstrates creating indexes for better query performance.
func Example_indexes() {
	db, _ := sql.Open("sqlite3", "myapp.db")
	defer db.Close()

	driver := sqlite.New(db)
	q := queen.New(driver)
	defer q.Close()

	// Create table
	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL: `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				email TEXT NOT NULL,
				name TEXT,
				created_at TEXT DEFAULT (datetime('now'))
			)
		`,
		DownSQL: `DROP TABLE users`,
	})

	// Add indexes in a separate migration
	q.MustAdd(queen.M{
		Version: "002",
		Name:    "add_user_indexes",
		UpSQL: `
			CREATE UNIQUE INDEX idx_users_email ON users(email);
			CREATE INDEX idx_users_created_at ON users(created_at);
		`,
		DownSQL: `
			DROP INDEX IF EXISTS idx_users_email;
			DROP INDEX IF EXISTS idx_users_created_at;
		`,
	})

	ctx := context.Background()
	if err := q.Up(ctx); err != nil {
		log.Fatal(err)
	}
}

// Example_status demonstrates checking migration status.
func Example_status() {
	db, _ := sql.Open("sqlite3", ":memory:")
	defer db.Close()

	driver := sqlite.New(db)
	q := queen.New(driver)
	defer q.Close()

	// Register migrations
	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL:   `CREATE TABLE users (id INTEGER PRIMARY KEY)`,
		DownSQL: `DROP TABLE users`,
	})

	q.MustAdd(queen.M{
		Version: "002",
		Name:    "create_posts",
		UpSQL:   `CREATE TABLE posts (id INTEGER PRIMARY KEY)`,
		DownSQL: `DROP TABLE posts`,
	})

	ctx := context.Background()

	// Apply first migration only
	if err := q.UpSteps(ctx, 1); err != nil {
		log.Fatal(err)
	}

	// Check status
	statuses, err := q.Status(ctx)
	if err != nil {
		log.Fatal(err)
	}

	for _, s := range statuses {
		fmt.Printf("%s: %s (%s)\n", s.Version, s.Name, s.Status)
	}

	// Output:
	// 001: create_users (applied)
	// 002: create_posts (pending)
}

// Example_withConfig demonstrates using custom configuration.
func Example_withConfig() {
	db, _ := sql.Open("sqlite3", "myapp.db")
	defer db.Close()

	driver := sqlite.New(db)

	// Create Queen with custom config
	config := &queen.Config{
		TableName:   "custom_migrations",
		LockTimeout: 5 * 60, // 5 minutes in seconds
	}
	q := queen.NewWithConfig(driver, config)
	defer q.Close()

	// Your migrations here
}

// Example_testing demonstrates best practices for testing migrations.
func Example_testing() {
	// Use in-memory database for fast, isolated tests
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	driver := sqlite.New(db)
	// Note: In actual tests, use queen.NewTest(t, driver)
	q := queen.New(driver)
	defer q.Close()

	// Register migrations
	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL:   `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`,
		DownSQL: `DROP TABLE users`,
	})

	ctx := context.Background()

	// Test up migration
	if err := q.Up(ctx); err != nil {
		log.Fatal(err)
	}

	// Verify table exists
	var tableName string
	err = db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='users'").Scan(&tableName)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Table created:", tableName)

	// Test down migration
	if err := q.Reset(ctx); err != nil {
		log.Fatal(err)
	}

	// Verify table is gone
	err = db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='users'").Scan(&tableName)
	if err != sql.ErrNoRows {
		log.Fatal("table should be gone")
	}
	fmt.Println("Table dropped successfully")

	// Output:
	// Table created: users
	// Table dropped successfully
}

// Helper function for email normalization
func normalizeEmail(email string) string {
	// Simple normalization (lowercase)
	// In real code, you might want more sophisticated logic
	return email // placeholder
}
