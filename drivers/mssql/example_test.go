package mssql_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/microsoft/go-mssqldb"

	"github.com/yaop-labs/queen"
	"github.com/yaop-labs/queen/drivers/mssql"
)

// Example demonstrates basic usage of the MS SQL Server driver.
func Example() {
	// Connect to SQL Server
	db, err := sql.Open("sqlserver", "sqlserver://user:password@localhost:1433?database=myapp")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Create MS SQL Server driver
	driver := mssql.New(db)

	// Create Queen instance
	q := queen.New(driver)
	defer func() { _ = q.Close() }()

	// Register migrations
	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users_table",
		UpSQL: `
			CREATE TABLE users (
				id INT IDENTITY(1,1) PRIMARY KEY,
				email NVARCHAR(255) NOT NULL UNIQUE,
				name NVARCHAR(255),
				created_at DATETIME2 DEFAULT GETUTCDATE()
			)
		`,
		DownSQL: `DROP TABLE users`,
	})

	q.MustAdd(queen.M{
		Version: "002",
		Name:    "add_users_bio",
		UpSQL:   `ALTER TABLE users ADD bio NVARCHAR(MAX)`,
		DownSQL: `ALTER TABLE users DROP COLUMN bio`,
	})

	// Apply all pending migrations
	ctx := context.Background()
	if err := q.Up(ctx); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Migrations applied successfully!")
}

// Example_customTableName demonstrates using a custom table name for migrations.
func Example_customTableName() {
	db, _ := sql.Open("sqlserver", "sqlserver://user:password@localhost:1433?database=myapp")
	defer func() { _ = db.Close() }()

	// Use custom table name
	driver := mssql.NewWithTableName(db, "my_custom_migrations")
	q := queen.New(driver)
	defer func() { _ = q.Close() }()

	// The migrations will be tracked in "my_custom_migrations" table
	// instead of the default "queen_migrations"
}

// Example_goFunctionMigration demonstrates using Go functions for complex migrations.
func Example_goFunctionMigration() {
	db, _ := sql.Open("sqlserver", "sqlserver://user:password@localhost:1433?database=myapp")
	defer func() { _ = db.Close() }()

	driver := mssql.New(db)
	q := queen.New(driver)
	defer func() { _ = q.Close() }()

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
			defer func() { _ = rows.Close() }()

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

// Example_withConfig demonstrates using custom configuration.
func Example_withConfig() {
	db, _ := sql.Open("sqlserver", "sqlserver://user:password@localhost:1433?database=myapp")
	defer func() { _ = db.Close() }()

	driver := mssql.New(db)

	// Create Queen with custom config
	config := &queen.Config{
		TableName:   "custom_migrations",
		LockTimeout: 10 * 60, // 10 minutes in seconds
	}
	q := queen.NewWithConfig(driver, config)
	defer func() { _ = q.Close() }()

	// Your migrations here
}

// Example_foreignKeys demonstrates handling foreign keys properly.
func Example_foreignKeys() {
	db, _ := sql.Open("sqlserver", "sqlserver://user:password@localhost:1433?database=myapp")
	defer func() { _ = db.Close() }()

	driver := mssql.New(db)
	q := queen.New(driver)
	defer func() { _ = q.Close() }()

	// First migration: create parent table
	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL: `
			CREATE TABLE users (
				id INT IDENTITY(1,1) PRIMARY KEY,
				email NVARCHAR(255) NOT NULL UNIQUE
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
				id INT IDENTITY(1,1) PRIMARY KEY,
				user_id INT NOT NULL,
				title NVARCHAR(255),
				FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
			);
			CREATE INDEX idx_posts_user_id ON posts(user_id)
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

// Example_status demonstrates checking migration status.
//
// Note: This example requires a running SQL Server instance.
// It will be skipped in CI if SQL Server is not available.
func Example_status() {
	db, err := sql.Open("sqlserver", "sqlserver://user:password@localhost:1433?database=myapp")
	if err != nil {
		fmt.Println("SQL Server not available")
		return
	}
	defer func() { _ = db.Close() }()

	// Check if SQL Server is actually available
	if err := db.Ping(); err != nil {
		fmt.Println("SQL Server not available")
		return
	}

	driver := mssql.New(db)
	q := queen.New(driver)
	defer func() { _ = q.Close() }()

	// Register migrations
	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL:   `CREATE TABLE users (id INT)`,
		DownSQL: `DROP TABLE users`,
	})

	q.MustAdd(queen.M{
		Version: "002",
		Name:    "create_posts",
		UpSQL:   `CREATE TABLE posts (id INT)`,
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

	// Example output (when SQL Server is available):
	// 001: create_users (applied)
	// 002: create_posts (pending)
}

// Helper function for email normalization
func normalizeEmail(email string) string {
	// Simple normalization (lowercase)
	// In real code, you might want more sophisticated logic
	return email // placeholder
}
