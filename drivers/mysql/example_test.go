package mysql_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"

	"github.com/honeynil/queen"
	"github.com/honeynil/queen/drivers/mysql"
)

// Example demonstrates basic usage of the MySQL driver.
func Example() {
	// Connect to MySQL
	// IMPORTANT: parseTime=true is required for proper TIMESTAMP handling
	db, err := sql.Open("mysql", "user:password@tcp(localhost:3306)/myapp?parseTime=true")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create MySQL driver
	driver := mysql.New(db)

	// Create Queen instance
	q := queen.New(driver)
	defer q.Close()

	// Register migrations
	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users_table",
		UpSQL: `
			CREATE TABLE users (
				id INT AUTO_INCREMENT PRIMARY KEY,
				email VARCHAR(255) NOT NULL UNIQUE,
				name VARCHAR(255),
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				INDEX idx_email (email)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
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

// Example_customTableName demonstrates using a custom table name for migrations.
func Example_customTableName() {
	db, _ := sql.Open("mysql", "user:password@tcp(localhost:3306)/myapp?parseTime=true")
	defer db.Close()

	// Use custom table name
	driver := mysql.NewWithTableName(db, "my_custom_migrations")
	q := queen.New(driver)
	defer q.Close()

	// The migrations will be tracked in "my_custom_migrations" table
	// instead of the default "queen_migrations"
}

// Example_goFunctionMigration demonstrates using Go functions for complex migrations.
func Example_goFunctionMigration() {
	db, _ := sql.Open("mysql", "user:password@tcp(localhost:3306)/myapp?parseTime=true")
	defer db.Close()

	driver := mysql.New(db)
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

// Example_withConfig demonstrates using custom configuration.
func Example_withConfig() {
	db, _ := sql.Open("mysql", "user:password@tcp(localhost:3306)/myapp?parseTime=true")
	defer db.Close()

	driver := mysql.New(db)

	// Create Queen with custom config
	config := &queen.Config{
		TableName:   "custom_migrations",
		LockTimeout: 10 * 60, // 10 minutes in seconds
	}
	q := queen.NewWithConfig(driver, config)
	defer q.Close()

	// Your migrations here
}

// Example_foreignKeys demonstrates handling foreign keys properly.
func Example_foreignKeys() {
	db, _ := sql.Open("mysql", "user:password@tcp(localhost:3306)/myapp?parseTime=true")
	defer db.Close()

	driver := mysql.New(db)
	q := queen.New(driver)
	defer q.Close()

	// First migration: create parent table
	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL: `
			CREATE TABLE users (
				id INT AUTO_INCREMENT PRIMARY KEY,
				email VARCHAR(255) NOT NULL UNIQUE
			) ENGINE=InnoDB
		`,
		DownSQL: `DROP TABLE users`,
	})

	// Second migration: create child table with foreign key
	q.MustAdd(queen.M{
		Version: "002",
		Name:    "create_posts",
		UpSQL: `
			CREATE TABLE posts (
				id INT AUTO_INCREMENT PRIMARY KEY,
				user_id INT NOT NULL,
				title VARCHAR(255),
				FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
				INDEX idx_user_id (user_id)
			) ENGINE=InnoDB
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
// Note: This example requires a running MySQL server.
// It will be skipped in CI if MySQL is not available.
func Example_status() {
	db, err := sql.Open("mysql", "user:password@tcp(localhost:3306)/myapp?parseTime=true")
	if err != nil {
		fmt.Println("MySQL not available")
		return
	}
	defer db.Close()

	// Check if MySQL is actually available
	if err := db.Ping(); err != nil {
		fmt.Println("MySQL not available")
		return
	}

	driver := mysql.New(db)
	q := queen.New(driver)
	defer q.Close()

	// Register migrations
	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL:   `CREATE TABLE users (id INT) ENGINE=InnoDB`,
		DownSQL: `DROP TABLE users`,
	})

	q.MustAdd(queen.M{
		Version: "002",
		Name:    "create_posts",
		UpSQL:   `CREATE TABLE posts (id INT) ENGINE=InnoDB`,
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

	// Example output (when MySQL is available):
	// 001: create_users (applied)
	// 002: create_posts (pending)
}

// Helper function for email normalization
func normalizeEmail(email string) string {
	// Simple normalization (lowercase)
	// In real code, you might want more sophisticated logic
	return email // placeholder
}
