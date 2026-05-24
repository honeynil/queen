package clickhouse_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2" // modern driver (recommended in 2026)

	"github.com/yaop-labs/queen"
	"github.com/yaop-labs/queen/drivers/clickhouse"
)

// Example demonstrates basic usage of the ClickHouse driver.
func Example() {
	// Connect to ClickHouse
	db, err := sql.Open("clickhouse", "clickhouse://default:password@localhost:9000/default?")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Create ClickHouse driver
	driver, err := clickhouse.New(db)
	if err != nil {
		log.Fatal(err)
	}

	// Create Queen instance
	q := queen.New(driver)
	defer func() { _ = q.Close() }()

	// Register migrations
	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users_table",
		UpSQL: `
			CREATE TABLE users(
				id          UUID DEFAULT generateUUIDv4(), 
				email       String                                 NOT NULL,  -- UNIQUE не поддерживается
				name        String,
				created_at  DateTime DEFAULT now()
			)
			ENGINE = ReplacingMergeTree()
			ORDER BY (id)           
		`,
		DownSQL: `DROP TABLE users`,
	})

	q.MustAdd(queen.M{
		Version: "002",
		Name:    "add_users_bio",
		UpSQL:   `ALTER TABLE users ADD COLUMN bio String`,
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
	db, err := sql.Open("clickhouse", "clickhouse://default:password@localhost:9000/default?")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Use custom table name
	driver, err := clickhouse.NewWithTableName(db, "my_custom_migrations")
	if err != nil {
		log.Fatal(err)
	}
	q := queen.New(driver)
	defer func() { _ = q.Close() }()

	// The migrations will be tracked in "my_custom_migrations" table
	// instead of the default "queen_migrations"
}

// Example_goFunctionMigration demonstrates using Go functions for complex migrations.
func Example_goFunctionMigration() {
	db, err := sql.Open("clickhouse", "clickhouse://default:password@localhost:9000/default?")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	driver, err := clickhouse.New(db)
	if err != nil {
		log.Fatal(err)
	}
	q := queen.New(driver)
	defer func() { _ = q.Close() }()

	q.MustAdd(queen.M{
		Version: "003",
		Name:    "add_users",
		UpSQL: `INSERT INTO users (email, name)
				  VALUES
					('alice@example.com',    'Alice Smith'),
					('bob@example.com',      'Bob Johnson'),
					('carol@example.com',    'Carol Williams'),
					('david@example.com',    'David Brown'),
					('eve@example.com',      'Eve Davis'),
					('frank@example.com',    'Frank Miller'),
					('grace@example.com',    'Grace Wilson'),
					('henry@example.com',    'Henry Moore'),
					('isabella@example.com', 'Isabella Taylor'),
					('jack@example.com',     'Jack Anderson');`,
		DownSQL: `DELETE FROM users
					WHERE email IN (
						'alice@example.com',
						'bob@example.com',
						'carol@example.com',
						'david@example.com',
						'eve@example.com',
						'frank@example.com',
						'grace@example.com',
						'henry@example.com',
						'isabella@example.com',
						'jack@example.com'
					);`,
	})

	q.MustAdd(queen.M{
		Version: "004",
		Name:    "modify_setting",
		UpSQL: `ALTER TABLE users
					MODIFY SETTING
						enable_block_number_column = 1,
						enable_block_offset_column = 1;`,
		DownSQL: `ALTER TABLE users
					MODIFY SETTING
						enable_block_number_column = 0,
						enable_block_offset_column = 0;`,
	})

	// Migration using Go function for complex logic
	q.MustAdd(queen.M{
		Version:        "005",
		Name:           "normalize_names",
		ManualChecksum: "v1", // Important: track function changes!
		UpFunc: func(ctx context.Context, tx *sql.Tx) error {
			// Fetch all users
			rows, err := tx.QueryContext(ctx, "SELECT id, name FROM users")
			if err != nil {
				return err
			}
			defer func() { _ = rows.Close() }()

			// Normalize each email
			for rows.Next() {
				var id string
				var name string
				if err := rows.Scan(&id, &name); err != nil {
					return err
				}

				// Convert to lowercase
				normalized := normalizeNames(name)

				// Update the email
				_, err = tx.ExecContext(ctx,
					"UPDATE users SET name = ? WHERE id = ?",
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
	db, err := sql.Open("clickhouse", "clickhouse://default:password@localhost:9000/default?")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	driver, err := clickhouse.New(db)
	if err != nil {
		log.Fatal(err)
	}

	// Create Queen with custom config
	config := &queen.Config{
		TableName:   "custom_migrations",
		LockTimeout: 10 * time.Minute, // 10 minutes
	}
	q := queen.NewWithConfig(driver, config)
	defer func() { _ = q.Close() }()

	// Your migrations here
}

// Example_status demonstrates checking migration status.
//
// Note: This example requires a running ClickHouse server.
// It will be skipped in CI if MySQL is not available.
func Example_status() {
	db, err := sql.Open("clickhouse", "clickhouse://default:password@localhost:9000/default?")
	if err != nil {
		fmt.Println("ClickHouse not available")
		return
	}
	defer func() { _ = db.Close() }()

	// Check if ClickHouse is actually available
	if err := db.Ping(); err != nil {
		fmt.Println("ClickHouse not available")
		return
	}

	driver, err := clickhouse.New(db)
	if err != nil {
		log.Fatal(err)
	}
	q := queen.New(driver)
	defer func() { _ = q.Close() }()

	// Register migrations
	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL: `CREATE TABLE users(
				id          UUID DEFAULT generateUUIDv4(),
				name        String
			)
			ENGINE = ReplacingMergeTree()
			ORDER BY (id)`,
		DownSQL: `DROP TABLE users`,
	})

	q.MustAdd(queen.M{
		Version: "002",
		Name:    "add_users_bio",
		UpSQL:   `ALTER TABLE users ADD COLUMN bio String`,
		DownSQL: `ALTER TABLE users DROP COLUMN bio`,
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

	// Example output (when ClickHouse is available):
	// 001: create_users (applied)
	// 002: create_posts (pending)
}

// Helper function for name normalization
func normalizeNames(name string) string {
	// Simple normalization (lowercase)
	// In real code, you might want more sophisticated logic
	return strings.ToUpper(name) // placeholder
}
