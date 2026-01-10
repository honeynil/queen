# Queen

**Database migrations for Go.**

Queen is a database migration library that lets you define migrations in code, not separate files. It supports both SQL and Go function migrations, with built-in testing helpers and a simple, idiomatic API.

[![Go Reference](https://pkg.go.dev/badge/github.com/honeynil/queen.svg)](https://pkg.go.dev/github.com/honeynil/queen)
[![Go Report Card](https://goreportcard.com/badge/github.com/honeynil/queen)](https://goreportcard.com/report/github.com/honeynil/queen)
[![GitHub release](https://img.shields.io/github/v/release/honeynil/queen)](https://github.com/honeynil/queen/releases)
[![License](https://img.shields.io/github/license/honeynil/queen)](LICENSE)


## Features

- **Migrations in code** - Define migrations as Go code, not separate `.sql` files
- **Flexible syntax** - Use SQL strings in code, Go functions, or mix both
- **Testing helpers** - Built-in support for testing your migrations
- **Natural sorting** - Smart version ordering: "1" < "2" < "10" < "100", "user_1" < "user_10"
- **Flexible versioning** - Use sequential numbers, prefixes, or any naming scheme
- **Type-safe** - Full Go type safety for programmatic migrations
- **PostgreSQL first** - Excellent Postgres support, more databases coming soon
- **Lock protection** - Prevents concurrent migration runs
- **Checksum validation** - Detects when applied migrations have changed

## Quick Start

### Installation

```bash
go get github.com/honeynil/queen
go get github.com/honeynil/queen/drivers/postgres
go get github.com/jackc/pgx/v5/stdlib
```

### Basic Usage

```go
package main

import (
    "context"
    "database/sql"
    "log"

    _ "github.com/jackc/pgx/v5/stdlib"

    "github.com/honeynil/queen"
    "github.com/honeynil/queen/drivers/postgres"
)

func main() {
    // Connect to database
    db, _ := sql.Open("pgx", "postgres://localhost/myapp?sslmode=disable")
    defer db.Close()

    // Create Queen instance
    driver := postgres.New(db)
    q := queen.New(driver)
    defer q.Close()

    // Register migrations
    q.MustAdd(queen.M{
        Version: "001",
        Name:    "create_users_table",
        UpSQL: `
            CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                email VARCHAR(255) NOT NULL UNIQUE,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        `,
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

    log.Println("Migrations applied successfully!")
}
```

## Usage Examples

### Modular Migrations (Registry Pattern)

For large projects, organize migrations by domain:

```go
// users/migrations.go
package users

func Register(q *queen.Queen) {
    q.MustAdd(queen.M{
        Version: "users_001",
        Name:    "create_users",
        UpSQL:   `CREATE TABLE users (...)`,
        DownSQL: `DROP TABLE users`,
    })
}

// posts/migrations.go
package posts

func Register(q *queen.Queen) {
    q.MustAdd(queen.M{
        Version: "posts_001",
        Name:    "create_posts",
        UpSQL:   `CREATE TABLE posts (...)`,
        DownSQL: `DROP TABLE posts`,
    })
}

// main.go
func main() {
    q := queen.New(driver)

    users.Register(q)
    posts.Register(q)

    q.Up(ctx)
}
```

### Go Function Migrations

For complex migrations that need programmatic logic:

```go
q.MustAdd(queen.M{
    Version:        "003",
    Name:           "normalize_emails",
    ManualChecksum: "v1", // Important: track function changes!
    UpFunc: func(ctx context.Context, tx *sql.Tx) error {
        // Fetch users
        rows, err := tx.QueryContext(ctx, "SELECT id, email FROM users")
        if err != nil {
            return err
        }
        defer rows.Close()

        // Process each user
        for rows.Next() {
            var id int
            var email string
            if err := rows.Scan(&id, &email); err != nil {
                return err
            }

            // Normalize email
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
    DownFunc: func(ctx context.Context, tx *sql.Tx) error {
        // Rollback logic (if possible)
        return nil
    },
})
```

### Testing Migrations

Queen makes it easy to test your migrations:

```go
func TestMigrations(t *testing.T) {
    // Setup test database
    db := setupTestDB(t)
    driver := postgres.New(db)

    // Create test instance (auto-cleanup on test end)
    q := queen.NewTest(t, driver)

    // Register migrations
    q.MustAdd(queen.M{
        Version: "001",
        Name:    "create_users",
        UpSQL:   `CREATE TABLE users (id INT)`,
        DownSQL: `DROP TABLE users`,
    })

    // Test both up and down migrations
    q.TestUpDown()
}
```

### Migration Operations

```go
// Apply all pending migrations
q.Up(ctx)

// Apply next N migrations
q.UpSteps(ctx, 3)

// Rollback last migration
q.Down(ctx, 1)

// Rollback last N migrations
q.Down(ctx, 3)

// Rollback all migrations
q.Reset(ctx)

// Get migration status
statuses, _ := q.Status(ctx)
for _, s := range statuses {
    fmt.Printf("%s: %s (%s)\n", s.Version, s.Name, s.Status)
}

// Validate migrations
if err := q.Validate(ctx); err != nil {
    log.Fatal(err)
}
```

## Philosophy

Queen follows the principle: **migrations are code, not files**. This approach enables:
- Type safety and IDE support
- Easier testing and refactoring
- No file organization overhead
- Full programmatic control when needed

Queen is designed for developers who want clean, testable migrations without the ceremony.

## Configuration

```go
config := &queen.Config{
    TableName:   "custom_migrations", // Default: "queen_migrations"
    LockTimeout: 30 * time.Minute,    // Default: 30 minutes
    SkipLock:    false,               // Default: false (recommended)
}

q := queen.NewWithConfig(driver, config)
```

## API Documentation

See [pkg.go.dev](https://pkg.go.dev/github.com/honeynil/queen) for complete API documentation.

### Key Types

#### Migration / M

```go
type Migration struct {
    Version        string        // Unique version identifier
    Name           string        // Human-readable name
    UpSQL          string        // SQL to apply migration
    DownSQL        string        // SQL to rollback migration
    UpFunc         MigrationFunc // Go function to apply
    DownFunc       MigrationFunc // Go function to rollback
    ManualChecksum string        // Manual checksum for Go functions
}

type M = Migration // Convenient alias
```

#### Queen

```go
type Queen struct { /* ... */ }

func New(driver Driver) *Queen
func NewWithConfig(driver Driver, config *Config) *Queen
func NewTest(t *testing.T, driver Driver) *TestHelper

func (q *Queen) Add(m M) error
func (q *Queen) MustAdd(m M)
func (q *Queen) Up(ctx context.Context) error
func (q *Queen) UpSteps(ctx context.Context, n int) error
func (q *Queen) Down(ctx context.Context, n int) error
func (q *Queen) Reset(ctx context.Context) error
func (q *Queen) Status(ctx context.Context) ([]MigrationStatus, error)
func (q *Queen) Validate(ctx context.Context) error
func (q *Queen) Close() error
```

## What's Next

Currently working on:
- **SQLite driver** - In-process database support
- **MySQL driver** - Broader database compatibility

## License

MIT License - see [LICENSE](LICENSE) for details.

## Author

Created by [honeynil](https://github.com/honeynil)
