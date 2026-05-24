package cli

import (
	"errors"
	"fmt"
	"os"
)

func writeFileExclusive(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("file already exists: %s", path)
		}
		return err
	}
	defer func() { _ = file.Close() }()

	if _, err := file.Write(data); err != nil {
		return err
	}
	return nil
}

func createMigrationsFile(path string, _ string) error {
	content := `package migrations

import (
	"github.com/yaop-labs/queen"
)

// Register wires this project's migrations into Queen.
func Register(q *queen.Queen) {
	// Add your migrations here. For example:
	// q.MustAdd(queen.M{
	//     Version: "001",
	//     Name:    "initial_schema",
	//     UpSQL:   "CREATE TABLE users (id SERIAL PRIMARY KEY, email VARCHAR(255))",
	//     DownSQL: "DROP TABLE users",
	// })
	Register001InitialSchema(q)
	}
	`

	return writeFileExclusive(path, []byte(content))
}

func createExampleMigration(path string) error {
	content := `package migrations

import (
	"github.com/yaop-labs/queen"
)

// Register001InitialSchema adds the first schema migration.
func Register001InitialSchema(q *queen.Queen) {
	q.MustAdd(queen.M{
		Version: "001",
		Name:    "initial_schema",
		UpSQL: ` + "`" + `
			CREATE TABLE users (
				id SERIAL PRIMARY KEY,
				email VARCHAR(255) NOT NULL UNIQUE,
				name VARCHAR(255),
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			);
		` + "`" + `,
		DownSQL: ` + "`" + `
			DROP TABLE users;
		` + "`" + `,
	})
	}
	`

	return writeFileExclusive(path, []byte(content))
}

func createMainFile(path string, migrationsDir string) error {
	content := fmt.Sprintf(`package main

import (
	"github.com/yaop-labs/queen/cli"

	"yourmodule/%s"

	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL
	// _ "github.com/go-sql-driver/mysql" // MySQL
	// _ "github.com/mattn/go-sqlite3" // SQLite
	// _ "github.com/ClickHouse/clickhouse-go/v2" // ClickHouse
	// CockroachDB also uses github.com/jackc/pgx/v5/stdlib
	// _ "github.com/microsoft/go-mssqldb" // MSSQL
)

func main() {
	cli.Run(migrations.Register)
}
`, migrationsDir)

	return writeFileExclusive(path, []byte(content))
}

func createConfigFile(driver string) error {
	content := fmt.Sprintf(`# Queen Migration Configuration

config_locked: false

naming:
  pattern: sequential-padded
  padding: 3
  enforce: true

# Environment configurations
development:
  driver: %s
  dsn: "%s"
  table: queen_migrations
  lock_timeout: 5m

staging:
  driver: %s
  dsn: "${DATABASE_URL}"
  table: queen_migrations
  lock_timeout: 10m

production:
  driver: %s
  dsn: "${DATABASE_URL}"
  table: queen_migrations
  lock_timeout: 30m
  require_confirmation: true
  require_explicit_unlock: true
`, driver, initDSNExample(driver), driver, driver)

	return writeFileExclusive(".queen.yaml", []byte(content))
}

func initDSNExample(driver string) string {
	switch driver {
	case DriverPostgres:
		return "postgres://user:pass@localhost:5432/dbname?sslmode=disable"
	case DriverMySQL:
		return "user:pass@tcp(localhost:3306)/dbname?parseTime=true"
	case DriverSQLite:
		return "./app.db?_journal_mode=WAL"
	case DriverClickHouse:
		return "tcp://localhost:9000/dbname"
	case DriverCockroach:
		return "postgres://user:pass@localhost:26257/dbname?sslmode=disable"
	case DriverMSSQL:
		return "sqlserver://user:pass@localhost:1433?database=dbname"
	default:
		return "postgres://localhost/mydb"
	}
}
