package cli

import (
	"fmt"
	"os"
	"path/filepath"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/honeynil/queen/cli/tui"
	"github.com/spf13/cobra"
)

func (app *App) initCmd() *cobra.Command {
	var (
		driver        string
		withConfig    bool
		interactive   bool
		migrationsDir string
	)

	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize Queen in your project",
		RunE: func(cmd *cobra.Command, args []string) error {
			if interactive {
				return runInteractiveInit()
			}

			if driver == "" {
				driver = DriverPostgres // Default driver
			}

			return initializeProject(driver, withConfig, migrationsDir)
		},
	}

	cmd.Flags().StringVar(&driver, "driver", "", "Database driver (postgres, mysql, sqlite, clickhouse, cockroachdb, mssql)")
	cmd.Flags().BoolVar(&withConfig, "with-config", false, "Create .queen.yaml configuration file")
	cmd.Flags().BoolVar(&interactive, "interactive", false, "Interactive setup wizard")
	cmd.Flags().StringVar(&migrationsDir, "migrations-dir", "migrations", "Migrations directory name")

	return cmd
}

func initializeProject(driver string, withConfig bool, migrationsDir string) error {
	fmt.Println("Initializing Queen migration setup...")
	fmt.Println()

	if err := os.MkdirAll(migrationsDir, 0755); err != nil {
		return fmt.Errorf("failed to create migrations directory: %w", err)
	}
	fmt.Printf("Created directory: %s/\n", migrationsDir)

	migrationsFile := filepath.Join(migrationsDir, "migrations.go")
	if err := createMigrationsFile(migrationsFile, driver); err != nil {
		return fmt.Errorf("failed to create migrations.go: %w", err)
	}
	fmt.Printf("Created file: %s\n", migrationsFile)

	exampleFile := filepath.Join(migrationsDir, "001_initial_schema.go")
	if err := createExampleMigration(exampleFile); err != nil {
		return fmt.Errorf("failed to create example migration: %w", err)
	}
	fmt.Printf("Created file: %s\n", exampleFile)

	cmdDir := filepath.Join("cmd", "migrate")
	if err := os.MkdirAll(cmdDir, 0755); err != nil {
		return fmt.Errorf("failed to create cmd directory: %w", err)
	}
	mainFile := filepath.Join(cmdDir, "main.go")
	if err := createMainFile(mainFile, migrationsDir); err != nil {
		return fmt.Errorf("failed to create main.go: %w", err)
	}
	fmt.Printf("Created file: %s\n", mainFile)

	if withConfig {
		if err := createConfigFile(driver); err != nil {
			return fmt.Errorf("failed to create .queen.yaml: %w", err)
		}
		fmt.Println("Created file: .queen.yaml")
	}

	fmt.Println()
	fmt.Println("Initialization complete!")
	fmt.Println()
	fmt.Println("Next steps:")
	fmt.Println("  1. Update your database connection in cmd/migrate/main.go")
	fmt.Println("  2. Build the migration CLI: go build -o migrate cmd/migrate/main.go")
	fmt.Println("  3. Run migrations: ./migrate up")
	fmt.Println()
	fmt.Println("For more information, visit: https://github.com/honeynil/queen")

	return nil
}

func createMigrationsFile(path string, _ string) error {
	content := `package migrations

import (
	"github.com/honeynil/queen"
)

// Register registers all migrations with Queen.
func Register(q *queen.Queen) {
	// Register your migrations here
	// Example:
	// q.MustAdd(queen.M{
	//     Version: "001",
	//     Name:    "initial_schema",
	//     UpSQL:   "CREATE TABLE users (id SERIAL PRIMARY KEY, email VARCHAR(255))",
	//     DownSQL: "DROP TABLE users",
	// })

	// See 001_initial_schema.go for an example
	Register001InitialSchema(q)
}
`

	return os.WriteFile(path, []byte(content), 0644)
}

func createExampleMigration(path string) error {
	content := `package migrations

import (
	"github.com/honeynil/queen"
)

// Register001InitialSchema registers the initial schema migration.
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

	return os.WriteFile(path, []byte(content), 0644)
}

func createMainFile(path string, migrationsDir string) error {
	content := fmt.Sprintf(`package main

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/honeynil/queen/cli"

	// Import your migrations package
	"yourmodule/%s"

	// Import database driver
	_ "github.com/lib/pq" // PostgreSQL
	// _ "github.com/go-sql-driver/mysql" // MySQL
	// _ "github.com/mattn/go-sqlite3" // SQLite
	// _ "github.com/ClickHouse/clickhouse-go/v2" // ClickHouse
	// _ "github.com/jackc/pgx/v5/stdlib" // CockroachDB
	// _ "github.com/microsoft/go-mssqldb" // MSSQL
)

func main() {
	// Run CLI with migration registration
	cli.Run(migrations.Register)

	// Or with custom DB opener:
	// cli.RunWithDB(migrations.Register, func(dsn string) (*sql.DB, error) {
	//     return sql.Open("postgres", dsn)
	// })
}
`, migrationsDir)

	return os.WriteFile(path, []byte(content), 0644)
}

func createConfigFile(driver string) error {
	var dsnExample string
	switch driver {
	case DriverPostgres:
		dsnExample = "postgres://user:pass@localhost:5432/dbname?sslmode=disable"
	case DriverMySQL:
		dsnExample = "user:pass@tcp(localhost:3306)/dbname?parseTime=true"
	case DriverSQLite:
		dsnExample = "./app.db?_journal_mode=WAL"
	case DriverClickHouse:
		dsnExample = "tcp://localhost:9000/dbname"
	case DriverCockroach:
		dsnExample = "postgres://user:pass@localhost:26257/dbname?sslmode=disable"
	case DriverMSSQL:
		dsnExample = "sqlserver://user:pass@localhost:1433?database=dbname"
	default:
		dsnExample = "postgres://localhost/mydb"
	}

	content := fmt.Sprintf(`# Queen Migration Configuration

# Default environment (if --env is not specified)
default: development

# Environment configurations
environments:
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
    # Requires --unlock-production flag to run migrations
    locked: true
`, driver, dsnExample, driver, driver)

	return os.WriteFile(".queen.yaml", []byte(content), 0644)
}

func runInteractiveInit() error {
	model := tui.NewInitModel()

	p := tea.NewProgram(
		model,
		tea.WithAltScreen(),
	)

	if _, err := p.Run(); err != nil {
		return fmt.Errorf("failed to start interactive setup: %w", err)
	}

	result := model.Result()
	if result == nil || !result.Confirmed {
		fmt.Println("Setup cancelled.")
		return nil
	}

	return initializeProject(result.Driver, result.WithConfig, result.MigrationsDir)
}
