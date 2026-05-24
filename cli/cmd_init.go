package cli

import (
	"fmt"
	"os"
	"path/filepath"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/cobra"
	"github.com/yaop-labs/queen/cli/tui"
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
				driver = DriverPostgres
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
	fmt.Println("  1. Update your database connection in .queen.yaml")
	fmt.Println("  2. Run go mod tidy")
	fmt.Println("  3. Build the migration CLI: go build -o migrate ./cmd/migrate")
	fmt.Println("  4. Run migrations: ./migrate up")
	fmt.Println()
	fmt.Println("For more information, visit: https://github.com/yaop-labs/queen")

	return nil
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
