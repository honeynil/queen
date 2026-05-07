package cli

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"

	"github.com/honeynil/queen"
	"github.com/spf13/cobra"
)

func TestAppGlobalFlags(t *testing.T) {
	app := &App{
		registerFunc: func(q *queen.Queen) {},
		config:       &Config{},
	}

	app.rootCmd = createTestRootCmd(app)
	app.addGlobalFlags()

	args := []string{
		"--driver", "postgres",
		"--dsn", "postgres://localhost/test",
		"--table", "custom_migrations",
		"--use-config",
		"--env", "production",
		"--unlock-production",
		"--yes",
		"--json",
		"--verbose",
	}

	app.rootCmd.SetArgs(args)
	if err := app.rootCmd.Execute(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if app.config.Driver != DriverPostgres {
		t.Errorf("driver = %q, want %q", app.config.Driver, DriverPostgres)
	}
	if app.config.DSN != "postgres://localhost/test" {
		t.Errorf("dsn = %q, want %q", app.config.DSN, "postgres://localhost/test")
	}
	if app.config.Table != "custom_migrations" {
		t.Errorf("table = %q, want %q", app.config.Table, "custom_migrations")
	}
	if !app.config.UseConfig {
		t.Error("use-config should be true")
	}
	if app.config.Env != "production" {
		t.Errorf("env = %q, want %q", app.config.Env, "production")
	}
	if !app.config.UnlockProduction {
		t.Error("unlock-production should be true")
	}
	if !app.config.Yes {
		t.Error("yes should be true")
	}
	if !app.config.JSON {
		t.Error("json should be true")
	}
	if !app.config.Verbose {
		t.Error("verbose should be true")
	}
}

func TestAppDefaultFlags(t *testing.T) {
	app := &App{
		registerFunc: func(q *queen.Queen) {},
		config:       &Config{},
	}

	app.rootCmd = createTestRootCmd(app)
	app.addGlobalFlags()

	app.rootCmd.SetArgs([]string{})
	if err := app.rootCmd.Execute(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if app.config.Table != "queen_migrations" {
		t.Errorf("default table = %q, want %q", app.config.Table, "queen_migrations")
	}
	if app.config.UseConfig {
		t.Error("default use-config should be false")
	}
	if app.config.Yes {
		t.Error("default yes should be false")
	}
	if app.config.JSON {
		t.Error("default json should be false")
	}
}

func TestRootCommandHelp(t *testing.T) {
	app := &App{
		registerFunc: func(q *queen.Queen) {},
		config:       &Config{},
	}

	app.rootCmd = createFullRootCmd()
	app.addGlobalFlags()
	app.addCommands()

	var out bytes.Buffer
	app.rootCmd.SetOut(&out)
	app.rootCmd.SetErr(&out)

	app.rootCmd.SetArgs([]string{"--help"})
	if err := app.rootCmd.Execute(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	output := out.String()
	checks := []string{
		"Queen migration CLI",
		"--driver",
		"--dsn",
		"create",
		"up",
		"down",
		"status",
	}

	for _, check := range checks {
		if !strings.Contains(output, check) {
			t.Errorf("help output missing %q", check)
		}
	}
}

func TestSubcommandHelp(t *testing.T) {
	subcommands := []struct {
		name   string
		checks []string
	}{
		{
			name:   "create",
			checks: []string{"Create a new migration", "--type"},
		},
		{
			name:   "up",
			checks: []string{"Apply pending migrations", "--steps"},
		},
		{
			name:   "down",
			checks: []string{"Rollback migrations", "--steps"},
		},
		{
			name:   "status",
			checks: []string{"status of all registered migrations"},
		},
		{
			name:   "validate",
			checks: []string{"Validate"},
		},
		{
			name:   "reset",
			checks: []string{"Rollback all migrations"},
		},
	}

	for _, sc := range subcommands {
		t.Run(sc.name, func(t *testing.T) {
			app := &App{
				registerFunc: func(q *queen.Queen) {},
				config:       &Config{},
			}
			app.rootCmd = createFullRootCmd()
			app.addGlobalFlags()
			app.addCommands()

			var out bytes.Buffer
			app.rootCmd.SetOut(&out)
			app.rootCmd.SetErr(&out)

			app.rootCmd.SetArgs([]string{sc.name, "--help"})
			if err := app.rootCmd.Execute(); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			output := out.String()
			for _, check := range sc.checks {
				if !strings.Contains(output, check) {
					t.Errorf("%s help missing %q\nGot output:\n%s", sc.name, check, output)
				}
			}
		})
	}
}

func TestLoadConfigPriority(t *testing.T) {
	tempDir := t.TempDir()

	oldWd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chdir(oldWd) }()

	oldDriver := os.Getenv("QUEEN_DRIVER")
	oldDSN := os.Getenv("QUEEN_DSN")
	defer func() {
		_ = os.Setenv("QUEEN_DRIVER", oldDriver)
		_ = os.Setenv("QUEEN_DSN", oldDSN)
	}()

	if err := os.Chdir(tempDir); err != nil {
		t.Fatal(err)
	}

	configYAML := `config_locked: false
development:
  driver: sqlite
  dsn: file:config.db
`
	if err := os.WriteFile(".queen.yaml", []byte(configYAML), 0644); err != nil {
		t.Fatal(err)
	}

	_ = os.Setenv("QUEEN_DRIVER", "mysql")
	_ = os.Setenv("QUEEN_DSN", "mysql://env/db")

	t.Run("flags win over env and config", func(t *testing.T) {
		app := &App{
			config: &Config{
				Driver:    "postgres",
				DSN:       "postgres://flag/db",
				Table:     "queen_migrations",
				UseConfig: true,
				Env:       "development",
			},
		}

		if err := app.loadConfig(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if app.config.Driver != DriverPostgres {
			t.Errorf("driver = %q, want %q (flag should win)", app.config.Driver, DriverPostgres)
		}
		if app.config.DSN != "postgres://flag/db" {
			t.Errorf("dsn = %q, want %q (flag should win)", app.config.DSN, "postgres://flag/db")
		}
	})

	t.Run("env wins over config", func(t *testing.T) {
		app := &App{
			config: &Config{
				Table:     "queen_migrations",
				UseConfig: true,
				Env:       "development",
			},
		}

		if err := app.loadConfig(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if app.config.Driver != "sqlite" {
			t.Errorf("driver = %q, want %q (config loaded first)", app.config.Driver, "sqlite")
		}
	})
}

func createTestRootCmd(_ *App) *cobra.Command {
	return &cobra.Command{
		Use:           "queen",
		SilenceUsage:  true,
		SilenceErrors: true,
		Run:           func(cmd *cobra.Command, args []string) {},
	}
}

func createFullRootCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "queen",
		Short: "Queen migration CLI",
		Long: `Queen migration CLI - Database migrations for Go.

Configuration priority:
  1. Command-line flags (highest)
  2. Environment variables (QUEEN_DRIVER, QUEEN_DSN, etc.)
  3. Config file .queen.yaml (lowest, requires --use-config)`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
}

func TestSetupQueen(t *testing.T) {
	t.Parallel()

	t.Run("returns error when driver is missing", func(t *testing.T) {
		t.Parallel()

		app := &App{
			config:       &Config{DSN: "test"},
			registerFunc: func(q *queen.Queen) {},
		}

		ctx := context.Background()
		_, err := app.setupQueen(ctx)

		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "driver is required") {
			t.Errorf("error = %q, want 'driver is required'", err.Error())
		}
	})

	t.Run("returns error when dsn is missing", func(t *testing.T) {
		t.Parallel()

		app := &App{
			config:       &Config{Driver: "postgres"},
			registerFunc: func(q *queen.Queen) {},
		}

		ctx := context.Background()
		_, err := app.setupQueen(ctx)

		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "dsn is required") {
			t.Errorf("error = %q, want 'dsn is required'", err.Error())
		}
	})
}

func TestAddCommands(t *testing.T) {
	t.Parallel()

	app := &App{
		registerFunc: func(q *queen.Queen) {},
		config:       &Config{},
	}

	app.rootCmd = createTestRootCmd(app)
	app.addCommands()

	expectedCommands := []string{
		"create", "up", "down", "reset", "status", "validate",
		"version", "plan", "explain", "log", "goto", "gap",
		"diff", "doctor", "check", "init", "squash", "baseline",
		"import", "tui",
	}

	for _, cmdName := range expectedCommands {
		found := false
		for _, cmd := range app.rootCmd.Commands() {
			if cmd.Name() == cmdName {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("command %q not added", cmdName)
		}
	}
}
