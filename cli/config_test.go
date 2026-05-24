package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testLoadConfigFile(t *testing.T, name, configYAML, env string, wantErr bool, errContains, wantDriver, wantDSN, wantTable string) {
	tempDir := t.TempDir()
	oldWd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chdir(oldWd) }()

	testDir := filepath.Join(tempDir, name)
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(testDir); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(".queen.yaml", []byte(configYAML), 0644); err != nil {
		t.Fatal(err)
	}

	app := &App{
		config: &Config{
			UseConfig: true,
			Env:       env,
			Table:     "queen_migrations",
		},
	}

	if err := app.loadConfigFile(); err != nil {
		if wantErr && (errContains == "" || strings.Contains(err.Error(), errContains)) {
			return
		}
		t.Fatalf("loadConfigFile error mismatch: got %v, want contains %q", err, errContains)
	}

	// Simple field checks
	if wantDriver != "" && app.config.Driver != wantDriver {
		t.Errorf("driver = %q, want %q", app.config.Driver, wantDriver)
	}
	if wantDSN != "" && app.config.DSN != wantDSN {
		t.Errorf("dsn = %q, want %q", app.config.DSN, wantDSN)
	}
	if wantTable != "" && app.config.Table != wantTable {
		t.Errorf("table = %q, want %q", app.config.Table, wantTable)
	}
}

func TestLoadConfigFile(t *testing.T) {
	testLoadConfigFile(t, "locked config",
		`config_locked: true
development:
  driver: postgres
  dsn: postgres://localhost/dev
`, "", true, "config file is locked", "", "", "")

	testLoadConfigFile(t, "unlocked config with environment",
		`config_locked: false
development:
  driver: postgres
  dsn: postgres://localhost/dev
  table: custom_migrations
`, "development", false, "", "postgres", "postgres://localhost/dev", "custom_migrations")

	testLoadConfigFile(t, "missing environment",
		`config_locked: false
development:
  driver: postgres
  dsn: postgres://localhost/dev
`, "production", true, "environment 'production' not found", "", "", "")

	testLoadConfigFile(t, "environment requires unlock",
		`config_locked: false
production:
  driver: postgres
  dsn: postgres://localhost/prod
  require_explicit_unlock: true
`, "production", true, "requires --unlock-production", "", "", "")
}

func TestLoadEnv(t *testing.T) {
	// Save and restore environment
	oldDriver := os.Getenv("QUEEN_DRIVER")
	oldDSN := os.Getenv("QUEEN_DSN")
	oldTable := os.Getenv("QUEEN_TABLE")
	defer func() {
		_ = os.Setenv("QUEEN_DRIVER", oldDriver)
		_ = os.Setenv("QUEEN_DSN", oldDSN)
		_ = os.Setenv("QUEEN_TABLE", oldTable)
	}()

	t.Run("loads from environment", func(t *testing.T) {
		_ = os.Setenv("QUEEN_DRIVER", "postgres")
		_ = os.Setenv("QUEEN_DSN", "postgres://localhost/test")
		_ = os.Setenv("QUEEN_TABLE", "custom_table")

		app := &App{
			config: &Config{
				Table: "queen_migrations", // default
			},
		}

		app.loadEnv()

		if app.config.Driver != "postgres" {
			t.Errorf("driver = %q, want %q", app.config.Driver, "postgres")
		}
		if app.config.DSN != "postgres://localhost/test" {
			t.Errorf("dsn = %q, want %q", app.config.DSN, "postgres://localhost/test")
		}
		if app.config.Table != "custom_table" {
			t.Errorf("table = %q, want %q", app.config.Table, "custom_table")
		}
	})

	t.Run("flags override env", func(t *testing.T) {
		_ = os.Setenv("QUEEN_DRIVER", "postgres")
		_ = os.Setenv("QUEEN_DSN", "postgres://localhost/test")

		app := &App{
			config: &Config{
				Driver: "mysql",                  // set by flag
				DSN:    "mysql://localhost/test", // set by flag
				Table:  "queen_migrations",       // default
			},
		}

		app.loadEnv()

		// Flags should not be overwritten
		if app.config.Driver != "mysql" {
			t.Errorf("driver = %q, want %q (flag should win)", app.config.Driver, "mysql")
		}
		if app.config.DSN != "mysql://localhost/test" {
			t.Errorf("dsn = %q, want %q (flag should win)", app.config.DSN, "mysql://localhost/test")
		}
	})
}

func TestRequiresConfirmation(t *testing.T) {
	tests := []struct {
		name   string
		config *Config
		want   bool
	}{
		{
			name: "yes flag skips confirmation",
			config: &Config{
				Yes: true,
				Env: "production",
				configFile: &ConfigFile{
					Environments: map[string]*Environment{
						"production": {RequireConfirmation: true},
					},
				},
			},
			want: false,
		},
		{
			name: "no config file",
			config: &Config{
				Env: "production",
			},
			want: false,
		},
		{
			name: "environment requires confirmation",
			config: &Config{
				Env: "staging",
				configFile: &ConfigFile{
					Environments: map[string]*Environment{
						"staging": {RequireConfirmation: true},
					},
				},
			},
			want: true,
		},
		{
			name: "environment does not require confirmation",
			config: &Config{
				Env: "development",
				configFile: &ConfigFile{
					Environments: map[string]*Environment{
						"development": {RequireConfirmation: false},
					},
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := &App{config: tt.config}
			got := app.requiresConfirmation()
			if got != tt.want {
				t.Errorf("requiresConfirmation() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetEnvironmentName(t *testing.T) {
	tests := []struct {
		name   string
		config *Config
		want   string
	}{
		{
			name:   "returns environment name",
			config: &Config{Env: "production"},
			want:   "production",
		},
		{
			name:   "returns custom when no environment",
			config: &Config{},
			want:   "custom",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := &App{config: tt.config}
			got := app.getEnvironmentName()
			if got != tt.want {
				t.Errorf("getEnvironmentName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRedactDSN(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
		want string
	}{
		{
			name: "postgres url password",
			dsn:  "postgres://user:secret@localhost:5432/app?sslmode=disable",
			want: "postgres://user:redacted@localhost:5432/app?sslmode=disable",
		},
		{
			name: "mysql tcp dsn",
			dsn:  "user:secret@tcp(localhost:3306)/app?parseTime=true",
			want: "user:redacted@tcp(localhost:3306)/app?parseTime=true",
		},
		{
			name: "mssql url password",
			dsn:  "sqlserver://sa:secret@localhost:1433?database=app",
			want: "sqlserver://sa:redacted@localhost:1433?database=app",
		},
		{
			name: "key value password",
			dsn:  "server=localhost;user id=sa;password=secret;database=app",
			want: "server=localhost;user id=sa;password=redacted;database=app",
		},
		{
			name: "no secret",
			dsn:  "./app.db?_journal_mode=WAL",
			want: "./app.db?_journal_mode=WAL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := redactDSN(tt.dsn); got != tt.want {
				t.Fatalf("redactDSN() = %q, want %q", got, tt.want)
			}
			if strings.Contains(redactDSN(tt.dsn), "secret") {
				t.Fatalf("redactDSN() leaked secret in %q", tt.dsn)
			}
		})
	}
}

func TestEnvironmentLockTimeout(t *testing.T) {
	tempDir := t.TempDir()

	configYAML := `config_locked: false
development:
  driver: postgres
  dsn: postgres://localhost/dev
  lock_timeout: 1h30m
`

	// Change to temp directory
	oldWd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chdir(oldWd) }()

	if err := os.Chdir(tempDir); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(".queen.yaml", []byte(configYAML), 0644); err != nil {
		t.Fatal(err)
	}

	app := &App{
		config: &Config{
			UseConfig: true,
			Env:       "development",
			Table:     "queen_migrations",
		},
	}

	if err := app.loadConfigFile(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := 90 * time.Minute
	if app.config.LockTimeout != expected {
		t.Errorf("lock_timeout = %v, want %v", app.config.LockTimeout, expected)
	}
}

// contains checks if s contains substr
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
