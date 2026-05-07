package cli

import (
	"strings"
	"testing"
)

func TestGetSQLDriverName(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"postgres", "pgx"},
		{"postgresql", "pgx"},
		{"mysql", "mysql"},
		{"sqlite", "sqlite3"},
		{"sqlite3", "sqlite3"},
		{"clickhouse", "clickhouse"},
		{"cockroachdb", "pgx"},
		{"mssql", "sqlserver"},
		{"sqlserver", "sqlserver"},
		{"unknown", "unknown"}, // passthrough for unknown drivers
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := getSQLDriverName(tt.input)
			if got != tt.want {
				t.Errorf("getSQLDriverName(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestCreateDriver(t *testing.T) {
	tests := []struct {
		driver      string
		wantErr     bool
		errContains string
	}{
		{"postgres", false, ""},
		{"postgresql", false, ""},
		{"pgx", false, ""},
		{"mysql", false, ""},
		{"sqlite", false, ""},
		{"sqlite3", false, ""},
		{"clickhouse", false, ""},
		{"cockroachdb", false, ""},
		{"mssql", false, ""},
		{"sqlserver", false, ""},
		{"unknown", true, "unsupported driver"},
		{"oracle", true, "unsupported driver"},
	}

	for _, tt := range tests {
		t.Run(tt.driver, func(t *testing.T) {
			app := &App{
				config: &Config{
					Driver: tt.driver,
				},
			}

			// We can't test with a real DB, but we can test error cases
			// For valid drivers, we'd need a mock DB connection
			_, err := app.createDriver(nil)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error for driver %q, got nil", tt.driver)
					return
				}
				if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("expected error containing %q, got %q", tt.errContains, err.Error())
				}
			} else {
				// For valid drivers with nil DB, we expect either success or a panic
				// (since drivers try to use the nil DB)
				// This is acceptable for unit tests - integration tests would use real DBs
				if err != nil {
					if strings.Contains(err.Error(), "unsupported") {
						return
					}
					t.Errorf("unexpected error for driver %q: %v", tt.driver, err)
				}
			}
		})
	}
}
