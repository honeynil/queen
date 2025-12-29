package queen

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

func TestMigrationValidate(t *testing.T) {
	tests := []struct {
		name    string
		m       Migration
		wantErr bool
	}{
		{
			name: "valid SQL migration",
			m: Migration{
				Version: "001",
				Name:    "create_users",
				UpSQL:   "CREATE TABLE users (id INT)",
				DownSQL: "DROP TABLE users",
			},
			wantErr: false,
		},
		{
			name: "valid with UpSQL only",
			m: Migration{
				Version: "001",
				Name:    "create_users",
				UpSQL:   "CREATE TABLE users (id INT)",
			},
			wantErr: false,
		},
		{
			name: "missing version",
			m: Migration{
				Name:  "create_users",
				UpSQL: "CREATE TABLE users (id INT)",
			},
			wantErr: true,
		},
		{
			name: "missing name",
			m: Migration{
				Version: "001",
				UpSQL:   "CREATE TABLE users (id INT)",
			},
			wantErr: true,
		},
		{
			name: "missing Up",
			m: Migration{
				Version: "001",
				Name:    "create_users",
			},
			wantErr: true,
		},
		{
			name: "valid Go function migration",
			m: Migration{
				Version: "001",
				Name:    "seed_data",
				UpFunc: func(ctx context.Context, tx *sql.Tx) error {
					return nil
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.m.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Migration.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMigrationChecksum(t *testing.T) {
	t.Run("SQL migration checksum", func(t *testing.T) {
		m := Migration{
			Version: "001",
			Name:    "test",
			UpSQL:   "CREATE TABLE users (id INT)",
			DownSQL: "DROP TABLE users",
		}

		checksum1 := m.Checksum()
		checksum2 := m.Checksum()

		// Should be deterministic
		if checksum1 != checksum2 {
			t.Error("Checksum should be deterministic")
		}

		// Should be non-empty
		if checksum1 == "" {
			t.Error("Checksum should not be empty")
		}
	})

	t.Run("manual checksum takes precedence", func(t *testing.T) {
		m := Migration{
			Version:        "001",
			Name:           "test",
			UpSQL:          "CREATE TABLE users (id INT)",
			ManualChecksum: "v1",
		}

		if m.Checksum() != "v1" {
			t.Errorf("Expected manual checksum 'v1', got %s", m.Checksum())
		}
	})

	t.Run("Go function without manual checksum", func(t *testing.T) {
		m := Migration{
			Version: "001",
			Name:    "test",
			UpFunc: func(ctx context.Context, tx *sql.Tx) error {
				return nil
			},
		}

		if m.Checksum() != "no-checksum-go-func" {
			t.Errorf("Expected 'no-checksum-go-func', got %s", m.Checksum())
		}
	})
}

func TestMigrationHasRollback(t *testing.T) {
	tests := []struct {
		name string
		m    Migration
		want bool
	}{
		{
			name: "has DownSQL",
			m: Migration{
				Version: "001",
				Name:    "test",
				UpSQL:   "CREATE TABLE users (id INT)",
				DownSQL: "DROP TABLE users",
			},
			want: true,
		},
		{
			name: "has DownFunc",
			m: Migration{
				Version: "001",
				Name:    "test",
				UpFunc: func(ctx context.Context, tx *sql.Tx) error {
					return nil
				},
				DownFunc: func(ctx context.Context, tx *sql.Tx) error {
					return nil
				},
			},
			want: true,
		},
		{
			name: "no rollback",
			m: Migration{
				Version: "001",
				Name:    "test",
				UpSQL:   "CREATE TABLE users (id INT)",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.m.HasRollback(); got != tt.want {
				t.Errorf("Migration.HasRollback() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMigrationIsDestructive(t *testing.T) {
	tests := []struct {
		name string
		m    Migration
		want bool
	}{
		{
			name: "DROP TABLE",
			m: Migration{
				Version: "001",
				Name:    "test",
				UpSQL:   "CREATE TABLE users (id INT)",
				DownSQL: "DROP TABLE users",
			},
			want: true,
		},
		{
			name: "TRUNCATE",
			m: Migration{
				Version: "001",
				Name:    "test",
				UpSQL:   "INSERT INTO users...",
				DownSQL: "TRUNCATE TABLE users",
			},
			want: true,
		},
		{
			name: "DROP DATABASE",
			m: Migration{
				Version: "001",
				Name:    "test",
				DownSQL: "DROP DATABASE test",
			},
			want: true,
		},
		{
			name: "safe ALTER",
			m: Migration{
				Version: "001",
				Name:    "test",
				UpSQL:   "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
				DownSQL: "ALTER TABLE users DROP COLUMN email",
			},
			want: false,
		},
		{
			name: "no Down",
			m: Migration{
				Version: "001",
				Name:    "test",
				UpSQL:   "CREATE TABLE users (id INT)",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.m.IsDestructive(); got != tt.want {
				t.Errorf("Migration.IsDestructive() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMigrationExecuteUp(t *testing.T) {
	t.Run("invalid migration", func(t *testing.T) {
		m := Migration{
			Version: "001",
			Name:    "test",
			// No Up method
		}

		err := m.executeUp(context.Background(), nil)
		if !errors.Is(err, ErrInvalidMigration) {
			t.Errorf("Expected ErrInvalidMigration, got %v", err)
		}
	})

	t.Run("UpFunc takes precedence", func(t *testing.T) {
		called := false
		m := Migration{
			Version: "001",
			Name:    "test",
			UpSQL:   "should not be used",
			UpFunc: func(ctx context.Context, tx *sql.Tx) error {
				called = true
				return nil
			},
		}

		m.executeUp(context.Background(), nil)

		if !called {
			t.Error("UpFunc was not called")
		}
	})
}
