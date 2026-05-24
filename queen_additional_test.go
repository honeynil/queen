package queen

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestWithLogger(t *testing.T) {
	t.Parallel()

	driver := &testDriver{}
	logger := &testLogger{}

	q := New(driver, WithLogger(logger))

	if q.logger != logger {
		t.Error("WithLogger option did not set logger")
	}
}

func TestReset(t *testing.T) {
	t.Parallel()

	t.Run("returns error when no driver", func(t *testing.T) {
		t.Parallel()

		q := &Queen{driver: nil}
		err := q.Reset(context.Background())

		if !errors.Is(err, ErrNoDriver) {
			t.Errorf("Reset() error = %v, want %v", err, ErrNoDriver)
		}
	})

	t.Run("resets all migrations", func(t *testing.T) {
		t.Parallel()

		driver := &testDriver{}
		q := New(driver)

		q.MustAdd(M{
			Version: "001",
			Name:    "test",
			UpSQL:   "SELECT 1",
			DownSQL: "SELECT 2",
		})

		err := q.Reset(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestDownNonPositiveRollsBackOneMigration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	q := New(&testDriver{})

	rolledBack := ""
	for i := 1; i <= 3; i++ {
		version := fmt.Sprintf("%03d", i)
		q.MustAdd(M{
			Version: version,
			Name:    fmt.Sprintf("migration_%03d", i),
			UpFunc:  func(context.Context, *sql.Tx) error { return nil },
			DownFunc: func(context.Context, *sql.Tx) error {
				rolledBack = version
				return nil
			},
		})
	}

	if err := q.Up(ctx); err != nil {
		t.Fatalf("Up() failed: %v", err)
	}
	if err := q.Down(ctx, 0); err != nil {
		t.Fatalf("Down(ctx, 0) failed: %v", err)
	}
	if rolledBack != "003" {
		t.Fatalf("Down(ctx, 0) rolled back %q, want latest migration 003", rolledBack)
	}

	statuses, err := q.Status(ctx)
	if err != nil {
		t.Fatalf("Status() failed: %v", err)
	}
	applied := 0
	for _, s := range statuses {
		if s.Status == StatusApplied {
			applied++
		}
	}
	if applied != 2 {
		t.Fatalf("applied migrations after Down(ctx, 0) = %d, want 2", applied)
	}
}

func TestStatus(t *testing.T) {
	t.Parallel()

	t.Run("returns error when no driver", func(t *testing.T) {
		t.Parallel()

		q := &Queen{driver: nil}
		_, err := q.Status(context.Background())

		if !errors.Is(err, ErrNoDriver) {
			t.Errorf("Status() error = %v, want %v", err, ErrNoDriver)
		}
	})

	t.Run("returns migration statuses", func(t *testing.T) {
		t.Parallel()

		driver := &testDriver{}
		q := New(driver)

		q.MustAdd(M{
			Version: "001",
			Name:    "test",
			UpSQL:   "SELECT 1",
		})

		statuses, err := q.Status(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(statuses) != 1 {
			t.Errorf("expected 1 status, got %d", len(statuses))
		}
	})

	t.Run("returns statuses in natural version order", func(t *testing.T) {
		t.Parallel()

		driver := &testDriver{}
		q := New(driver)

		for _, m := range []M{
			{Version: "010", Name: "ten", UpSQL: "SELECT 10"},
			{Version: "002", Name: "two", UpSQL: "SELECT 2"},
			{Version: "001", Name: "one", UpSQL: "SELECT 1"},
		} {
			q.MustAdd(m)
		}

		statuses, err := q.Status(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		got := []string{statuses[0].Version, statuses[1].Version, statuses[2].Version}
		want := []string{"001", "002", "010"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("status order = %v, want %v", got, want)
		}
	})
}

func TestValidate(t *testing.T) {
	t.Parallel()

	t.Run("returns error when no migrations", func(t *testing.T) {
		t.Parallel()

		driver := &testDriver{}
		q := New(driver)

		err := q.Validate(context.Background())

		if err == nil {
			t.Error("Validate() should return error when no migrations registered")
		}
	})

	t.Run("validates migrations", func(t *testing.T) {
		t.Parallel()

		driver := &testDriver{}
		q := New(driver)

		q.MustAdd(M{
			Version: "001",
			Name:    "test",
			UpSQL:   "SELECT 1",
		})

		err := q.Validate(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestMigrationCommandsRejectChecksumMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(context.Context, *Queen) error
	}{
		{name: "up", run: func(ctx context.Context, q *Queen) error { return q.Up(ctx) }},
		{name: "down", run: func(ctx context.Context, q *Queen) error { return q.Down(ctx, 1) }},
		{name: "reset", run: func(ctx context.Context, q *Queen) error { return q.Reset(ctx) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			driver := &testDriver{
				applied: map[string]Applied{
					"001": {
						Version:  "001",
						Name:     "test",
						Checksum: "old-checksum",
					},
				},
			}
			q := New(driver)
			q.MustAdd(M{
				Version: "001",
				Name:    "test",
				UpSQL:   "SELECT 1",
				DownSQL: "SELECT 0",
			})

			err := tt.run(context.Background(), q)
			if !errors.Is(err, ErrChecksumMismatch) {
				t.Fatalf("%s error = %v, want ErrChecksumMismatch", tt.name, err)
			}
		})
	}
}

func TestDryRun(t *testing.T) {
	t.Parallel()

	t.Run("returns error when no driver", func(t *testing.T) {
		t.Parallel()

		q := &Queen{driver: nil}
		_, err := q.DryRun(context.Background(), DirectionUp, 0)

		if !errors.Is(err, ErrNoDriver) {
			t.Errorf("DryRun() error = %v, want %v", err, ErrNoDriver)
		}
	})

	t.Run("returns migration plan", func(t *testing.T) {
		t.Parallel()

		driver := &testDriver{}
		q := New(driver)

		q.MustAdd(M{
			Version: "001",
			Name:    "test",
			UpSQL:   "SELECT 1",
		})

		plan, err := q.DryRun(context.Background(), DirectionUp, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(plan) != 1 {
			t.Errorf("expected 1 plan item, got %d", len(plan))
		}
	})
}

func TestExplain(t *testing.T) {
	t.Parallel()

	t.Run("returns error when no driver", func(t *testing.T) {
		t.Parallel()

		q := &Queen{driver: nil}
		_, err := q.Explain(context.Background(), "001")

		if !errors.Is(err, ErrNoDriver) {
			t.Errorf("Explain() error = %v, want %v", err, ErrNoDriver)
		}
	})

	t.Run("explains migration", func(t *testing.T) {
		t.Parallel()

		driver := &testDriver{}
		q := New(driver)

		q.MustAdd(M{
			Version: "001",
			Name:    "test",
			UpSQL:   "SELECT 1",
		})

		plan, err := q.Explain(context.Background(), "001")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if plan == nil {
			t.Error("expected plan, got nil")
		}
	})
}

func TestDriver(t *testing.T) {
	t.Parallel()

	driver := &testDriver{}
	q := New(driver)

	if q.Driver() != driver {
		t.Error("Driver() did not return the correct driver")
	}
}

func TestGetIsolationLevel(t *testing.T) {
	t.Parallel()

	t.Run("returns migration-specific isolation level", func(t *testing.T) {
		t.Parallel()

		q := &Queen{
			config: &Config{},
		}

		m := &Migration{
			IsolationLevel: 4, // sql.LevelSerializable
		}

		level := q.getIsolationLevel(m)
		if level != 4 {
			t.Errorf("isolation level = %v, want 4", level)
		}
	})

	t.Run("returns config isolation level when migration has default", func(t *testing.T) {
		t.Parallel()

		q := &Queen{
			config: &Config{
				IsolationLevel: 2, // sql.LevelReadCommitted
			},
		}

		m := &Migration{
			IsolationLevel: 0, // Default
		}

		level := q.getIsolationLevel(m)
		if level != 2 {
			t.Errorf("isolation level = %v, want 2", level)
		}
	})
}

func TestMustAdd(t *testing.T) {
	t.Parallel()

	t.Run("panics on duplicate version", func(t *testing.T) {
		t.Parallel()

		defer func() {
			if r := recover(); r == nil {
				t.Error("MustAdd should panic on duplicate version")
			}
		}()

		q := New(&testDriver{})
		q.MustAdd(M{Version: "001", Name: "first"})
		q.MustAdd(M{Version: "001", Name: "duplicate"})
	})
}
