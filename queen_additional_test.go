package queen

import (
	"context"
	"errors"
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
