package mock_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/honeynil/queen"
	"github.com/honeynil/queen/drivers/mock"
)

func TestMockDriver_Integration(t *testing.T) {
	driver := mock.New()
	q := queen.New(driver)

	q.MustAdd(queen.M{
		Version:        "001",
		Name:           "first",
		ManualChecksum: "v1",
		UpFunc: func(ctx context.Context, tx *sql.Tx) error {
			return nil
		},
		DownFunc: func(ctx context.Context, tx *sql.Tx) error {
			return nil
		},
	})

	q.MustAdd(queen.M{
		Version:        "002",
		Name:           "second",
		ManualChecksum: "v1",
		UpFunc: func(ctx context.Context, tx *sql.Tx) error {
			return nil
		},
		DownFunc: func(ctx context.Context, tx *sql.Tx) error {
			return nil
		},
	})

	ctx := context.Background()

	// Apply migrations
	if err := q.Up(ctx); err != nil {
		t.Fatalf("Up failed: %v", err)
	}

	// Verify both migrations were applied
	if driver.AppliedCount() != 2 {
		t.Errorf("Expected 2 applied migrations, got %d", driver.AppliedCount())
	}

	if !driver.HasVersion("001") {
		t.Error("Expected version 001 to be applied")
	}

	if !driver.HasVersion("002") {
		t.Error("Expected version 002 to be applied")
	}
}

func TestMockDriver_Down(t *testing.T) {
	driver := mock.New()
	q := queen.New(driver)

	q.MustAdd(queen.M{
		Version:        "001",
		Name:           "first",
		ManualChecksum: "v1",
		UpFunc:         func(ctx context.Context, tx *sql.Tx) error { return nil },
		DownFunc:       func(ctx context.Context, tx *sql.Tx) error { return nil },
	})

	q.MustAdd(queen.M{
		Version:        "002",
		Name:           "second",
		ManualChecksum: "v1",
		UpFunc:         func(ctx context.Context, tx *sql.Tx) error { return nil },
		DownFunc:       func(ctx context.Context, tx *sql.Tx) error { return nil },
	})

	ctx := context.Background()

	// Apply migrations
	if err := q.Up(ctx); err != nil {
		t.Fatalf("Up failed: %v", err)
	}

	// Rollback last migration
	if err := q.Down(ctx, 1); err != nil {
		t.Fatalf("Down failed: %v", err)
	}

	// Should have 1 migration left
	if driver.AppliedCount() != 1 {
		t.Errorf("Expected 1 applied migration after Down, got %d", driver.AppliedCount())
	}

	if !driver.HasVersion("001") {
		t.Error("Expected version 001 to still be applied")
	}

	if driver.HasVersion("002") {
		t.Error("Expected version 002 to be rolled back")
	}
}

func TestMockDriver_GoFunctions(t *testing.T) {
	driver := mock.New()
	q := queen.New(driver)

	upCalled := false
	downCalled := false

	q.MustAdd(queen.M{
		Version:        "001",
		Name:           "go_function_migration",
		ManualChecksum: "v1",
		UpFunc: func(ctx context.Context, tx *sql.Tx) error {
			upCalled = true
			return nil
		},
		DownFunc: func(ctx context.Context, tx *sql.Tx) error {
			downCalled = true
			return nil
		},
	})

	ctx := context.Background()

	// Apply migration
	if err := q.Up(ctx); err != nil {
		t.Fatalf("Up failed: %v", err)
	}

	if !upCalled {
		t.Error("Expected UpFunc to be called")
	}

	if driver.AppliedCount() != 1 {
		t.Errorf("Expected 1 applied migration, got %d", driver.AppliedCount())
	}

	// Rollback
	if err := q.Down(ctx, 1); err != nil {
		t.Fatalf("Down failed: %v", err)
	}

	if !downCalled {
		t.Error("Expected DownFunc to be called")
	}

	if driver.AppliedCount() != 0 {
		t.Errorf("Expected 0 applied migrations after Down, got %d", driver.AppliedCount())
	}
}

func TestMockDriver_Lock(t *testing.T) {
	driver := mock.New()
	q := queen.New(driver)

	q.MustAdd(queen.M{
		Version:        "001",
		Name:           "test",
		ManualChecksum: "v1",
		UpFunc:         func(ctx context.Context, tx *sql.Tx) error { return nil },
	})

	ctx := context.Background()

	// Manually lock
	if err := driver.Lock(ctx, queen.DefaultConfig().LockTimeout); err != nil {
		t.Fatalf("Manual lock failed: %v", err)
	}

	// Try to run migration (should fail due to lock)
	err := q.Up(ctx)
	if err != queen.ErrLockTimeout {
		t.Errorf("Expected ErrLockTimeout, got %v", err)
	}

	// Unlock
	driver.Unlock(ctx)

	// Now it should work
	if err := q.Up(ctx); err != nil {
		t.Fatalf("Up after unlock failed: %v", err)
	}
}

func TestMockDriver_Reset(t *testing.T) {
	driver := mock.New()
	q := queen.New(driver)

	q.MustAdd(queen.M{
		Version:        "001",
		Name:           "first",
		ManualChecksum: "v1",
		UpFunc:         func(ctx context.Context, tx *sql.Tx) error { return nil },
		DownFunc:       func(ctx context.Context, tx *sql.Tx) error { return nil },
	})

	ctx := context.Background()

	// Apply migrations
	if err := q.Up(ctx); err != nil {
		t.Fatalf("Up failed: %v", err)
	}

	// Verify lock is released after Up
	if driver.IsLocked() {
		t.Fatal("Lock should be released after Up()")
	}

	// Reset all
	if err := q.Reset(ctx); err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	// Should have 0 migrations
	if driver.AppliedCount() != 0 {
		t.Errorf("Expected 0 applied migrations after Reset, got %d", driver.AppliedCount())
	}
}

func TestMockDriver_ErrorHandling(t *testing.T) {
	driver := mock.New()
	q := queen.New(driver)

	q.MustAdd(queen.M{
		Version:        "001",
		Name:           "failing_migration",
		ManualChecksum: "v1",
		UpFunc: func(ctx context.Context, tx *sql.Tx) error {
			return errors.New("migration failed")
		},
	})

	ctx := context.Background()

	// Try to apply failing migration
	err := q.Up(ctx)
	if err == nil {
		t.Fatal("Expected error from failing migration")
	}

	// Migration should not be recorded
	if driver.AppliedCount() != 0 {
		t.Errorf("Expected 0 applied migrations after failure, got %d", driver.AppliedCount())
	}
}
