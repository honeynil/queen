package queen

import (
	"context"
	"testing"
)

// TestHelper provides testing utilities for migrations.
type TestHelper struct {
	*Queen
	t   *testing.T
	ctx context.Context
}

// NewTest creates a new Queen instance for testing.
// It uses the provided driver and will clean up automatically when the test ends.
//
// Usage:
//
//	func TestMigrations(t *testing.T) {
//	    db := setupTestDB(t) // Your test DB setup
//	    driver := postgres.New(db)
//	    q := queen.NewTest(t, driver)
//
//	    q.MustAdd(queen.M{...})
//
//	    // Test will automatically clean up
//	}
func NewTest(t *testing.T, driver Driver) *TestHelper {
	t.Helper()

	q := New(driver)
	ctx := context.Background()

	// Initialize on creation
	if err := q.driver.Init(ctx); err != nil {
		t.Fatalf("Failed to initialize driver: %v", err)
	}

	// Clean up when test ends
	t.Cleanup(func() {
		q.Close()
	})

	return &TestHelper{
		Queen: q,
		t:     t,
		ctx:   ctx,
	}
}

// TestUpDown tests that migrations can be applied and rolled back successfully.
// This validates that your Down migrations work correctly.
//
// Usage:
//
//	func TestMigrations(t *testing.T) {
//	    q := queen.NewTest(t, driver)
//	    q.MustAdd(queen.M{...})
//
//	    q.TestUpDown() // Tests both up and down
//	}
func (th *TestHelper) TestUpDown() {
	th.t.Helper()

	// First, apply all migrations
	if err := th.Up(th.ctx); err != nil {
		th.t.Fatalf("Failed to apply migrations: %v", err)
	}

	// Get count of applied migrations
	applied, err := th.driver.GetApplied(th.ctx)
	if err != nil {
		th.t.Fatalf("Failed to get applied migrations: %v", err)
	}

	count := len(applied)
	if count == 0 {
		th.t.Fatal("No migrations were applied")
	}

	// Now rollback all migrations
	if err := th.Reset(th.ctx); err != nil {
		th.t.Fatalf("Failed to rollback migrations: %v", err)
	}

	// Verify all migrations were rolled back
	applied, err = th.driver.GetApplied(th.ctx)
	if err != nil {
		th.t.Fatalf("Failed to get applied migrations after rollback: %v", err)
	}

	if len(applied) != 0 {
		th.t.Fatalf("Expected 0 migrations after rollback, got %d", len(applied))
	}

	th.t.Logf("✓ Successfully applied and rolled back %d migrations", count)
}

// MustUp is like Up but fails the test on error.
func (th *TestHelper) MustUp() {
	th.t.Helper()
	if err := th.Up(th.ctx); err != nil {
		th.t.Fatalf("Failed to apply migrations: %v", err)
	}
}

// MustDown is like Down but fails the test on error.
func (th *TestHelper) MustDown(n int) {
	th.t.Helper()
	if err := th.Down(th.ctx, n); err != nil {
		th.t.Fatalf("Failed to rollback migrations: %v", err)
	}
}

// MustReset is like Reset but fails the test on error.
func (th *TestHelper) MustReset() {
	th.t.Helper()
	if err := th.Reset(th.ctx); err != nil {
		th.t.Fatalf("Failed to reset migrations: %v", err)
	}
}

// MustValidate is like Validate but fails the test on error.
func (th *TestHelper) MustValidate() {
	th.t.Helper()
	if err := th.Validate(th.ctx); err != nil {
		th.t.Fatalf("Migration validation failed: %v", err)
	}
}
