package queen

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// testDriver is a minimal driver implementation for testing.
type testDriver struct{}

func (d *testDriver) Init(ctx context.Context) error                    { return nil }
func (d *testDriver) GetApplied(ctx context.Context) ([]Applied, error) { return nil, nil }
func (d *testDriver) Record(ctx context.Context, m *Migration, meta *MigrationMetadata) error {
	return nil
}
func (d *testDriver) Remove(ctx context.Context, version string) error      { return nil }
func (d *testDriver) Lock(ctx context.Context, timeout time.Duration) error { return nil }
func (d *testDriver) Unlock(ctx context.Context) error                      { return nil }
func (d *testDriver) Exec(ctx context.Context, isolationLevel sql.IsolationLevel, fn func(*sql.Tx) error) error {
	return nil
}
func (d *testDriver) Close() error { return nil }

func TestGetDriverName(t *testing.T) {
	driver := &testDriver{}
	q := New(driver)

	driverName := q.getDriverName()
	// Should extract "queen" from "*queen.testDriver"
	if driverName != "queen" {
		t.Errorf("getDriverName() = %q, want %q", driverName, "queen")
	}
}

func TestGetDriverNameNilDriver(t *testing.T) {
	q := &Queen{driver: nil}
	driverName := q.getDriverName()
	if driverName != driverNameUnknown {
		t.Errorf("getDriverName() with nil driver = %q, want %q", driverName, driverNameUnknown)
	}
}
