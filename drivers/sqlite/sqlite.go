// Package sqlite provides a SQLite driver for Queen migrations.
package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/yaop-labs/queen"
	"github.com/yaop-labs/queen/drivers/base"
)

type processLock struct {
	token chan struct{}
}

var processLocks sync.Map

// Driver implements the queen.Driver interface for SQLite.
type Driver struct {
	base.Driver
	lockMu    sync.Mutex
	lock      *processLock
	acquiring bool
	lockHeld  bool
}

var _ queen.TransactionalRecorder = (*Driver)(nil)

// New creates a new SQLite driver.
func New(db *sql.DB) *Driver {
	return NewWithTableName(db, "queen_migrations")
}

// NewWithTableName creates a new SQLite driver with a custom table name.
func NewWithTableName(db *sql.DB, tableName string) *Driver {
	lockValue, _ := processLocks.LoadOrStore(tableName, newProcessLock())

	return &Driver{
		Driver: base.Driver{
			DB:        db,
			TableName: tableName,
			Config: base.Config{
				Placeholder:     base.PlaceholderQuestion,
				QuoteIdentifier: base.QuoteDoubleQuotes,
				ParseTime:       base.ParseTimeISO8601,
			},
		},
		lock: lockValue.(*processLock),
	}
}

func newProcessLock() *processLock {
	lock := &processLock{token: make(chan struct{}, 1)}
	lock.token <- struct{}{}
	return lock
}

// Init creates the migrations tracking table if it doesn't exist.
func (d *Driver) Init(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			version TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			applied_at TEXT NOT NULL DEFAULT (datetime('now')),
			checksum TEXT NOT NULL,
			applied_by TEXT,
			duration_ms INTEGER,
			hostname TEXT,
			environment TEXT,
			action TEXT DEFAULT 'apply',
			status TEXT DEFAULT 'success',
			error_message TEXT
		) WITHOUT ROWID
	`, d.Config.QuoteIdentifier(d.TableName))

	if _, err := d.DB.ExecContext(ctx, query); err != nil {
		return err
	}

	migrations := []struct {
		column string
		query  string
	}{
		{"applied_by", fmt.Sprintf(`ALTER TABLE %s ADD COLUMN applied_by TEXT`, d.Config.QuoteIdentifier(d.TableName))},
		{"duration_ms", fmt.Sprintf(`ALTER TABLE %s ADD COLUMN duration_ms INTEGER`, d.Config.QuoteIdentifier(d.TableName))},
		{"hostname", fmt.Sprintf(`ALTER TABLE %s ADD COLUMN hostname TEXT`, d.Config.QuoteIdentifier(d.TableName))},
		{"environment", fmt.Sprintf(`ALTER TABLE %s ADD COLUMN environment TEXT`, d.Config.QuoteIdentifier(d.TableName))},
		{"action", fmt.Sprintf(`ALTER TABLE %s ADD COLUMN action TEXT DEFAULT 'apply'`, d.Config.QuoteIdentifier(d.TableName))},
		{"status", fmt.Sprintf(`ALTER TABLE %s ADD COLUMN status TEXT DEFAULT 'success'`, d.Config.QuoteIdentifier(d.TableName))},
		{"error_message", fmt.Sprintf(`ALTER TABLE %s ADD COLUMN error_message TEXT`, d.Config.QuoteIdentifier(d.TableName))},
	}

	for _, migration := range migrations {
		var columnExists int
		err := d.DB.QueryRowContext(
			ctx,
			`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?`,
			d.TableName,
			migration.column,
		).Scan(&columnExists)
		if err != nil {
			return fmt.Errorf("check migration metadata column %q: %w", migration.column, err)
		}
		if columnExists == 0 {
			if _, err := d.DB.ExecContext(ctx, migration.query); err != nil {
				return fmt.Errorf("add migration metadata column %q: %w", migration.column, err)
			}
		}
	}

	return nil
}

// Lock serializes Queen instances in the current process. SQLite still provides
// transaction-level database locking; this lock is not cross-process.
func (d *Driver) Lock(ctx context.Context, timeout time.Duration) error {
	d.lockMu.Lock()
	if d.acquiring || d.lockHeld {
		d.lockMu.Unlock()
		return fmt.Errorf("%w: lock already held for table '%s'", queen.ErrLockTimeout, d.TableName)
	}
	d.acquiring = true
	d.lockMu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		d.lockMu.Lock()
		d.acquiring = false
		d.lockMu.Unlock()
		return ctx.Err()
	case <-timer.C:
		d.lockMu.Lock()
		d.acquiring = false
		d.lockMu.Unlock()
		return fmt.Errorf("%w: failed to acquire process-local lock for table '%s'",
			queen.ErrLockTimeout, d.TableName)
	case <-d.lock.token:
		d.lockMu.Lock()
		d.acquiring = false
		d.lockHeld = true
		d.lockMu.Unlock()
		return nil
	}
}

// Unlock releases the migration lock.
func (d *Driver) Unlock(_ context.Context) error {
	d.lockMu.Lock()
	defer d.lockMu.Unlock()

	if !d.lockHeld {
		return nil
	}

	d.lockHeld = false
	d.lock.token <- struct{}{}
	return nil
}

// RecordTx records a migration in the transaction used for its body.
func (d *Driver) RecordTx(ctx context.Context, tx *sql.Tx, m *queen.Migration, meta *queen.MigrationMetadata) error {
	return base.RecordTx(ctx, &d.Driver, tx, m, meta)
}

// RemoveTx removes a migration record in the transaction used for rollback.
func (d *Driver) RemoveTx(ctx context.Context, tx *sql.Tx, version string) error {
	return base.RemoveTx(ctx, &d.Driver, tx, version)
}

// Close releases a held process-local lock before closing the database.
func (d *Driver) Close() error {
	_ = d.Unlock(context.Background())
	return d.DB.Close()
}
