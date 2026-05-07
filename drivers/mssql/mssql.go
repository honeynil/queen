// Package mssql provides a MS SQL Server driver for Queen migrations.
package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/honeynil/queen"
	"github.com/honeynil/queen/drivers/base"
)

// Driver implements the queen.Driver interface for MS SQL Server.
type Driver struct {
	base.Driver
	lockName string
	conn     *sql.Conn
}

// New creates a new MS SQL Server driver.
func New(db *sql.DB) *Driver {
	return NewWithTableName(db, "queen_migrations")
}

// NewWithTableName creates a new MS SQL Server driver with a custom table name.
func NewWithTableName(db *sql.DB, tableName string) *Driver {
	return &Driver{
		Driver: base.Driver{
			DB:        db,
			TableName: tableName,
			Config: base.Config{
				Placeholder:     base.PlaceholderAtSign,
				QuoteIdentifier: base.QuoteBrackets,
				ParseTime:       nil,
			},
		},
		lockName: "queen_lock_" + tableName,
	}
}

// Init creates the migrations tracking table if it doesn't exist.
func (d *Driver) Init(ctx context.Context) error {
	query := fmt.Sprintf(`
		IF OBJECT_ID(N'%s', N'U') IS NULL
		BEGIN
			CREATE TABLE %s (
				version NVARCHAR(255) PRIMARY KEY,
				name NVARCHAR(255) NOT NULL,
				applied_at DATETIME2 DEFAULT GETUTCDATE(),
				checksum NVARCHAR(64) NOT NULL,
				applied_by NVARCHAR(255),
				duration_ms BIGINT,
				hostname NVARCHAR(255),
				environment NVARCHAR(50),
				action NVARCHAR(20) DEFAULT 'apply',
				status NVARCHAR(20) DEFAULT 'success',
				error_message NVARCHAR(MAX)
			)
		END
	`, d.TableName, d.Config.QuoteIdentifier(d.TableName))

	if _, err := d.DB.ExecContext(ctx, query); err != nil {
		return err
	}

	migrations := []struct {
		column string
		query  string
	}{
		{"applied_by", fmt.Sprintf(`IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'%s') AND name = 'applied_by')
			ALTER TABLE %s ADD applied_by NVARCHAR(255)`, d.TableName, d.Config.QuoteIdentifier(d.TableName))},
		{"duration_ms", fmt.Sprintf(`IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'%s') AND name = 'duration_ms')
			ALTER TABLE %s ADD duration_ms BIGINT`, d.TableName, d.Config.QuoteIdentifier(d.TableName))},
		{"hostname", fmt.Sprintf(`IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'%s') AND name = 'hostname')
			ALTER TABLE %s ADD hostname NVARCHAR(255)`, d.TableName, d.Config.QuoteIdentifier(d.TableName))},
		{"environment", fmt.Sprintf(`IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'%s') AND name = 'environment')
			ALTER TABLE %s ADD environment NVARCHAR(50)`, d.TableName, d.Config.QuoteIdentifier(d.TableName))},
		{"action", fmt.Sprintf(`IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'%s') AND name = 'action')
			ALTER TABLE %s ADD action NVARCHAR(20) DEFAULT 'apply'`, d.TableName, d.Config.QuoteIdentifier(d.TableName))},
		{"status", fmt.Sprintf(`IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'%s') AND name = 'status')
			ALTER TABLE %s ADD status NVARCHAR(20) DEFAULT 'success'`, d.TableName, d.Config.QuoteIdentifier(d.TableName))},
		{"error_message", fmt.Sprintf(`IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'%s') AND name = 'error_message')
			ALTER TABLE %s ADD error_message NVARCHAR(MAX)`, d.TableName, d.Config.QuoteIdentifier(d.TableName))},
	}

	for _, migration := range migrations {
		_, _ = d.DB.ExecContext(ctx, migration.query)
	}

	return nil
}

// Lock acquires an application lock to prevent concurrent migrations.
func (d *Driver) Lock(ctx context.Context, timeout time.Duration) error {
	conn, err := d.DB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}

	var result int
	query := `
		DECLARE @result INT;
		EXEC @result = sp_getapplock
			@Resource = @p1,
			@LockMode = 'Exclusive',
			@LockOwner = 'Session',
			@LockTimeout = @p2;
		SELECT @result;
	`

	err = conn.QueryRowContext(ctx, query,
		sql.Named("p1", d.lockName),
		sql.Named("p2", int(timeout.Milliseconds())),
	).Scan(&result)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	if result < 0 {
		_ = conn.Close()
		var reason string
		switch result {
		case -1:
			reason = "timeout"
		case -2:
			reason = "canceled"
		case -3:
			reason = "deadlock"
		default:
			reason = "error"
		}
		return fmt.Errorf("%w: failed to acquire lock '%s' for table '%s' (reason: %s, code: %d)",
			queen.ErrLockTimeout, d.lockName, d.TableName, reason, result)
	}

	d.conn = conn
	return nil
}

// Unlock releases the migration lock.
func (d *Driver) Unlock(ctx context.Context) error {
	if d.conn == nil {
		return nil
	}

	var result int
	query := `
		DECLARE @result INT;
		EXEC @result = sp_releaseapplock
			@Resource = @p1,
			@LockOwner = 'Session';
		SELECT @result;
	`

	err := d.conn.QueryRowContext(ctx, query, sql.Named("p1", d.lockName)).Scan(&result)
	if err != nil {
		return fmt.Errorf("failed to release lock '%s' for table '%s': %w",
			d.lockName, d.TableName, err)
	}

	_ = d.conn.Close()
	d.conn = nil

	return nil
}
