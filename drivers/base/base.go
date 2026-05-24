// Package base provides common functionality for Queen database drivers.
package base

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/yaop-labs/queen"
)

// Config contains configuration for the base driver.
type Config struct {
	Placeholder     func(n int) string
	QuoteIdentifier func(name string) string
	ParseTime       func(src any) (time.Time, error)
}

// Driver holds the shared database handle and migration-table helpers.
type Driver struct {
	DB        *sql.DB
	TableName string
	Config    Config
}

type execContext interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

// SQLDB exposes the underlying database handle for optional diagnostics.
func (d *Driver) SQLDB() *sql.DB {
	return d.DB
}

// Exec executes a function within a transaction with the specified isolation level.
func (d *Driver) Exec(ctx context.Context, isolationLevel sql.IsolationLevel, fn func(*sql.Tx) error) error {
	txOpts := &sql.TxOptions{
		Isolation: isolationLevel,
	}

	tx, err := d.DB.BeginTx(ctx, txOpts)
	if err != nil {
		return err
	}

	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (d *Driver) Close() error {
	return d.DB.Close()
}

// GetApplied returns all applied migrations sorted by applied_at.
func (d *Driver) GetApplied(ctx context.Context) ([]queen.Applied, error) {
	query := fmt.Sprintf(`
		SELECT version, name, applied_at, checksum,
		       applied_by, duration_ms, hostname, environment,
		       action, status, error_message
		FROM %s
		ORDER BY applied_at ASC
	`, d.Config.QuoteIdentifier(d.TableName))

	rows, err := d.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var applied []queen.Applied
	for rows.Next() {
		var a queen.Applied
		var appliedBy, hostname, environment, action, status, errorMessage sql.NullString
		var durationMS sql.NullInt64

		if d.Config.ParseTime != nil {
			var appliedAtStr string
			if err := rows.Scan(
				&a.Version, &a.Name, &appliedAtStr, &a.Checksum,
				&appliedBy, &durationMS, &hostname, &environment,
				&action, &status, &errorMessage,
			); err != nil {
				return nil, err
			}
			parsedTime, err := d.Config.ParseTime(appliedAtStr)
			if err != nil {
				return nil, fmt.Errorf("failed to parse applied_at: %w", err)
			}
			a.AppliedAt = parsedTime
		} else {
			if err := rows.Scan(
				&a.Version, &a.Name, &a.AppliedAt, &a.Checksum,
				&appliedBy, &durationMS, &hostname, &environment,
				&action, &status, &errorMessage,
			); err != nil {
				return nil, err
			}
		}

		if appliedBy.Valid {
			a.AppliedBy = appliedBy.String
		}
		if durationMS.Valid {
			a.DurationMS = durationMS.Int64
		}
		if hostname.Valid {
			a.Hostname = hostname.String
		}
		if environment.Valid {
			a.Environment = environment.String
		}
		if action.Valid {
			a.Action = action.String
		}
		if status.Valid {
			a.Status = status.String
		}
		if errorMessage.Valid {
			a.ErrorMessage = errorMessage.String
		}

		applied = append(applied, a)
	}

	return applied, rows.Err()
}

// Record marks a migration as applied in the database.
func (d *Driver) Record(ctx context.Context, m *queen.Migration, meta *queen.MigrationMetadata) error {
	return d.record(ctx, d.DB, m, meta)
}

// RecordTx marks a migration as applied using tx. Concrete drivers can expose
// this through queen.TransactionalRecorder when their database supports it.
func RecordTx(ctx context.Context, d *Driver, tx *sql.Tx, m *queen.Migration, meta *queen.MigrationMetadata) error {
	return d.record(ctx, tx, m, meta)
}

func (d *Driver) record(ctx context.Context, execer execContext, m *queen.Migration, meta *queen.MigrationMetadata) error {
	query := fmt.Sprintf(`
			INSERT INTO %s (version, name, checksum, applied_by, duration_ms, hostname, environment, action, status, error_message)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
	`,
		d.Config.QuoteIdentifier(d.TableName),
		d.Config.Placeholder(1),
		d.Config.Placeholder(2),
		d.Config.Placeholder(3),
		d.Config.Placeholder(4),
		d.Config.Placeholder(5),
		d.Config.Placeholder(6),
		d.Config.Placeholder(7),
		d.Config.Placeholder(8),
		d.Config.Placeholder(9),
		d.Config.Placeholder(10),
	)

	var appliedBy, hostname, environment, action, status, errorMessage sql.NullString
	var durationMS sql.NullInt64

	if meta != nil {
		if meta.AppliedBy != "" {
			appliedBy = sql.NullString{String: meta.AppliedBy, Valid: true}
		}
		if meta.DurationMS > 0 {
			durationMS = sql.NullInt64{Int64: meta.DurationMS, Valid: true}
		}
		if meta.Hostname != "" {
			hostname = sql.NullString{String: meta.Hostname, Valid: true}
		}
		if meta.Environment != "" {
			environment = sql.NullString{String: meta.Environment, Valid: true}
		}
		if meta.Action != "" {
			action = sql.NullString{String: meta.Action, Valid: true}
		}
		if meta.Status != "" {
			status = sql.NullString{String: meta.Status, Valid: true}
		}
		if meta.ErrorMessage != "" {
			errorMessage = sql.NullString{String: meta.ErrorMessage, Valid: true}
		}
	}

	_, err := execer.ExecContext(ctx, query,
		m.Version, m.Name, m.Checksum(),
		appliedBy, durationMS, hostname, environment,
		action, status, errorMessage,
	)
	return err
}

// Remove removes a migration record from the database.
func (d *Driver) Remove(ctx context.Context, version string) error {
	return d.remove(ctx, d.DB, version)
}

// RemoveTx removes a migration record using tx. Concrete drivers can expose
// this through queen.TransactionalRecorder when their database supports it.
func RemoveTx(ctx context.Context, d *Driver, tx *sql.Tx, version string) error {
	return d.remove(ctx, tx, version)
}

func (d *Driver) remove(ctx context.Context, execer execContext, version string) error {
	query := fmt.Sprintf(`
			DELETE FROM %s WHERE version = %s
		`,
		d.Config.QuoteIdentifier(d.TableName),
		d.Config.Placeholder(1),
	)

	_, err := execer.ExecContext(ctx, query, version)
	return err
}

// PlaceholderDollar creates placeholders in the format $1, $2, $3...
func PlaceholderDollar(n int) string {
	return fmt.Sprintf("$%d", n)
}

// PlaceholderQuestion creates placeholders in the format ?, ?, ?...
func PlaceholderQuestion(n int) string {
	return "?"
}

// PlaceholderAtSign creates placeholders in the format @p1, @p2, @p3...
// Used for MS SQL Server which requires named parameters.
func PlaceholderAtSign(n int) string {
	return fmt.Sprintf("@p%d", n)
}

// ParseTimeISO8601 parses common SQLite timestamp and ISO-8601 string formats.
func ParseTimeISO8601(src any) (time.Time, error) {
	if t, ok := src.(time.Time); ok {
		return t, nil
	}

	str, ok := src.(string)
	if !ok {
		return time.Time{}, fmt.Errorf("expected string, got %T", src)
	}

	formats := []string{
		time.RFC3339Nano,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
	}
	for _, format := range formats {
		if t, err := time.Parse(format, str); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported time format %q", str)
}
