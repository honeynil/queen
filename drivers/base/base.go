// Package base provides common functionality for Queen database drivers.
package base

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/honeynil/queen"
)

// Config contains configuration for the base driver.
type Config struct {
	Placeholder     func(n int) string
	QuoteIdentifier func(name string) string
	ParseTime       func(src interface{}) (time.Time, error)
}

// Driver provides a base implementation of common queen.Driver methods.
// Concrete drivers should embed this type and implement Init() and Lock()/Unlock().
type Driver struct {
	DB        *sql.DB
	TableName string
	Config    Config
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

	_, err := d.DB.ExecContext(ctx, query,
		m.Version, m.Name, m.Checksum(),
		appliedBy, durationMS, hostname, environment,
		action, status, errorMessage,
	)
	return err
}

// Remove removes a migration record from the database.
func (d *Driver) Remove(ctx context.Context, version string) error {
	query := fmt.Sprintf(`
		DELETE FROM %s WHERE version = %s
	`,
		d.Config.QuoteIdentifier(d.TableName),
		d.Config.Placeholder(1),
	)

	_, err := d.DB.ExecContext(ctx, query, version)
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

// ParseTimeISO8601 parses time from ISO8601 string format.
func ParseTimeISO8601(src any) (time.Time, error) {
	str, ok := src.(string)
	if !ok {
		return time.Time{}, fmt.Errorf("expected string, got %T", src)
	}
	return time.Parse("2006-01-02 15:04:05", str)
}
