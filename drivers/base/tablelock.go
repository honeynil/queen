// Package base provides common functionality for database drivers.
package base

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/yaop-labs/queen"
)

// TableLockConfig configures table-based distributed locking.
type TableLockConfig struct {
	CleanupQuery string
	CleanupArgs  func(lockKey string, expiresAt time.Time) []any
	CheckQuery   string
	InsertQuery  string
	ScanFunc     func(*sql.Row) (bool, error)
}

// AcquireTableLock retries a table-backed lock until it is acquired or times out.
func AcquireTableLock(ctx context.Context, db *sql.DB, config TableLockConfig, lockKey, ownerID string, timeout time.Duration) error {
	start := time.Now()

	backoff := 50 * time.Millisecond
	maxBackoff := 1 * time.Second

	for {
		expiresAt := time.Now().Add(timeout)
		if _, err := db.ExecContext(ctx, config.CleanupQuery, cleanupArgs(config, lockKey, expiresAt)...); err == nil {
			row := db.QueryRowContext(ctx, config.CheckQuery, lockKey)
			hasLock, err := config.ScanFunc(row)
			if err == nil || errors.Is(err, sql.ErrNoRows) {
				if !hasLock {
					if _, err := db.ExecContext(ctx, config.InsertQuery, lockKey, expiresAt, ownerID); err == nil {
						return nil
					}
				}
			}
		}

		if time.Since(start) >= timeout {
			return queen.ErrLockTimeout
		}

		select {
		case <-time.After(backoff):
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func cleanupArgs(config TableLockConfig, lockKey string, expiresAt time.Time) []any {
	if config.CleanupArgs != nil {
		return config.CleanupArgs(lockKey, expiresAt)
	}
	return []any{lockKey, expiresAt}
}
