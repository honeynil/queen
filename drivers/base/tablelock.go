// Package base provides common functionality for database drivers.
package base

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
	// RetryableInsertError reports whether a failed insert means another
	// contender won the lock race and acquisition should be retried.
	RetryableInsertError func(error) bool
}

// AcquireTableLock retries a table-backed lock until it is acquired or times out.
func AcquireTableLock(ctx context.Context, db *sql.DB, config TableLockConfig, lockKey, ownerID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	backoff := 50 * time.Millisecond
	maxBackoff := 1 * time.Second

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !time.Now().Before(deadline) {
			return queen.ErrLockTimeout
		}

		expiresAt := time.Now().Add(timeout)
		if _, err := db.ExecContext(ctx, config.CleanupQuery, cleanupArgs(config, lockKey, expiresAt)...); err != nil {
			return fmt.Errorf("clean stale lock: %w", err)
		}

		row := db.QueryRowContext(ctx, config.CheckQuery, lockKey)
		hasLock, err := config.ScanFunc(row)
		if errors.Is(err, sql.ErrNoRows) {
			hasLock = false
		} else if err != nil {
			return fmt.Errorf("check existing lock: %w", err)
		}

		if !hasLock {
			if _, err := db.ExecContext(ctx, config.InsertQuery, lockKey, expiresAt, ownerID); err == nil {
				return nil
			} else if config.RetryableInsertError == nil || !config.RetryableInsertError(err) {
				return fmt.Errorf("insert lock: %w", err)
			}
		}

		wait := backoff
		if remaining := time.Until(deadline); remaining < wait {
			wait = remaining
		}
		if wait <= 0 {
			return queen.ErrLockTimeout
		}

		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
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
