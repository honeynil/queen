package queen

import (
	"context"
	"database/sql"
	"strings"

	"github.com/yaop-labs/queen/internal/checksum"
)

type MigrationFunc func(ctx context.Context, tx *sql.Tx) error

// Migration represents a database migration.
type Migration struct {
	Version        string
	Name           string
	UpSQL          string
	DownSQL        string
	UpFunc         MigrationFunc
	DownFunc       MigrationFunc
	ManualChecksum string
	IsolationLevel sql.IsolationLevel
}

type M = Migration

func (m *Migration) Validate() error {
	if m.Version == "" || strings.Contains(m.Version, " ") || !IsValidMigrationVersion(m.Version) {
		return ErrInvalidMigration
	}

	if len(m.Name) > 63 {
		return ErrNameTooLong
	}

	if m.Name == "" || !IsValidMigrationName(m.Name) {
		return ErrInvalidMigrationName
	}

	if m.UpSQL == "" && m.UpFunc == nil {
		return ErrInvalidMigration
	}

	return nil
}

const noChecksumMarker = "no-checksum-go-func"

func (m *Migration) Checksum() string {
	if m.ManualChecksum != "" {
		return m.ManualChecksum
	}

	if m.UpSQL != "" || m.DownSQL != "" {
		return checksum.Calculate(m.UpSQL, m.DownSQL)
	}

	return noChecksumMarker
}

func (m *Migration) HasRollback() bool {
	return m.DownSQL != "" || m.DownFunc != nil
}

func (m *Migration) IsDestructive() bool {
	if m.DownSQL == "" {
		return false
	}

	sql := strings.ToUpper(m.DownSQL)

	destructiveKeywords := []string{
		"DROP TABLE",
		"DROP DATABASE",
		"DROP SCHEMA",
		"TRUNCATE",
	}

	for _, keyword := range destructiveKeywords {
		if strings.Contains(sql, keyword) {
			return true
		}
	}

	return false
}
