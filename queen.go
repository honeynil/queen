// Package queen provides a lightweight database migration library for Go.
package queen

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/user"
	"sort"
	"strings"
	"time"

	naturalsort "github.com/honeynil/queen/internal/sort"
)

const (
	DirectionUp       = "up"
	DirectionDown     = "down"
	DriverUnknown     = "unknown"
	driverNameUnknown = "DriverUnknown"
)

// Queen manages database migrations.
type Queen struct {
	driver     Driver
	migrations []*Migration
	config     *Config
	logger     Logger
	applied    map[string]*Applied
}

// Config configures Queen behavior.
type Config struct {
	TableName      string
	LockTimeout    time.Duration
	SkipLock       bool
	Naming         *NamingConfig
	IsolationLevel sql.IsolationLevel
}

// DefaultConfig returns default configuration.
func DefaultConfig() *Config {
	return &Config{
		TableName:   "queen_migrations",
		LockTimeout: 30 * time.Minute,
		SkipLock:    false,
		Naming:      nil,
	}
}

// Option configures a Queen instance.
type Option func(*Queen)

// WithLogger sets a custom logger. Compatible with *slog.Logger.
func WithLogger(logger Logger) Option {
	return func(q *Queen) {
		if logger != nil {
			q.logger = logger
		}
	}
}

// New creates a Queen instance with default configuration.
func New(driver Driver, opts ...Option) *Queen {
	q := NewWithConfig(driver, DefaultConfig())
	for _, opt := range opts {
		opt(q)
	}
	return q
}

// NewWithConfig creates a Queen instance with custom configuration.
func NewWithConfig(driver Driver, config *Config) *Queen {
	if config == nil {
		config = DefaultConfig()
	}

	if config.TableName == "" {
		config.TableName = "queen_migrations"
	}
	if config.LockTimeout <= 0 {
		config.LockTimeout = 30 * time.Minute
	}

	return &Queen{
		driver:     driver,
		migrations: make([]*Migration, 0),
		config:     config,
		logger:     defaultLogger(),
		applied:    make(map[string]*Applied),
	}
}

// Add registers a migration. Returns ErrVersionConflict if version already exists.
func (q *Queen) Add(m M) error {
	if err := m.Validate(); err != nil {
		return err
	}

	for _, existing := range q.migrations {
		if existing.Version == m.Version {
			return fmt.Errorf("%w: %s", ErrVersionConflict, m.Version)
		}
	}

	if q.config.Naming != nil {
		if err := q.config.Naming.Validate(m.Version); err != nil {
			if q.config.Naming.Enforce {
				return fmt.Errorf("naming pattern validation failed: %w", err)
			}
			q.logger.WarnContext(context.Background(), "naming pattern validation failed",
				"version", m.Version,
				"name", m.Name,
				"pattern", q.config.Naming.Pattern,
				"error", err)
		}
	}

	// Store pointer to prevent mutation after registration
	migration := m
	q.migrations = append(q.migrations, &migration)

	return nil
}

// MustAdd is like Add but panics on error.
func (q *Queen) MustAdd(m M) {
	if err := q.Add(m); err != nil {
		panic(err)
	}
}

// Up applies all pending migrations. Equivalent to UpSteps(ctx, 0).
func (q *Queen) Up(ctx context.Context) error {
	return q.UpSteps(ctx, 0)
}

// UpSteps applies up to n pending migrations. If n <= 0, applies all.
func (q *Queen) UpSteps(ctx context.Context, n int) error {
	if q.driver == nil {
		return ErrNoDriver
	}

	if len(q.migrations) == 0 {
		return ErrNoMigrations
	}

	if err := q.driver.Init(ctx); err != nil {
		return err
	}

	unlock, err := q.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()

	if err := q.loadApplied(ctx); err != nil {
		return err
	}

	pending := q.getPending()
	if len(pending) == 0 {
		return nil
	}

	if n > 0 && n < len(pending) {
		pending = pending[:n]
	}

	for _, m := range pending {
		if err := q.applyMigration(ctx, m); err != nil {
			return newMigrationError(m.Version, m.Name, DirectionUp, q.getDriverName(), err)
		}
	}

	return nil
}

// Down rolls back the last n migrations. If n <= 0, rolls back only the last migration.
func (q *Queen) Down(ctx context.Context, n int) error {
	if n <= 0 {
		n = 1
	}

	if q.driver == nil {
		return ErrNoDriver
	}

	if err := q.driver.Init(ctx); err != nil {
		return err
	}

	unlock, err := q.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()

	if err := q.loadApplied(ctx); err != nil {
		return err
	}

	applied := q.getAppliedMigrations()
	if len(applied) == 0 {
		return nil
	}

	if n > len(applied) {
		n = len(applied)
	}

	toRollback := applied[:n]

	for _, m := range toRollback {
		if !m.HasRollback() {
			return newMigrationError(m.Version, m.Name, DirectionDown, q.getDriverName(), fmt.Errorf("no down migration defined"))
		}

		if err := q.rollbackMigration(ctx, m); err != nil {
			return newMigrationError(m.Version, m.Name, DirectionDown, q.getDriverName(), err)
		}
	}

	return nil
}

// Reset rolls back all applied migrations.
func (q *Queen) Reset(ctx context.Context) error {
	if q.driver == nil {
		return ErrNoDriver
	}

	if err := q.driver.Init(ctx); err != nil {
		return err
	}

	unlock, err := q.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()

	if err := q.loadApplied(ctx); err != nil {
		return err
	}

	applied := q.getAppliedMigrations()
	if len(applied) == 0 {
		return nil
	}

	// Don't call Down() to avoid double-locking
	for _, m := range applied {
		if !m.HasRollback() {
			return newMigrationError(m.Version, m.Name, DirectionDown, q.getDriverName(), fmt.Errorf("no down migration defined"))
		}

		if err := q.rollbackMigration(ctx, m); err != nil {
			return newMigrationError(m.Version, m.Name, DirectionDown, q.getDriverName(), err)
		}
	}

	return nil
}

// Status returns the status of all registered migrations.
func (q *Queen) Status(ctx context.Context) ([]MigrationStatus, error) {
	if q.driver == nil {
		return nil, ErrNoDriver
	}

	if err := q.driver.Init(ctx); err != nil {
		return nil, err
	}

	if err := q.loadApplied(ctx); err != nil {
		return nil, err
	}

	statuses := make([]MigrationStatus, len(q.migrations))
	for i, m := range q.migrations {
		status := MigrationStatus{
			Version:     m.Version,
			Name:        m.Name,
			Checksum:    m.Checksum(),
			HasRollback: m.HasRollback(),
			Destructive: m.IsDestructive(),
			Status:      StatusPending,
		}

		if applied, ok := q.applied[m.Version]; ok {
			status.Status = StatusApplied
			status.AppliedAt = &applied.AppliedAt

			// Check for checksum mismatch
			if applied.Checksum != m.Checksum() && m.Checksum() != noChecksumMarker {
				status.Status = StatusModified
			}
		}

		statuses[i] = status
	}

	return statuses, nil
}

// Validate checks for duplicate versions, invalid migrations, and checksum mismatches.
func (q *Queen) Validate(ctx context.Context) error {
	if len(q.migrations) == 0 {
		return ErrNoMigrations
	}

	seen := make(map[string]bool)
	for _, m := range q.migrations {
		if seen[m.Version] {
			return fmt.Errorf("%w: duplicate version %s", ErrVersionConflict, m.Version)
		}
		seen[m.Version] = true

		if err := m.Validate(); err != nil {
			return fmt.Errorf("invalid migration %s: %w", m.Version, err)
		}
	}

	if q.driver != nil {
		if err := q.driver.Init(ctx); err != nil {
			return err
		}

		if err := q.loadApplied(ctx); err != nil {
			return err
		}

		for _, m := range q.migrations {
			if applied, ok := q.applied[m.Version]; ok {
				if applied.Checksum != m.Checksum() && m.Checksum() != noChecksumMarker {
					q.logger.ErrorContext(ctx, "checksum mismatch detected",
						"version", m.Version,
						"name", m.Name,
						"expected_checksum", applied.Checksum,
						"actual_checksum", m.Checksum())
					return fmt.Errorf("%w: migration %s (expected %s, got %s)",
						ErrChecksumMismatch, m.Version, applied.Checksum, m.Checksum())
				}
			}
		}
	}

	return nil
}

// DryRun returns a migration execution plan without applying migrations.
// Direction can be DirectionUp (pending) or DirectionDown (applied).
func (q *Queen) DryRun(ctx context.Context, direction string, limit int) ([]MigrationPlan, error) {
	if q.driver == nil {
		return nil, ErrNoDriver
	}

	if direction != DirectionUp && direction != DirectionDown {
		return nil, fmt.Errorf("invalid direction: %s (must be 'up' or 'down')", direction)
	}

	if err := q.driver.Init(ctx); err != nil {
		return nil, err
	}

	if err := q.loadApplied(ctx); err != nil {
		return nil, err
	}

	var migrations []*Migration
	if direction == DirectionUp {
		migrations = q.getPending()
	} else {
		migrations = q.getAppliedMigrations()
	}

	if limit > 0 && limit < len(migrations) {
		migrations = migrations[:limit]
	}

	plans := make([]MigrationPlan, len(migrations))
	for i, m := range migrations {
		plans[i] = q.createMigrationPlan(m, direction)
	}

	return plans, nil
}

// Explain returns a detailed migration plan for a specific version.
func (q *Queen) Explain(ctx context.Context, version string) (*MigrationPlan, error) {
	if q.driver == nil {
		return nil, ErrNoDriver
	}

	if err := q.driver.Init(ctx); err != nil {
		return nil, err
	}

	if err := q.loadApplied(ctx); err != nil {
		return nil, err
	}

	var migration *Migration
	for _, m := range q.migrations {
		if m.Version == version {
			migration = m
			break
		}
	}

	if migration == nil {
		return nil, fmt.Errorf("migration not found: %s", version)
	}

	direction := DirectionUp
	if _, applied := q.applied[version]; applied {
		direction = DirectionDown
	}

	plan := q.createMigrationPlan(migration, direction)
	return &plan, nil
}

// Close releases database resources.
func (q *Queen) Close() error {
	if q.driver != nil {
		return q.driver.Close()
	}
	return nil
}

// Driver returns the underlying database driver.
func (q *Queen) Driver() Driver {
	return q.driver
}

// FindMigration returns a registered migration by version, or nil if not found.
func (q *Queen) FindMigration(version string) *Migration {
	for _, m := range q.migrations {
		if m.Version == version {
			return m
		}
	}
	return nil
}

// lock acquires a migration lock and returns an unlock function.
func (q *Queen) lock(ctx context.Context) (func(), error) {
	if q.config.SkipLock {
		return func() {}, nil
	}

	if err := q.driver.Lock(ctx, q.config.LockTimeout); err != nil {
		return nil, err
	}

	q.logger.InfoContext(ctx, "lock acquired", "table", q.config.TableName)

	return func() {
		_ = q.driver.Unlock(context.Background())
		q.logger.InfoContext(context.Background(), "lock released", "table", q.config.TableName)
	}, nil
}

// getDriverName returns the driver name for error context.
func (q *Queen) getDriverName() string {
	if q.driver == nil {
		return driverNameUnknown
	}

	driverType := fmt.Sprintf("%T", q.driver)

	// Extract driver name from package path: "*postgres.Driver" -> "postgres"
	if idx := strings.LastIndex(driverType, "."); idx != -1 {
		if idx2 := strings.LastIndex(driverType[:idx], "/"); idx2 != -1 {
			return driverType[idx2+1 : idx]
		}
		driverType = driverType[:idx]
		driverType = strings.TrimPrefix(driverType, "*")
		return driverType
	}

	return driverNameUnknown
}

func (q *Queen) loadApplied(ctx context.Context) error {
	applied, err := q.driver.GetApplied(ctx)
	if err != nil {
		return err
	}

	q.applied = make(map[string]*Applied)
	for i := range applied {
		q.applied[applied[i].Version] = &applied[i]
	}

	return nil
}

func (q *Queen) getPending() []*Migration {
	pending := make([]*Migration, 0)

	for _, m := range q.migrations {
		if _, applied := q.applied[m.Version]; !applied {
			pending = append(pending, m)
		}
	}

	sort.Slice(pending, func(i, j int) bool {
		return naturalsort.Compare(pending[i].Version, pending[j].Version) < 0
	})

	return pending
}

func (q *Queen) getAppliedMigrations() []*Migration {
	applied := make([]*Migration, 0)

	for _, m := range q.migrations {
		if _, ok := q.applied[m.Version]; ok {
			applied = append(applied, m)
		}
	}

	sort.Slice(applied, func(i, j int) bool {
		return naturalsort.Compare(applied[i].Version, applied[j].Version) > 0
	})

	return applied
}

// getIsolationLevel returns the effective isolation level for a migration.
// Priority: Migration.IsolationLevel -> Config.IsolationLevel -> LevelDefault
func (q *Queen) getIsolationLevel(m *Migration) sql.IsolationLevel {
	if m.IsolationLevel != sql.LevelDefault {
		return m.IsolationLevel
	}
	if q.config.IsolationLevel != sql.LevelDefault {
		return q.config.IsolationLevel
	}
	return sql.LevelDefault
}

func (q *Queen) collectMetadata(action string, status string, durationMS int64, err error) *MigrationMetadata {
	meta := &MigrationMetadata{
		Action:     action,
		Status:     status,
		DurationMS: durationMS,
	}

	if currentUser, userErr := user.Current(); userErr == nil {
		meta.AppliedBy = currentUser.Username
	}

	if hostname, hostErr := os.Hostname(); hostErr == nil {
		meta.Hostname = hostname
	}

	meta.Environment = os.Getenv("QUEEN_ENV")

	if err != nil {
		meta.ErrorMessage = err.Error()
	}

	return meta
}

func (q *Queen) applyMigration(ctx context.Context, m *Migration) error {
	start := time.Now()
	isolationLevel := q.getIsolationLevel(m)

	logArgs := []any{
		"version", m.Version,
		"name", m.Name,
		"direction", DirectionUp,
	}
	if isolationLevel != sql.LevelDefault {
		logArgs = append(logArgs, "isolation_level", isolationLevel.String())
	}
	q.logger.InfoContext(ctx, "migration started", logArgs...)

	err := q.driver.Exec(ctx, isolationLevel, func(tx *sql.Tx) error {
		return m.executeUp(ctx, tx)
	})

	durationMS := time.Since(start).Milliseconds()

	if err != nil {
		q.logger.ErrorContext(ctx, "migration failed",
			"version", m.Version,
			"name", m.Name,
			"direction", DirectionUp,
			"error", err,
			"duration_ms", durationMS)

		return err
	}

	meta := q.collectMetadata("apply", "success", durationMS, nil)

	if err := q.driver.Record(ctx, m, meta); err != nil {
		q.logger.ErrorContext(ctx, "migration record failed",
			"version", m.Version,
			"name", m.Name,
			"direction", DirectionUp,
			"error", err,
			"duration_ms", durationMS)
		return err
	}

	q.applied[m.Version] = &Applied{
		Version:      m.Version,
		Name:         m.Name,
		AppliedAt:    time.Now(),
		Checksum:     m.Checksum(),
		AppliedBy:    meta.AppliedBy,
		DurationMS:   meta.DurationMS,
		Hostname:     meta.Hostname,
		Environment:  meta.Environment,
		Action:       meta.Action,
		Status:       meta.Status,
		ErrorMessage: meta.ErrorMessage,
	}

	q.logger.InfoContext(ctx, "migration completed",
		"version", m.Version,
		"name", m.Name,
		"direction", DirectionUp,
		"duration_ms", durationMS)

	return nil
}

func (q *Queen) rollbackMigration(ctx context.Context, m *Migration) error {
	start := time.Now()
	isolationLevel := q.getIsolationLevel(m)

	logArgs := []any{
		"version", m.Version,
		"name", m.Name,
		"direction", DirectionDown,
	}
	if isolationLevel != sql.LevelDefault {
		logArgs = append(logArgs, "isolation_level", isolationLevel.String())
	}
	q.logger.InfoContext(ctx, "migration started", logArgs...)

	err := q.driver.Exec(ctx, isolationLevel, func(tx *sql.Tx) error {
		return m.executeDown(ctx, tx)
	})

	durationMS := time.Since(start).Milliseconds()

	if err != nil {
		meta := q.collectMetadata("rollback", "failed", durationMS, err)

		q.logger.ErrorContext(ctx, "migration failed",
			"version", m.Version,
			"name", m.Name,
			"direction", DirectionDown,
			"error", err,
			"duration_ms", durationMS,
			"applied_by", meta.AppliedBy,
			"hostname", meta.Hostname)
		return err
	}

	if err := q.driver.Remove(ctx, m.Version); err != nil {
		q.logger.ErrorContext(ctx, "migration remove failed",
			"version", m.Version,
			"name", m.Name,
			"direction", DirectionDown,
			"error", err,
			"duration_ms", durationMS)
		return err
	}

	delete(q.applied, m.Version)

	meta := q.collectMetadata("rollback", "success", durationMS, nil)

	q.logger.InfoContext(ctx, "migration completed",
		"version", m.Version,
		"name", m.Name,
		"direction", DirectionDown,
		"duration_ms", durationMS,
		"applied_by", meta.AppliedBy,
		"hostname", meta.Hostname)

	return nil
}

func (q *Queen) createMigrationPlan(m *Migration, direction string) MigrationPlan {
	plan := MigrationPlan{
		Version:       m.Version,
		Name:          m.Name,
		Direction:     direction,
		HasRollback:   m.HasRollback(),
		IsDestructive: false,
		Checksum:      m.Checksum(),
		Warnings:      make([]string, 0),
	}

	if applied, ok := q.applied[m.Version]; ok {
		plan.Status = "applied"
		if applied.Checksum != m.Checksum() && m.Checksum() != noChecksumMarker {
			plan.Status = "modified"
			plan.Warnings = append(plan.Warnings, "Checksum mismatch - migration has been modified after being applied")
			q.logger.WarnContext(context.Background(), "checksum mismatch in migration plan",
				"version", m.Version,
				"name", m.Name,
				"expected_checksum", applied.Checksum,
				"actual_checksum", m.Checksum())
		}
	} else {
		plan.Status = "pending"
	}

	var sql string
	hasSQL := false
	hasFunc := false

	if direction == DirectionUp {
		if m.UpSQL != "" {
			hasSQL = true
			sql = m.UpSQL
		}
		if m.UpFunc != nil {
			hasFunc = true
		}
	} else {
		if m.DownSQL != "" {
			hasSQL = true
			sql = m.DownSQL
		}
		if m.DownFunc != nil {
			hasFunc = true
		}
		plan.IsDestructive = m.IsDestructive()
	}

	if hasSQL && hasFunc {
		plan.Type = MigrationTypeMixed
	} else if hasSQL {
		plan.Type = MigrationTypeSQL
	} else {
		plan.Type = MigrationTypeGoFunc
	}

	plan.SQL = sql

	if !plan.HasRollback {
		plan.Warnings = append(plan.Warnings, "No rollback defined")
	}

	if plan.Type == MigrationTypeGoFunc || plan.Type == MigrationTypeMixed {
		if m.ManualChecksum == "" {
			plan.Warnings = append(plan.Warnings, "Go function without manual checksum")
		}
	}

	if plan.IsDestructive && direction == DirectionDown {
		plan.Warnings = append(plan.Warnings, "Destructive operation")
	}

	return plan
}
