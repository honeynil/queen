package queen

import (
	"context"
	"database/sql"
	"time"

	"github.com/yaop-labs/queen/tap"
)

func (q *Queen) emitStart(m *Migration, dir tap.Direction, startedAt time.Time) {
	if q.tap == nil {
		return
	}
	q.tap.Emit(tap.Event{
		Kind:      tap.KindStart,
		Version:   m.Version,
		Name:      m.Name,
		Direction: dir,
		StartedAt: startedAt,
	})
}

func (q *Queen) emitEnd(m *Migration, dir tap.Direction, startedAt time.Time, dur time.Duration, err error) {
	if q.tap == nil {
		return
	}
	e := tap.Event{
		Kind:      tap.KindEnd,
		Version:   m.Version,
		Name:      m.Name,
		Direction: dir,
		StartedAt: startedAt,
		Duration:  dur,
	}
	if err != nil {
		e.Error = err.Error()
	}
	q.tap.Emit(e)
}

func (q *Queen) emitTx(m *Migration, dir tap.Direction, kind tap.Kind, startedAt time.Time, dur time.Duration, err error) {
	if q.tap == nil {
		return
	}
	e := tap.Event{
		Kind:      kind,
		Version:   m.Version,
		Name:      m.Name,
		Direction: dir,
		StartedAt: startedAt,
		Duration:  dur,
	}
	if err != nil {
		e.Error = err.Error()
	}
	q.tap.Emit(e)
}

func (q *Queen) emitExec(m *Migration, dir tap.Direction, startedAt time.Time, dur time.Duration, query string, rows int64, err error) {
	if q.tap == nil {
		return
	}
	e := tap.Event{
		Kind:         tap.KindExec,
		Version:      m.Version,
		Name:         m.Name,
		Direction:    dir,
		Index:        1,
		StartedAt:    startedAt,
		Duration:     dur,
		SQL:          query,
		RowsAffected: rows,
	}
	if err != nil {
		e.Error = err.Error()
	}
	q.tap.Emit(e)
}

// Mixed migrations keep schema SQL before Go data work.
func (q *Queen) runUp(ctx context.Context, tx *sql.Tx, m *Migration) error {
	if m.UpSQL == "" && m.UpFunc == nil {
		return ErrInvalidMigration
	}

	if m.UpSQL != "" {
		if err := q.tappedExec(ctx, tx, m, tap.DirectionUp, m.UpSQL); err != nil {
			return err
		}
	}
	if m.UpFunc != nil {
		scoped := tap.WithScope(ctx, q.tap, m.Version, m.Name, tap.DirectionUp)
		return m.UpFunc(scoped, tx)
	}
	return nil
}

// Rollback lets custom cleanup run before schema SQL removes objects.
func (q *Queen) runDown(ctx context.Context, tx *sql.Tx, m *Migration) error {
	if m.DownSQL == "" && m.DownFunc == nil {
		return ErrInvalidMigration
	}

	if m.DownFunc != nil {
		scoped := tap.WithScope(ctx, q.tap, m.Version, m.Name, tap.DirectionDown)
		if err := m.DownFunc(scoped, tx); err != nil {
			return err
		}
	}
	if m.DownSQL != "" {
		return q.tappedExec(ctx, tx, m, tap.DirectionDown, m.DownSQL)
	}
	return nil
}

func (q *Queen) tappedExec(ctx context.Context, tx *sql.Tx, m *Migration, dir tap.Direction, query string) error {
	start := time.Now()
	res, err := tx.ExecContext(ctx, query)
	dur := time.Since(start)

	var rows int64
	if res != nil {
		if n, rerr := res.RowsAffected(); rerr == nil {
			rows = n
		}
	}
	q.emitExec(m, dir, start, dur, query, rows, err)
	return err
}
