package tap

import (
	"context"
	"database/sql"
	"time"
)

// ObservedTx records Exec/Query calls made from a Go-function migration.
type ObservedTx struct {
	tx    *sql.Tx
	scope *scope
}

// ObserveTx returns a transaction wrapper bound to the tap scope on ctx.
func ObserveTx(ctx context.Context, tx *sql.Tx) *ObservedTx {
	return &ObservedTx{tx: tx, scope: fromContext(ctx)}
}

// Tx returns an ObservedTx bound to ctx and tx.
//
// Deprecated: use ObserveTx. The migration already runs inside a transaction;
// this helper only observes SQL calls for tap.
func Tx(ctx context.Context, tx *sql.Tx) *ObservedTx {
	return ObserveTx(ctx, tx)
}

// TxWrapper is kept as a source-compatible alias for the old wrapper name.
//
// Deprecated: use ObservedTx.
type TxWrapper = ObservedTx

// Unwrap returns the underlying *sql.Tx for operations not covered by
// the wrapper (e.g. Prepare, Stmt).
func (w *ObservedTx) Unwrap() *sql.Tx { return w.tx }

func (w *ObservedTx) emit(start time.Time, query string, args []any, rows int64, err error) {
	if w.scope == nil {
		return
	}
	e := Event{
		Kind:         KindExec,
		Version:      w.scope.version,
		Name:         w.scope.name,
		Direction:    w.scope.direction,
		Index:        w.scope.statementIndex(),
		StartedAt:    start,
		Duration:     time.Since(start),
		SQL:          query,
		Args:         cloneArgs(args),
		RowsAffected: rows,
	}
	if err != nil {
		e.Error = err.Error()
	}
	w.scope.sink.Emit(e)
}

func (w *ObservedTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	start := time.Now()
	res, err := w.tx.ExecContext(ctx, query, args...)
	var rows int64
	if res != nil {
		if n, rerr := res.RowsAffected(); rerr == nil {
			rows = n
		}
	}
	w.emit(start, query, args, rows, err)
	return res, err
}

// QueryContext emits the query with zero rows affected; database/sql does not expose that count.
func (w *ObservedTx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	start := time.Now()
	rows, err := w.tx.QueryContext(ctx, query, args...)
	w.emit(start, query, args, 0, err)
	return rows, err
}

// QueryRowContext mirrors *sql.Tx.QueryRowContext and taps the call when Scan
// is invoked, so deferred QueryRow errors are captured accurately.
func (w *ObservedTx) QueryRowContext(ctx context.Context, query string, args ...any) *RowWrapper {
	start := time.Now()
	row := w.tx.QueryRowContext(ctx, query, args...)
	return &RowWrapper{row: row, tx: w, start: start, query: query, args: cloneArgs(args)}
}

// PrepareContext mirrors *sql.Tx.PrepareContext and returns a tapped statement.
func (w *ObservedTx) PrepareContext(ctx context.Context, query string) (*StmtWrapper, error) {
	stmt, err := w.tx.PrepareContext(ctx, query)
	if err != nil {
		w.emit(time.Now(), query, nil, 0, err)
		return nil, err
	}
	return &StmtWrapper{stmt: stmt, tx: w, query: query}, nil
}

// RowWrapper wraps *sql.Row so Scan can emit the final QueryRow error.
type RowWrapper struct {
	row   *sql.Row
	tx    *ObservedTx
	start time.Time
	query string
	args  []any
}

func (r *RowWrapper) Scan(dest ...any) error {
	err := r.row.Scan(dest...)
	r.tx.emit(r.start, r.query, r.args, 0, err)
	return err
}

// StmtWrapper wraps *sql.Stmt and emits tap events for statement execution.
type StmtWrapper struct {
	stmt  *sql.Stmt
	tx    *ObservedTx
	query string
}

func (s *StmtWrapper) Close() error { return s.stmt.Close() }

func (s *StmtWrapper) Unwrap() *sql.Stmt { return s.stmt }

func (s *StmtWrapper) ExecContext(ctx context.Context, args ...any) (sql.Result, error) {
	start := time.Now()
	res, err := s.stmt.ExecContext(ctx, args...)
	var rows int64
	if res != nil {
		if n, rerr := res.RowsAffected(); rerr == nil {
			rows = n
		}
	}
	s.tx.emit(start, s.query, args, rows, err)
	return res, err
}

func (s *StmtWrapper) QueryContext(ctx context.Context, args ...any) (*sql.Rows, error) {
	start := time.Now()
	rows, err := s.stmt.QueryContext(ctx, args...)
	s.tx.emit(start, s.query, args, 0, err)
	return rows, err
}

func (s *StmtWrapper) QueryRowContext(ctx context.Context, args ...any) *RowWrapper {
	start := time.Now()
	row := s.stmt.QueryRowContext(ctx, args...)
	return &RowWrapper{row: row, tx: s.tx, start: start, query: s.query, args: cloneArgs(args)}
}

func cloneArgs(args []any) []any {
	if len(args) == 0 {
		return nil
	}
	out := make([]any, len(args))
	copy(out, args)
	return out
}
