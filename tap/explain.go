package tap

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// ExplainMode selects between a planner-only EXPLAIN and an execution-backed
// ANALYZE where the connected database supports it.
type ExplainMode string

const (
	ExplainOnly    ExplainMode = "EXPLAIN"
	ExplainAnalyze ExplainMode = "EXPLAIN ANALYZE"
)

// ExplainResult is the textual plan returned by the database.
type ExplainResult struct {
	Mode     ExplainMode
	Plan     string
	Duration time.Duration
}

// RunExplain executes EXPLAIN for query. It keeps the API deliberately small:
// callers pass the driver name Queen already knows, the original query, and
// captured args.
func RunExplain(ctx context.Context, db *sql.DB, driverName string, mode ExplainMode, query string, args []any) (*ExplainResult, error) {
	if db == nil {
		return nil, fmt.Errorf("database handle is not available")
	}
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, fmt.Errorf("query is empty")
	}

	explainQuery, execArgs := explainQuery(driverName, mode, query, args)
	start := time.Now()
	rows, err := db.QueryContext(ctx, explainQuery, execArgs...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	plan, err := scanExplainRows(rows)
	if err != nil {
		return nil, err
	}
	return &ExplainResult{
		Mode:     mode,
		Plan:     plan,
		Duration: time.Since(start),
	}, nil
}

func explainQuery(driverName string, mode ExplainMode, query string, args []any) (string, []any) {
	name := strings.ToLower(driverName)
	analyze := mode == ExplainAnalyze
	switch {
	case strings.Contains(name, "sqlite"):
		return "EXPLAIN QUERY PLAN " + query, args
	case strings.Contains(name, "postgres"), strings.Contains(name, "cockroach"):
		if analyze {
			return "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) " + query, args
		}
		return "EXPLAIN " + query, args
	case strings.Contains(name, "mysql"), strings.Contains(name, "tidb"):
		if analyze {
			return "EXPLAIN ANALYZE " + query, args
		}
		return "EXPLAIN " + query, args
	default:
		return "EXPLAIN " + query, args
	}
}

func scanExplainRows(rows *sql.Rows) (string, error) {
	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}
	if len(cols) == 0 {
		return "", nil
	}

	var lines []string
	for rows.Next() {
		values := make([]sql.NullString, len(cols))
		dest := make([]any, len(cols))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return "", err
		}

		parts := make([]string, 0, len(cols))
		for i, v := range values {
			if !v.Valid {
				continue
			}
			if len(cols) == 1 {
				parts = append(parts, v.String)
			} else {
				parts = append(parts, fmt.Sprintf("%s=%s", cols[i], v.String))
			}
		}
		if len(parts) > 0 {
			lines = append(lines, strings.Join(parts, "  "))
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return strings.Join(lines, "\n"), nil
}
