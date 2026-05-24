// tapdemo is a focused development preview for the low-level tap live renderer.
// The product demo is tests/tuidemo, where the same migration story appears
// inside the main TUI right-column tap inspector.
//
// Run with:
//
//	go run ./tests/tapdemo
//
// Press q or ctrl+c to exit.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/yaop-labs/queen"
	"github.com/yaop-labs/queen/cli/tui/live"
	"github.com/yaop-labs/queen/drivers/sqlite"
	"github.com/yaop-labs/queen/tap"
	"github.com/yaop-labs/queen/tests/demodata"
)

func main() {
	ctx := context.Background()

	err := live.Run(ctx, func(runCtx context.Context, sink tap.Sink) error {
		db, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return err
		}
		defer func() { _ = db.Close() }()

		cfg := tap.DefaultAnalyzerConfig()
		cfg.SlowThreshold = 100 * time.Millisecond
		q := queen.New(sqlite.New(db), queen.WithTap(tap.NewAnalyzerSink(sink, cfg)))
		defer func() { _ = q.Close() }()

		demodata.Register(q, demodata.TapLiveOptions())

		time.Sleep(300 * time.Millisecond)
		return q.Up(runCtx)
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "demo error: %v\n", err)
		os.Exit(1)
	}
}
