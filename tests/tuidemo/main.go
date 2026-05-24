// tuidemo launches the main Queen TUI against an in-memory SQLite database.
// It uses the shared demo migration story with two intentional numbering gaps,
// so the Migrations tab, Gaps tab, and integrated tap inspector all have useful
// content in one cockpit.
//
// Run with:
//
//	go run ./tests/tuidemo
//
// Try: enter on migration 005 to watch the tap stream open in the right column.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	tea "github.com/charmbracelet/bubbletea"
	_ "github.com/mattn/go-sqlite3"
	"github.com/yaop-labs/queen"
	"github.com/yaop-labs/queen/cli/tui"
	"github.com/yaop-labs/queen/drivers/sqlite"
	"github.com/yaop-labs/queen/tests/demodata"
)

func main() {
	ctx := context.Background()

	db, err := sql.Open("sqlite3", "file:queen_tuidemo?mode=memory&cache=shared")
	if err != nil {
		fmt.Fprintf(os.Stderr, "open sqlite: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)

	q := queen.New(sqlite.New(db))
	defer func() { _ = q.Close() }()

	demodata.Register(q, demodata.MainTUIOptions())

	// Seed the cockpit with an interesting state: 001, 002 and 004 are applied.
	// Version 003 is intentionally absent, so Gaps can show why this release
	// needs attention while migration 005 remains ready for the integrated tap.
	if err := q.UpSteps(ctx, 3); err != nil {
		fmt.Fprintf(os.Stderr, "seed migrations: %v\n", err)
		os.Exit(1)
	}

	p := tea.NewProgram(
		tui.NewModel(q, ctx),
		tea.WithAltScreen(),
		tea.WithMouseCellMotion(),
	)

	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "tui error: %v\n", err)
		os.Exit(1)
	}
}
