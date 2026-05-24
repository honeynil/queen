package cli

import (
	"testing"

	"github.com/yaop-labs/queen"
)

func TestWithQueenOptionsRestoresPreviousOptions(t *testing.T) {
	baseOpt := func(*queen.Queen) {}
	tempOpt := func(*queen.Queen) {}

	app := &App{queenOpts: []queen.Option{baseOpt}}
	restore := app.withQueenOptions(tempOpt)

	if got := len(app.queenOpts); got != 2 {
		t.Fatalf("len(app.queenOpts)=%d, want 2", got)
	}

	restore()

	if got := len(app.queenOpts); got != 1 {
		t.Fatalf("len(app.queenOpts) after restore=%d, want 1", got)
	}
}

func TestWithQueenOptionsNoopWhenEmpty(t *testing.T) {
	app := &App{}
	restore := app.withQueenOptions()
	restore()

	if len(app.queenOpts) != 0 {
		t.Fatalf("len(app.queenOpts)=%d, want 0", len(app.queenOpts))
	}
}
