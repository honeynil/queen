package cli

import (
	"testing"

	"github.com/honeynil/queen"
)

func TestUpCmd(t *testing.T) {
	t.Parallel()

	t.Run("creates command with correct flags", func(t *testing.T) {
		t.Parallel()

		app := &App{
			registerFunc: func(q *queen.Queen) {},
			config:       &Config{},
		}

		cmd := app.upCmd()

		if cmd.Name() != "up" {
			t.Errorf("command name = %q, want %q", cmd.Name(), "up")
		}

		if !cmd.Flags().HasAvailableFlags() {
			t.Error("up command should have flags")
		}

		stepsFlag := cmd.Flags().Lookup("steps")
		if stepsFlag == nil {
			t.Error("up command should have --steps flag")
		}

		toFlag := cmd.Flags().Lookup("to")
		if toFlag == nil {
			t.Error("up command should have --to flag")
		}
	})
}

func TestDownCmd(t *testing.T) {
	t.Parallel()

	t.Run("creates command with correct flags", func(t *testing.T) {
		t.Parallel()

		app := &App{
			registerFunc: func(q *queen.Queen) {},
			config:       &Config{},
		}

		cmd := app.downCmd()

		if cmd.Name() != "down" {
			t.Errorf("command name = %q, want %q", cmd.Name(), "down")
		}

		if !cmd.Flags().HasAvailableFlags() {
			t.Error("down command should have flags")
		}

		stepsFlag := cmd.Flags().Lookup("steps")
		if stepsFlag == nil {
			t.Error("down command should have --steps flag")
		}

		toFlag := cmd.Flags().Lookup("to")
		if toFlag == nil {
			t.Error("down command should have --to flag")
		}
	})
}

func TestResetCmd(t *testing.T) {
	t.Parallel()

	app := &App{
		registerFunc: func(q *queen.Queen) {},
		config:       &Config{},
	}

	cmd := app.resetCmd()

	if cmd.Name() != "reset" {
		t.Errorf("command name = %q, want %q", cmd.Name(), "reset")
	}
}

func TestValidateCmd(t *testing.T) {
	t.Parallel()

	app := &App{
		registerFunc: func(q *queen.Queen) {},
		config:       &Config{},
	}

	cmd := app.validateCmd()

	if cmd.Name() != "validate" {
		t.Errorf("command name = %q, want %q", cmd.Name(), "validate")
	}
}

func TestVersionCmd(t *testing.T) {
	t.Parallel()

	app := &App{
		registerFunc: func(q *queen.Queen) {},
		config:       &Config{},
	}

	cmd := app.versionCmd()

	if cmd.Name() != "version" {
		t.Errorf("command name = %q, want %q", cmd.Name(), "version")
	}
}

func TestPlanCmd(t *testing.T) {
	t.Parallel()

	app := &App{
		registerFunc: func(q *queen.Queen) {},
		config:       &Config{},
	}

	cmd := app.planCmd()

	if cmd.Name() != "plan" {
		t.Errorf("command name = %q, want %q", cmd.Name(), "plan")
	}
}

func TestLogCmd(t *testing.T) {
	t.Parallel()

	app := &App{
		registerFunc: func(q *queen.Queen) {},
		config:       &Config{},
	}

	cmd := app.logCmd()

	if cmd.Name() != "log" {
		t.Errorf("command name = %q, want %q", cmd.Name(), "log")
	}

	lastFlag := cmd.Flags().Lookup("last")
	if lastFlag == nil {
		t.Error("log command should have --last flag")
	}
}

func TestExplainCmd(t *testing.T) {
	t.Parallel()

	app := &App{
		registerFunc: func(q *queen.Queen) {},
		config:       &Config{},
	}

	cmd := app.explainCmd()

	if cmd.Name() != "explain" {
		t.Errorf("command name = %q, want %q", cmd.Name(), "explain")
	}
}

func TestGotoCmd(t *testing.T) {
	t.Parallel()

	app := &App{
		registerFunc: func(q *queen.Queen) {},
		config:       &Config{},
	}

	cmd := app.gotoCmd()

	if cmd.Name() != "goto" {
		t.Errorf("command name = %q, want %q", cmd.Name(), "goto")
	}
}

func TestGapCmd(t *testing.T) {
	t.Parallel()

	app := &App{
		registerFunc: func(q *queen.Queen) {},
		config:       &Config{},
	}

	cmd := app.gapCmd()

	if cmd.Name() != "gap" {
		t.Errorf("command name = %q, want %q", cmd.Name(), "gap")
	}
}

func TestDiffCmd(t *testing.T) {
	t.Parallel()

	app := &App{
		registerFunc: func(q *queen.Queen) {},
		config:       &Config{},
	}

	cmd := app.diffCmd()

	if cmd.Name() != "diff" {
		t.Errorf("command name = %q, want %q", cmd.Name(), "diff")
	}
}

func TestDoctorCmd(t *testing.T) {
	t.Parallel()

	app := &App{
		registerFunc: func(q *queen.Queen) {},
		config:       &Config{},
	}

	cmd := app.doctorCmd()

	if cmd.Name() != "doctor" {
		t.Errorf("command name = %q, want %q", cmd.Name(), "doctor")
	}
}

func TestCheckCmd(t *testing.T) {
	t.Parallel()

	app := &App{
		registerFunc: func(q *queen.Queen) {},
		config:       &Config{},
	}

	cmd := app.checkCmd()

	if cmd.Name() != "check" {
		t.Errorf("command name = %q, want %q", cmd.Name(), "check")
	}
}

func TestInitCmd(t *testing.T) {
	t.Parallel()

	app := &App{
		registerFunc: func(q *queen.Queen) {},
		config:       &Config{},
	}

	cmd := app.initCmd()

	if cmd.Name() != "init" {
		t.Errorf("command name = %q, want %q", cmd.Name(), "init")
	}
}

func TestSquashCmd(t *testing.T) {
	t.Parallel()

	app := &App{
		registerFunc: func(q *queen.Queen) {},
		config:       &Config{},
	}

	cmd := app.squashCmd()

	if cmd.Name() != "squash" {
		t.Errorf("command name = %q, want %q", cmd.Name(), "squash")
	}
}

func TestBaselineCmd(t *testing.T) {
	t.Parallel()

	app := &App{
		registerFunc: func(q *queen.Queen) {},
		config:       &Config{},
	}

	cmd := app.baselineCmd()

	if cmd.Name() != "baseline" {
		t.Errorf("command name = %q, want %q", cmd.Name(), "baseline")
	}
}

func TestImportCmd(t *testing.T) {
	t.Parallel()

	app := &App{
		registerFunc: func(q *queen.Queen) {},
		config:       &Config{},
	}

	cmd := app.importCmd()

	if cmd.Name() != "import" {
		t.Errorf("command name = %q, want %q", cmd.Name(), "import")
	}
}

func TestTuiCmd(t *testing.T) {
	t.Parallel()

	app := &App{
		registerFunc: func(q *queen.Queen) {},
		config:       &Config{},
	}

	cmd := app.tuiCmd()

	if cmd.Name() != "tui" {
		t.Errorf("command name = %q, want %q", cmd.Name(), "tui")
	}
}

func TestStatusCmd(t *testing.T) {
	t.Parallel()

	app := &App{
		registerFunc: func(q *queen.Queen) {},
		config:       &Config{},
	}

	cmd := app.statusCmd()

	if cmd.Name() != "status" {
		t.Errorf("command name = %q, want %q", cmd.Name(), "status")
	}

	if cmd.Short != "Show status of all registered migrations" {
		t.Errorf("short description = %q, want %q", cmd.Short, "Show status of all registered migrations")
	}
}
