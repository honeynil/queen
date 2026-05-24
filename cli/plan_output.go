package cli

import (
	"fmt"
	"strings"

	"github.com/yaop-labs/queen"
)

func (app *App) outputPlanTable(plans []queen.MigrationPlan, direction string) error {
	directionLabel := strings.ToUpper(direction)
	fmt.Printf("Migration Plan (%s)\n", directionLabel)
	outputRule("━", 60)

	if len(plans) == 0 {
		if direction == "up" {
			fmt.Println("No pending migrations")
		} else {
			fmt.Println("No applied migrations to roll back")
		}
		return nil
	}

	table := outputTable("Version", "Name", "Type", "Status", "Warnings")

	var withRollback, withWarnings int

	for _, plan := range plans {
		arrow := "→"
		if direction == queen.DirectionDown {
			arrow = "←"
		}

		warnings := ""
		if len(plan.Warnings) > 0 {
			withWarnings++
			warnings = "WARNING: " + strings.Join(plan.Warnings, "; ")
		}

		if plan.HasRollback {
			withRollback++
		}

		row := []string{
			arrow + " " + plan.Version,
			plan.Name,
			string(plan.Type),
			plan.Status,
			warnings,
		}

		if err := table.Append(row); err != nil {
			return err
		}
	}

	if err := table.Render(); err != nil {
		return err
	}

	outputBlank()
	if direction == "up" {
		fmt.Printf("%d migration(s) will be applied\n", len(plans))
	} else {
		fmt.Printf("%d migration(s) will be rolled back\n", len(plans))
	}

	if withRollback < len(plans) && direction == "up" {
		outputWarning("%d migration(s) without rollback", len(plans)-withRollback)
	}

	if withWarnings > 0 {
		outputWarning("%d migration(s) with warnings", withWarnings)
	}

	return nil
}

func (app *App) outputPlanJSON(plans []queen.MigrationPlan, direction string) error {
	var withRollback, withWarnings int

	for _, plan := range plans {
		if plan.HasRollback {
			withRollback++
		}
		if len(plan.Warnings) > 0 {
			withWarnings++
		}
	}

	output := struct {
		Direction string                `json:"direction"`
		Plans     []queen.MigrationPlan `json:"plans"`
		Summary   struct {
			Total        int `json:"total"`
			WithRollback int `json:"with_rollback"`
			WithWarnings int `json:"with_warnings"`
		} `json:"summary"`
	}{
		Direction: direction,
		Plans:     plans,
	}

	output.Summary.Total = len(plans)
	output.Summary.WithRollback = withRollback
	output.Summary.WithWarnings = withWarnings

	return outputJSON(output)
}
