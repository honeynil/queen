package cli

import (
	"context"
	"fmt"

	"github.com/honeynil/queen"
	"github.com/spf13/cobra"
)

func (app *App) gotoCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "goto VERSION",
		Short: "Migrate to specific version (up or down)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			targetVersion := args[0]

			ctx := context.Background()
			q, err := app.setupQueen(ctx)
			if err != nil {
				return err
			}
			defer func() { _ = q.Close() }()

			if targetVersion == "latest" {
				return q.Up(ctx)
			}

			statuses, err := q.Status(ctx)
			if err != nil {
				return fmt.Errorf("failed to get migration status: %w", err)
			}

			targetIndex := -1
			var targetMigration *queen.MigrationStatus
			for i, s := range statuses {
				if s.Version == targetVersion {
					targetIndex = i
					targetMigration = &s
					break
				}
			}

			if targetMigration == nil {
				return fmt.Errorf("migration version not found: %s", targetVersion)
			}

			if targetMigration.Status == queen.StatusPending {
				return app.migrateUpTo(ctx, q, statuses, targetIndex)
			} else {
				return app.migrateDownTo(ctx, q, statuses, targetIndex)
			}
		},
	}

	return cmd
}

func (app *App) migrateUpTo(ctx context.Context, q *queen.Queen, statuses []queen.MigrationStatus, targetIndex int) error {
	stepsToApply := 0
	for i := 0; i <= targetIndex; i++ {
		if statuses[i].Status == queen.StatusPending {
			stepsToApply++
		}
	}

	if stepsToApply == 0 {
		fmt.Println("Already at or past the target version")
		return nil
	}

	if !app.config.Yes {
		fmt.Printf("Will apply %d migration(s):\n", stepsToApply)
		for i := 0; i <= targetIndex; i++ {
			if statuses[i].Status == queen.StatusPending {
				fmt.Printf("  ↑ %s - %s\n", statuses[i].Version, statuses[i].Name)
			}
		}
		fmt.Println()

		if !confirm("Proceed with migration?") {
			fmt.Println("Canceled")
			return nil
		}
	}

	return q.UpSteps(ctx, stepsToApply)
}

func (app *App) migrateDownTo(ctx context.Context, q *queen.Queen, statuses []queen.MigrationStatus, targetIndex int) error {
	stepsToRollback := 0
	hasDestructive := false
	for i := targetIndex + 1; i < len(statuses); i++ {
		if statuses[i].Status == queen.StatusApplied {
			stepsToRollback++
			if statuses[i].Destructive {
				hasDestructive = true
			}
		}
	}

	if stepsToRollback == 0 {
		fmt.Println("Already at the target version")
		return nil
	}

	if !app.config.Yes {
		fmt.Printf("Will rollback %d migration(s):\n", stepsToRollback)
		for i := len(statuses) - 1; i > targetIndex; i-- {
			if statuses[i].Status == queen.StatusApplied {
				marker := "↓"
				if statuses[i].Destructive {
					marker = "WARNING:"
				}
				fmt.Printf("  %s %s - %s\n", marker, statuses[i].Version, statuses[i].Name)
			}
		}
		if hasDestructive {
			fmt.Println("\nWARNING: Some migrations contain destructive operations")
		}
		fmt.Println()

		if !confirm("Proceed with rollback?") {
			fmt.Println("Canceled")
			return nil
		}
	}

	return q.Down(ctx, stepsToRollback)
}
