package cli

import (
	"context"
	"fmt"

	"github.com/honeynil/queen"
	"github.com/spf13/cobra"
)

func (app *App) downCmd() *cobra.Command {
	var (
		steps int
		to    string
	)

	cmd := &cobra.Command{
		Use:   "down",
		Short: "Rollback migrations",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			if steps > 0 && to != "" {
				return fmt.Errorf("cannot use both --steps and --to")
			}

			if steps == 0 && to == "" {
				steps = 1
			}

			operation := fmt.Sprintf("rollback %d migration(s)", steps)
			if to != "" {
				operation = fmt.Sprintf("rollback to version %s", to)
			}
			if err := app.checkConfirmation(operation); err != nil {
				return err
			}

			q, err := app.setupQueen(ctx)
			if err != nil {
				return err
			}
			defer func() { _ = q.Close() }()

			if to != "" {
				return migrateDownToVersion(ctx, q, to, app.config.Yes)
			}

			if err := q.Down(ctx, steps); err != nil {
				return fmt.Errorf("failed to rollback migrations: %w", err)
			}

			fmt.Printf("Rolled back %d migration(s)\n", steps)
			return nil
		},
	}

	cmd.Flags().IntVar(&steps, "steps", 0, "Number of migrations to rollback (default: 1)")
	cmd.Flags().StringVar(&to, "to", "", "Rollback to specific version (exclusive - keeps the target)")

	return cmd
}

func migrateDownToVersion(ctx context.Context, q *queen.Queen, targetVersion string, autoYes bool) error {
	statuses, err := q.Status(ctx)
	if err != nil {
		return fmt.Errorf("failed to get migration status: %w", err)
	}

	targetIndex := -1
	for i, s := range statuses {
		if s.Version == targetVersion {
			targetIndex = i
			break
		}
	}

	if targetIndex == -1 {
		return fmt.Errorf("migration version not found: %s", targetVersion)
	}

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

	if !autoYes {
		fmt.Printf("Will rollback %d migration(s) to version %s:\n", stepsToRollback, targetVersion)
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
			return fmt.Errorf("canceled by user")
		}
	}

	if err := q.Down(ctx, stepsToRollback); err != nil {
		return fmt.Errorf("failed to rollback migrations: %w", err)
	}

	fmt.Printf("Successfully rolled back to version %s\n", targetVersion)
	return nil
}
