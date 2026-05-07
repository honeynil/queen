package cli

import (
	"context"
	"fmt"

	"github.com/honeynil/queen"
	"github.com/spf13/cobra"
)

func (app *App) upCmd() *cobra.Command {
	var (
		steps int
		to    string
	)

	cmd := &cobra.Command{
		Use:   "up",
		Short: "Apply pending migrations",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			if steps > 0 && to != "" {
				return fmt.Errorf("cannot use both --steps and --to")
			}

			operation := "apply migrations"
			if steps > 0 {
				operation = fmt.Sprintf("apply %d migration(s)", steps)
			} else if to != "" {
				operation = fmt.Sprintf("migrate up to version %s", to)
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
				return migrateUpToVersion(ctx, q, to, app.config.Yes)
			}

			if steps > 0 {
				if err := q.UpSteps(ctx, steps); err != nil {
					return fmt.Errorf("failed to apply migrations: %w", err)
				}
				fmt.Printf("Applied %d migration(s)\n", steps)
			} else {
				if err := q.Up(ctx); err != nil {
					return fmt.Errorf("failed to apply migrations: %w", err)
				}
				fmt.Println("All migrations applied successfully")
			}

			return nil
		},
	}

	cmd.Flags().IntVar(&steps, "steps", 0, "Number of migrations to apply (0 = all)")
	cmd.Flags().StringVar(&to, "to", "", "Migrate up to specific version")

	return cmd
}

func migrateUpToVersion(ctx context.Context, q *queen.Queen, targetVersion string, autoYes bool) error {
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

	if !autoYes {
		fmt.Printf("Will apply %d migration(s) to reach version %s:\n", stepsToApply, targetVersion)
		for i := 0; i <= targetIndex; i++ {
			if statuses[i].Status == queen.StatusPending {
				fmt.Printf("  ↑ %s - %s\n", statuses[i].Version, statuses[i].Name)
			}
		}
		fmt.Println()

		if !confirm("Proceed with migration?") {
			return fmt.Errorf("canceled by user")
		}
	}

	if err := q.UpSteps(ctx, stepsToApply); err != nil {
		return fmt.Errorf("failed to apply migrations: %w", err)
	}

	fmt.Printf("Successfully migrated to version %s\n", targetVersion)
	return nil
}
