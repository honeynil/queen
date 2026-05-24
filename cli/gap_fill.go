package cli

import (
	"context"
	"fmt"
	"slices"

	"github.com/spf13/cobra"
	"github.com/yaop-labs/queen"
)

func (app *App) gapFillCmd() *cobra.Command {
	var markApplied bool

	cmd := &cobra.Command{
		Use:   "fill [versions...]",
		Short: "Fill detected gaps",
		RunE: func(cmd *cobra.Command, args []string) error {
			return app.runWithQueen(cmd, func(ctx context.Context, q *queen.Queen) error {
				gaps, err := q.DetectGaps(ctx)
				if err != nil {
					return fmt.Errorf("failed to detect gaps: %w", err)
				}

				if len(gaps) == 0 {
					fmt.Println("No gaps detected")
					return nil
				}

				applicationGaps := make([]queen.Gap, 0)
				for _, gap := range gaps {
					if gap.Type == queen.GapTypeApplication {
						if len(args) == 0 || slices.Contains(args, gap.Version) {
							applicationGaps = append(applicationGaps, gap)
						}
					}
				}

				if len(applicationGaps) == 0 {
					fmt.Println("No application gaps to fill")
					return nil
				}

				if markApplied {
					fmt.Println("WARNING: Marking migrations as applied without executing them")
					fmt.Println("This should only be used if migrations were manually applied.")
				}

				fmt.Printf("Will fill %d gap(s):\n", len(applicationGaps))
				for _, gap := range applicationGaps {
					fmt.Printf("  %s - %s\n", gap.Version, gap.Name)
				}
				fmt.Println()

				if !app.config.Yes {
					action := "apply"
					if markApplied {
						action = "mark as applied"
					}
					if !confirm(fmt.Sprintf("Proceed to %s these migrations?", action)) {
						fmt.Println("Canceled")
						return nil
					}
				}

				if markApplied {
					return fillGapsByMarking(ctx, q, applicationGaps)
				}

				return fillGapsByApplying(ctx, q, applicationGaps)
			})
		},
	}

	cmd.Flags().BoolVar(&markApplied, "mark-applied", false, "Mark migrations as applied without executing")

	return cmd
}

func fillGapsByApplying(ctx context.Context, q *queen.Queen, gaps []queen.Gap) error {
	statuses, err := q.Status(ctx)
	if err != nil {
		return fmt.Errorf("failed to get migration status: %w", err)
	}

	statusMap := make(map[string]int)
	for i, s := range statuses {
		statusMap[s.Version] = i
	}

	successCount := 0
	for _, gap := range gaps {
		fmt.Printf("Applying %s - %s... ", gap.Version, gap.Name)

		targetIndex, exists := statusMap[gap.Version]
		if !exists {
			fmt.Printf("SKIP (not found)\n")
			continue
		}

		stepsToApply := 0
		for i := 0; i <= targetIndex; i++ {
			if statuses[i].Status == queen.StatusPending {
				stepsToApply++
			}
		}

		if stepsToApply == 0 {
			fmt.Printf("SKIP (already applied)\n")
			continue
		}

		if err := q.UpSteps(ctx, stepsToApply); err != nil {
			fmt.Printf("FAILED\n")
			return fmt.Errorf("failed to apply migration %s: %w", gap.Version, err)
		}

		fmt.Printf("OK\n")
		successCount++

		statuses, err = q.Status(ctx)
		if err != nil {
			return fmt.Errorf("failed to reload status: %w", err)
		}
	}

	fmt.Println()
	fmt.Printf("Successfully filled %d gap(s)\n", successCount)
	return nil
}

// fillGapsByMarking is for baselined databases where the schema already exists.
func fillGapsByMarking(ctx context.Context, q *queen.Queen, gaps []queen.Gap) error {
	fmt.Println("WARNING: Marking migrations as applied without executing SQL (dangerous operation)")
	fmt.Println()

	currentUser := currentUsername()

	successCount := 0
	for _, gap := range gaps {
		fmt.Printf("Marking %s - %s... ", gap.Version, gap.Name)

		m := q.FindMigration(gap.Version)
		if m == nil {
			fmt.Printf("SKIP (not registered in code)\n")
			continue
		}

		meta := &queen.MigrationMetadata{
			AppliedBy: currentUser,
			Action:    "mark-applied",
			Status:    "success",
		}

		if err := q.Driver().Record(ctx, m, meta); err != nil {
			fmt.Printf("FAILED: %v\n", err)
			return fmt.Errorf("failed to mark migration %s: %w", gap.Version, err)
		}

		fmt.Printf("OK\n")
		successCount++
	}

	fmt.Println()
	fmt.Printf("Successfully marked %d migration(s) as applied\n", successCount)
	return nil
}
