package cli

import (
	"context"
	"fmt"

	"github.com/yaop-labs/queen"
)

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

	stepsToApply := countPendingThrough(statuses, targetIndex)
	if stepsToApply == 0 {
		fmt.Println("Already at or past the target version")
		return nil
	}

	if !autoYes {
		fmt.Printf("Will apply %d migration(s) to reach version %s:\n", stepsToApply, targetVersion)
		printPendingThrough(statuses, targetIndex)
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

	stepsToRollback, hasDestructive := rollbackPlanAfter(statuses, targetIndex)
	if stepsToRollback == 0 {
		fmt.Println("Already at the target version")
		return nil
	}

	if !autoYes {
		fmt.Printf("Will rollback %d migration(s) to version %s:\n", stepsToRollback, targetVersion)
		printRollbackAfter(statuses, targetIndex)
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

func (app *App) migrateUpTo(ctx context.Context, q *queen.Queen, statuses []queen.MigrationStatus, targetIndex int) error {
	stepsToApply := countPendingThrough(statuses, targetIndex)
	if stepsToApply == 0 {
		fmt.Println("Already at or past the target version")
		return nil
	}

	if !app.config.Yes {
		fmt.Printf("Will apply %d migration(s):\n", stepsToApply)
		printPendingThrough(statuses, targetIndex)
		fmt.Println()

		if !confirm("Proceed with migration?") {
			fmt.Println("Canceled")
			return nil
		}
	}

	return q.UpSteps(ctx, stepsToApply)
}

func (app *App) migrateDownTo(ctx context.Context, q *queen.Queen, statuses []queen.MigrationStatus, targetIndex int) error {
	stepsToRollback, hasDestructive := rollbackPlanAfter(statuses, targetIndex)
	if stepsToRollback == 0 {
		fmt.Println("Already at the target version")
		return nil
	}

	if !app.config.Yes {
		fmt.Printf("Will rollback %d migration(s):\n", stepsToRollback)
		printRollbackAfter(statuses, targetIndex)
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

func countPendingThrough(statuses []queen.MigrationStatus, targetIndex int) int {
	stepsToApply := 0
	for i := 0; i <= targetIndex; i++ {
		if statuses[i].Status == queen.StatusPending {
			stepsToApply++
		}
	}
	return stepsToApply
}

func rollbackPlanAfter(statuses []queen.MigrationStatus, targetIndex int) (steps int, hasDestructive bool) {
	for i := targetIndex + 1; i < len(statuses); i++ {
		if statuses[i].Status == queen.StatusApplied {
			steps++
			if statuses[i].Destructive {
				hasDestructive = true
			}
		}
	}
	return steps, hasDestructive
}

func printPendingThrough(statuses []queen.MigrationStatus, targetIndex int) {
	for i := 0; i <= targetIndex; i++ {
		if statuses[i].Status == queen.StatusPending {
			fmt.Printf("  ↑ %s - %s\n", statuses[i].Version, statuses[i].Name)
		}
	}
}

func printRollbackAfter(statuses []queen.MigrationStatus, targetIndex int) {
	for i := len(statuses) - 1; i > targetIndex; i-- {
		if statuses[i].Status == queen.StatusApplied {
			marker := "↓"
			if statuses[i].Destructive {
				marker = "!"
			}
			fmt.Printf("  %s %s - %s\n", marker, statuses[i].Version, statuses[i].Name)
		}
	}
}
