package cli

import (
	"context"
	"fmt"

	"github.com/yaop-labs/queen"
)

type baselineOptions struct {
	name   string
	at     string
	dryRun bool
}

func (app *App) runBaseline(ctx context.Context, q *queen.Queen, opts baselineOptions) error {
	statuses, err := q.Status(ctx)
	if err != nil {
		return fmt.Errorf("failed to get migration status: %w", err)
	}
	if len(statuses) == 0 {
		return fmt.Errorf("no registered migrations to baseline")
	}

	targetIndex, targetVersion, err := resolveBaselineTarget(statuses, opts.at)
	if err != nil {
		return err
	}

	toMark := pendingStatusesThrough(statuses, targetIndex)

	if opts.dryRun {
		fmt.Println("Dry run mode - no changes will be made")
		outputBlank()
	}

	fmt.Printf("Creating baseline: %s\n", opts.name)
	fmt.Printf("At version: %s\n", targetVersion)
	outputBlank()

	if len(toMark) == 0 {
		fmt.Println("No pending migrations to mark as applied")
		return nil
	}

	fmt.Printf("Will mark %d migration(s) as applied:\n", len(toMark))
	for _, status := range toMark {
		fmt.Printf("  %s - %s\n", status.Version, status.Name)
	}
	outputBlank()

	if opts.dryRun {
		fmt.Println("Run without --dry-run to write baseline records")
		return nil
	}

	meta := &queen.MigrationMetadata{
		AppliedBy: currentUsername(),
		Action:    "baseline",
		Status:    "success",
	}

	for _, status := range toMark {
		migration := q.FindMigration(status.Version)
		if migration == nil {
			return fmt.Errorf("migration not registered: %s", status.Version)
		}
		if err := q.Driver().Record(ctx, migration, meta); err != nil {
			return fmt.Errorf("failed to record baseline migration %s: %w", status.Version, err)
		}
	}

	fmt.Printf("Baseline recorded for %d migration(s)\n", len(toMark))
	return nil
}

func resolveBaselineTarget(statuses []queen.MigrationStatus, at string) (int, string, error) {
	if at == "" {
		last := len(statuses) - 1
		return last, statuses[last].Version, nil
	}

	for i, status := range statuses {
		if status.Version == at {
			return i, status.Version, nil
		}
	}
	return -1, "", fmt.Errorf("migration version not found: %s", at)
}

func pendingStatusesThrough(statuses []queen.MigrationStatus, targetIndex int) []queen.MigrationStatus {
	out := make([]queen.MigrationStatus, 0, targetIndex+1)
	for i := 0; i <= targetIndex && i < len(statuses); i++ {
		if statuses[i].Status == queen.StatusPending {
			out = append(out, statuses[i])
		}
	}
	return out
}
