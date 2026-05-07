package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/honeynil/queen"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

func (app *App) statusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show status of all registered migrations",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			q, err := app.setupQueen(ctx)
			if err != nil {
				return err
			}
			defer func() { _ = q.Close() }()

			statuses, err := q.Status(ctx)
			if err != nil {
				return fmt.Errorf("failed to get migration status: %w", err)
			}

			if app.config.JSON {
				return app.outputStatusJSON(statuses)
			}
			return app.outputStatusTable(statuses)
		},
	}
}

func countStatuses(statuses []queen.MigrationStatus) (applied, pending, modified int) {
	for _, s := range statuses {
		switch s.Status {
		case queen.StatusApplied:
			applied++
		case queen.StatusPending:
			pending++
		case queen.StatusModified:
			modified++
		}
	}
	return
}

func (app *App) outputStatusTable(statuses []queen.MigrationStatus) error {
	table := tablewriter.NewWriter(os.Stdout)
	table.Header([]string{"Version", "Name", "Status", "Applied At", "Checksum", "Rollback"})

	for _, s := range statuses {
		rollback := "no"
		if s.HasRollback {
			rollback = "yes"
		}

		appliedAt := "-"
		if s.AppliedAt != nil {
			appliedAt = s.AppliedAt.Format("2006-01-02 15:04:05")
		}

		checksum := s.Checksum
		if len(checksum) > 12 {
			checksum = checksum[:12] + "..."
		}

		if err := table.Append([]string{
			s.Version,
			s.Name,
			s.Status.String(),
			appliedAt,
			checksum,
			rollback,
		}); err != nil {
			return err
		}
	}

	if err := table.Render(); err != nil {
		return err
	}

	applied, pending, modified := countStatuses(statuses)
	fmt.Printf("\nSummary: %d total, %d applied, %d pending", len(statuses), applied, pending)
	if modified > 0 {
		fmt.Printf(", %d modified (WARNING: WARNING)", modified)
	}
	fmt.Println()
	return nil
}

func (app *App) outputStatusJSON(statuses []queen.MigrationStatus) error {
	applied, pending, modified := countStatuses(statuses)

	output := struct {
		Migrations []queen.MigrationStatus `json:"migrations"`
		Summary    struct {
			Total    int `json:"total"`
			Applied  int `json:"applied"`
			Pending  int `json:"pending"`
			Modified int `json:"modified"`
		} `json:"summary"`
	}{
		Migrations: statuses,
	}

	output.Summary.Total = len(statuses)
	output.Summary.Applied = applied
	output.Summary.Pending = pending
	output.Summary.Modified = modified

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(output)
}
