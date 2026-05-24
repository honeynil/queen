package cli

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/spf13/cobra"
	"github.com/yaop-labs/queen"
	naturalsort "github.com/yaop-labs/queen/internal/sort"
)

func (app *App) logCmd() *cobra.Command {
	var (
		last         int
		since        string
		withDuration bool
		withMeta     bool
		reverse      bool
	)

	cmd := &cobra.Command{
		Use:   "log",
		Short: "Show migration history",
		RunE: func(cmd *cobra.Command, args []string) error {
			return app.runWithQueen(cmd, func(ctx context.Context, q *queen.Queen) error {
				applied, err := q.Driver().GetApplied(ctx)
				if err != nil {
					return fmt.Errorf("failed to get applied migrations: %w", err)
				}

				if len(applied) == 0 {
					if !app.config.JSON {
						fmt.Println("No migrations applied yet")
					}
					return nil
				}

				sort.Slice(applied, func(i, j int) bool {
					return naturalsort.Compare(applied[i].Version, applied[j].Version) < 0
				})

				if since != "" {
					sinceTime, err := time.Parse("2006-01-02", since)
					if err != nil {
						return fmt.Errorf("invalid date format for --since (expected YYYY-MM-DD): %w", err)
					}

					filtered := make([]queen.Applied, 0)
					for _, a := range applied {
						if a.AppliedAt.After(sinceTime) || a.AppliedAt.Equal(sinceTime) {
							filtered = append(filtered, a)
						}
					}
					applied = filtered
				}

				if reverse {
					for i, j := 0, len(applied)-1; i < j; i, j = i+1, j-1 {
						applied[i], applied[j] = applied[j], applied[i]
					}
				}

				if last > 0 && last < len(applied) {
					applied = applied[len(applied)-last:]
				}

				if app.config.JSON {
					return outputLogJSON(applied)
				}

				outputLogTable(applied, withDuration, withMeta)
				return nil
			})
		},
	}

	cmd.Flags().IntVar(&last, "last", 0, "Show last N migrations")
	cmd.Flags().StringVar(&since, "since", "", "Show migrations since date (YYYY-MM-DD)")
	cmd.Flags().BoolVar(&withDuration, "with-duration", false, "Show execution duration")
	cmd.Flags().BoolVar(&withMeta, "with-meta", false, "Show all metadata (applied_by, hostname, etc.)")
	cmd.Flags().BoolVar(&reverse, "reverse", false, "Show in reverse order (newest first)")

	return cmd
}
