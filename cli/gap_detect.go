package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/yaop-labs/queen"
)

func (app *App) gapDetectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "detect",
		Short: "Detect migration gaps",
		RunE: func(cmd *cobra.Command, args []string) error {
			return app.runWithQueen(cmd, func(ctx context.Context, q *queen.Queen) error {
				gaps, err := q.DetectGaps(ctx)
				if err != nil {
					return fmt.Errorf("failed to detect gaps: %w", err)
				}

				if len(gaps) == 0 {
					if !app.config.JSON {
						fmt.Println("No gaps detected")
					} else {
						fmt.Println("[]")
					}
					return nil
				}

				if app.config.JSON {
					return outputGapsJSON(gaps)
				}

				outputGapsTable(gaps)
				return nil
			})
		},
	}

	return cmd
}

func (app *App) gapAnalyzeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "analyze",
		Short: "Analyze gap dependencies and impact",
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

				analyzeGaps(gaps)
				return nil
			})
		},
	}

	return cmd
}
