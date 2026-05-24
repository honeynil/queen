package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/yaop-labs/queen"
)

func (app *App) explainCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "explain <version>",
		Short: "Explain a specific migration",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			version := args[0]

			return app.runWithQueen(cmd, func(ctx context.Context, q *queen.Queen) error {
				plan, err := q.Explain(ctx, version)
				if err != nil {
					return fmt.Errorf("failed to explain migration: %w", err)
				}

				if app.config.JSON {
					return app.outputExplainJSON(plan)
				}
				app.outputExplainTable(plan)
				return nil
			})
		},
	}
}

func (app *App) outputExplainTable(plan *queen.MigrationPlan) {
	fmt.Printf("Migration: %s\n", plan.Version)
	outputRule("━", 60)
	outputBlank()

	fmt.Printf("Name:          %s\n", plan.Name)
	fmt.Printf("Status:        %s\n", plan.Status)
	fmt.Printf("Type:          %s\n", plan.Type)
	fmt.Printf("Direction:     %s\n", plan.Direction)
	fmt.Printf("Has Rollback:  %v\n", plan.HasRollback)
	fmt.Printf("Checksum:      %s\n", plan.Checksum)

	if plan.IsDestructive {
		fmt.Println("Destructive:   yes")
	}

	if len(plan.Warnings) > 0 {
		outputBlank()
		outputWarning("Warnings:")
		for _, warning := range plan.Warnings {
			fmt.Printf("  - %s\n", warning)
		}
	}

	if plan.SQL != "" {
		outputBlank()
		fmt.Printf("%s SQL:\n", strings.ToUpper(plan.Direction))
		outputRule("-", 60)
		fmt.Println(plan.SQL)
		outputRule("-", 60)
	} else if plan.Type == queen.MigrationTypeGoFunc {
		outputBlank()
		fmt.Printf("%s: Go function (code not shown)\n", strings.ToUpper(plan.Direction))
	}
}

func (app *App) outputExplainJSON(plan *queen.MigrationPlan) error {
	return outputJSON(plan)
}
