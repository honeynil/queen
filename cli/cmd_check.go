package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/honeynil/queen"
	"github.com/spf13/cobra"
)

func (app *App) checkCmd() *cobra.Command {
	var (
		ci        bool
		noPending bool
		noGaps    bool
	)

	cmd := &cobra.Command{
		Use:   "check",
		Short: "Quick validation for CI/CD pipelines",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			q, err := app.setupQueen(ctx)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Configuration error: %v\n", err)
				os.Exit(2)
			}
			defer func() { _ = q.Close() }()

			exitCode := 0
			passed := 0
			failed := 0

			fmt.Print("Validating migrations... ")
			if err := q.Validate(ctx); err != nil {
				fmt.Printf("FAIL\n  %v\n", err)
				failed++
				exitCode = 3
			} else {
				fmt.Println("OK")
				passed++
			}

			fmt.Print("Checking for gaps... ")
			gaps, err := q.DetectGaps(ctx)
			if err != nil {
				fmt.Printf("FAIL\n  %v\n", err)
				failed++
				if exitCode == 0 {
					exitCode = 3
				}
			} else if len(gaps) > 0 {
				fmt.Printf("WARNING (%d gaps found)\n", len(gaps))
				if noGaps || ci {
					failed++
					if exitCode == 0 {
						exitCode = 4
					}
				} else {
					passed++
				}
			} else {
				fmt.Println("OK")
				passed++
			}

			if noPending || ci {
				fmt.Print("Checking for pending migrations... ")
				statuses, err := q.Status(ctx)
				if err != nil {
					fmt.Printf("FAIL\n  %v\n", err)
					failed++
					if exitCode == 0 {
						exitCode = 3
					}
				} else {
					pendingCount := 0
					for _, s := range statuses {
						if s.Status == queen.StatusPending {
							pendingCount++
						}
					}
					if pendingCount > 0 {
						fmt.Printf("FAIL (%d pending)\n", pendingCount)
						failed++
						if exitCode == 0 {
							exitCode = 5
						}
					} else {
						fmt.Println("OK")
						passed++
					}
				}
			}

			fmt.Print("Database connectivity... ")
			_, err = q.Driver().GetApplied(ctx)
			if err != nil {
				fmt.Printf("FAIL\n  %v\n", err)
				failed++
				if exitCode == 0 {
					exitCode = 3
				}
			} else {
				fmt.Println("OK")
				passed++
			}

			fmt.Println()
			if failed > 0 {
				fmt.Printf("Checks: %d passed, %d failed\n", passed, failed)
				os.Exit(exitCode)
			} else {
				fmt.Printf("All checks passed (%d/%d)\n", passed, passed)
			}

			return nil
		},
	}

	cmd.Flags().BoolVar(&ci, "ci", false, "CI mode (strict validation, fails on warnings)")
	cmd.Flags().BoolVar(&noPending, "no-pending", false, "Fail if pending migrations exist")
	cmd.Flags().BoolVar(&noGaps, "no-gaps", false, "Fail if gaps are detected")

	return cmd
}
