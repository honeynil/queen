package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/yaop-labs/queen"
	"github.com/yaop-labs/queen/cli/tui/live"
	"github.com/yaop-labs/queen/tap"
)

func (app *App) upCmd() *cobra.Command {
	var (
		steps              int
		to                 string
		tapTUI             bool
		tapSlowThreshold   time.Duration
		tapNPlus1Threshold int
	)

	cmd := &cobra.Command{
		Use:   "up",
		Short: "Apply pending migrations",
		RunE: func(cmd *cobra.Command, args []string) error {
			if steps > 0 && to != "" {
				return fmt.Errorf("cannot use both --steps and --to")
			}

			if tapTUI {
				cfg := tap.DefaultAnalyzerConfig()
				cfg.SlowThreshold = tapSlowThreshold
				cfg.NPlus1Threshold = tapNPlus1Threshold
				return runUpWithTap(commandContext(cmd), app, steps, to, cfg)
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

			return app.runWithQueen(cmd, func(ctx context.Context, q *queen.Queen) error {
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
			})
		},
	}

	cmd.Flags().IntVar(&steps, "steps", 0, "Number of migrations to apply (0 = all)")
	cmd.Flags().StringVar(&to, "to", "", "Migrate up to specific version")
	cmd.Flags().BoolVar(&tapTUI, "tap", false, "Launch live tap TUI alongside migration execution")
	cmd.Flags().DurationVar(&tapSlowThreshold, "tap-slow-threshold", 100*time.Millisecond, "Slow query threshold for --tap (0 disables)")
	cmd.Flags().IntVar(&tapNPlus1Threshold, "tap-nplus1-threshold", 5, "Repeated SELECT threshold for --tap N+1 detection (0 disables)")

	return cmd
}

// runUpWithTap runs Up under a live tap TUI. The TUI consumes events from
// a ChannelSink while migrations execute on a background goroutine.
func runUpWithTap(ctx context.Context, app *App, steps int, to string, cfg tap.AnalyzerConfig) error {
	return live.Run(ctx, func(runCtx context.Context, sink tap.Sink) error {
		tapOpt := queen.WithTap(tap.NewAnalyzerSink(sink, cfg))
		return app.runWithQueenContext(runCtx, []queen.Option{tapOpt}, func(ctx context.Context, q *queen.Queen) error {
			if to != "" {
				return migrateUpToVersion(ctx, q, to, true)
			}
			if steps > 0 {
				return q.UpSteps(ctx, steps)
			}
			return q.Up(ctx)
		})
	})
}
