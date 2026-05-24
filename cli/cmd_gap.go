package cli

import "github.com/spf13/cobra"

func (app *App) gapCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "gap",
		Short: "Detect and manage migration gaps",
	}

	cmd.AddCommand(
		app.gapDetectCmd(),
		app.gapAnalyzeCmd(),
		app.gapFillCmd(),
		app.gapIgnoreCmd(),
		app.gapListIgnoredCmd(),
		app.gapUnignoreCmd(),
	)

	return cmd
}
