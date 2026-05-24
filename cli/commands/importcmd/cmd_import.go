package importcmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func New() *cobra.Command {
	var (
		fromTool string
		dryRun   bool
		output   string
	)

	cmd := &cobra.Command{
		Use:   "import PATH",
		Short: "Import migrations from other tools",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			sourcePath := args[0]

			if fromTool == "" {
				return fmt.Errorf("--from flag is required (goose)")
			}

			if output == "" {
				output = "migrations"
			}

			if _, err := os.Stat(sourcePath); os.IsNotExist(err) {
				return fmt.Errorf("source path does not exist: %s", sourcePath)
			}

			if dryRun {
				fmt.Println("Dry run mode - no changes will be made")
				fmt.Println()
			}

			fmt.Printf("Importing migrations from %s\n", fromTool)
			fmt.Printf("Source: %s\n", sourcePath)
			fmt.Printf("Output: %s\n", output)
			fmt.Println()

			switch fromTool {
			case "goose":
				return importFromGoose(sourcePath, output, dryRun)
			default:
				return fmt.Errorf("unsupported tool: %s (supported: goose)", fromTool)
			}
		},
	}

	cmd.Flags().StringVar(&fromTool, "from", "", "Migration tool to import from (goose)")
	cmd.Flags().StringVar(&output, "output", "", "Output directory for Queen migrations (default: migrations)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Preview import without making changes")

	return cmd
}
