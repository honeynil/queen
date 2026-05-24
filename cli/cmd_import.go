package cli

import (
	"github.com/spf13/cobra"
	"github.com/yaop-labs/queen/cli/commands/importcmd"
)

func (app *App) importCmd() *cobra.Command {
	return importcmd.New()
}
