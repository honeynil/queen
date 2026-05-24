package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

func (app *App) addGlobalFlags() {
	flags := app.rootCmd.PersistentFlags()

	flags.StringVar(
		&app.config.Driver,
		"driver",
		"",
		fmt.Sprintf(
			"Database driver (%s, %s, %s, %s, %s, %s)",
			DriverPostgres,
			DriverMySQL,
			DriverSQLite,
			DriverClickHouse,
			DriverCockroach,
			DriverMSSQL,
		),
	)
	flags.StringVar(&app.config.DSN, "dsn", "", "Database connection string")
	flags.StringVar(&app.config.Table, "table", "queen_migrations", "Migration table name")
	flags.DurationVar(&app.config.LockTimeout, "timeout", 0, "Lock timeout (e.g. 30m, 1h)")
	flags.BoolVar(&app.config.UseConfig, "use-config", false, "Enable config file (.queen.yaml)")
	flags.StringVar(&app.config.Env, "env", "", "Environment from config file (development, staging, production)")
	flags.BoolVar(&app.config.UnlockProduction, "unlock-production", false, "Unlock production environment")
	flags.BoolVar(&app.config.Yes, "yes", false, "Automatic yes to prompts (for CI/CD)")
	flags.BoolVar(&app.config.JSON, "json", false, "Output in JSON format")
	flags.BoolVar(&app.config.Verbose, "verbose", false, "Verbose output")
}

func (app *App) addCommands() {
	for _, group := range app.commandGroups() {
		app.rootCmd.AddCommand(group...)
	}
}

func (app *App) commandGroups() [][]*cobra.Command {
	return [][]*cobra.Command{
		app.migrationCommands(),
		app.inspectionCommands(),
		app.maintenanceCommands(),
		app.projectCommands(),
		app.uiCommands(),
	}
}

func (app *App) migrationCommands() []*cobra.Command {
	return []*cobra.Command{
		app.upCmd(),
		app.downCmd(),
		app.resetCmd(),
		app.gotoCmd(),
	}
}

func (app *App) inspectionCommands() []*cobra.Command {
	return []*cobra.Command{
		app.statusCmd(),
		app.validateCmd(),
		app.versionCmd(),
		app.planCmd(),
		app.explainCmd(),
		app.logCmd(),
		app.diffCmd(),
		app.doctorCmd(),
		app.checkCmd(),
	}
}

func (app *App) maintenanceCommands() []*cobra.Command {
	return []*cobra.Command{
		app.gapCmd(),
		app.squashCmd(),
		app.baselineCmd(),
		app.importCmd(),
	}
}

func (app *App) projectCommands() []*cobra.Command {
	return []*cobra.Command{
		app.createCmd(),
		app.initCmd(),
	}
}

func (app *App) uiCommands() []*cobra.Command {
	return []*cobra.Command{
		app.tuiCmd(),
	}
}
