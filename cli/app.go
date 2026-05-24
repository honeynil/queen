package cli

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/yaop-labs/queen"
)

// RegisterFunc is a function that registers migrations with Queen.
type RegisterFunc func(*queen.Queen)

// DBOpener is a function that opens a database connection.
type DBOpener func(dsn string) (*sql.DB, error)

const DefaultTableName = "queen_migrations"

// App holds the CLI application state.
type App struct {
	registerFunc RegisterFunc
	dbOpener     DBOpener
	config       *Config
	rootCmd      *cobra.Command
	queenOpts    []queen.Option
}

func newApp(register RegisterFunc, dbOpener DBOpener) *App {
	app := &App{
		registerFunc: register,
		dbOpener:     dbOpener,
		config:       &Config{},
	}

	app.rootCmd = &cobra.Command{
		Use:           "queen",
		Short:         "Queen migration CLI",
		Long:          queenLogo,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	app.addGlobalFlags()
	app.addCommands()

	return app
}

func (app *App) execute() {
	if err := app.rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
