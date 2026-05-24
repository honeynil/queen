package cli

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/yaop-labs/queen"
)

func (app *App) setupQueen(ctx context.Context) (*queen.Queen, error) {
	if err := app.loadConfig(); err != nil {
		return nil, err
	}

	if app.config.Driver == "" {
		return nil, fmt.Errorf("driver is required (use --driver or QUEEN_DRIVER)")
	}
	if app.config.DSN == "" {
		return nil, fmt.Errorf("dsn is required (use --dsn or QUEEN_DSN)")
	}

	var db *sql.DB
	var err error

	if app.dbOpener != nil {
		db, err = app.dbOpener(app.config.DSN)
	} else {
		sqlDriverName := getSQLDriverName(app.config.Driver)
		db, err = sql.Open(sqlDriverName, app.config.DSN)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	driver, err := app.createDriver(db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	queenConfig := &queen.Config{
		TableName: app.config.Table,
	}
	if app.config.LockTimeout > 0 {
		queenConfig.LockTimeout = app.config.LockTimeout
	}

	q := queen.NewWithConfig(driver, queenConfig, app.queenOpts...)
	app.registerFunc(q)

	return q, nil
}

func (app *App) loadConfig() error {
	if app.config.UseConfig {
		if err := app.loadConfigFile(); err != nil {
			return err
		}
	}
	app.loadEnv()
	return nil
}

func (app *App) loadEnv() {
	if app.config.Driver == "" {
		if driver := os.Getenv("QUEEN_DRIVER"); driver != "" {
			app.config.Driver = driver
		}
	}

	if app.config.DSN == "" {
		if dsn := os.Getenv("QUEEN_DSN"); dsn != "" {
			app.config.DSN = dsn
		}
	}

	if app.config.Table == "queen_migrations" {
		if table := os.Getenv("QUEEN_TABLE"); table != "" {
			app.config.Table = table
		}
	}
}
