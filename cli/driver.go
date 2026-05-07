package cli

import (
	"database/sql"
	"fmt"

	"github.com/honeynil/queen"
	"github.com/honeynil/queen/drivers/clickhouse"
	"github.com/honeynil/queen/drivers/cockroachdb"
	"github.com/honeynil/queen/drivers/mssql"
	"github.com/honeynil/queen/drivers/mysql"
	"github.com/honeynil/queen/drivers/postgres"
	"github.com/honeynil/queen/drivers/sqlite"
)

const (
	DriverPostgres   = "postgres"
	DriverPostgreSQL = "postgresql"
	DriverMySQL      = "mysql"
	DriverSQLite     = "sqlite"
	DriverSQLite3    = "sqlite3"
	DriverClickHouse = "clickhouse"
	DriverCockroach  = "cockroachdb"
	DriverMSSQL      = "mssql"
	DriverSQLServer  = "sqlserver"

	SQLDriverPostgres   = "pgx"
	SQLDriverMySQL      = "mysql"
	SQLDriverSQLite     = "sqlite3"
	SQLDriverClickHouse = "clickhouse"
	SQLDriverMSSQL      = "sqlserver"
)

func getSQLDriverName(driverName string) string {
	switch driverName {
	case DriverPostgres, DriverPostgreSQL:
		return SQLDriverPostgres
	case DriverMySQL:
		return SQLDriverMySQL
	case DriverSQLite, DriverSQLite3:
		return SQLDriverSQLite
	case DriverClickHouse:
		return SQLDriverClickHouse
	case DriverCockroach:
		return SQLDriverPostgres
	case DriverMSSQL, DriverSQLServer:
		return SQLDriverMSSQL
	default:
		return driverName
	}
}

func (app *App) createDriver(db *sql.DB) (queen.Driver, error) {
	switch app.config.Driver {
	case DriverPostgres, DriverPostgreSQL, "pgx":
		return postgres.New(db), nil

	case "mysql":
		return mysql.New(db), nil

	case "sqlite", "sqlite3":
		return sqlite.New(db), nil

	case "clickhouse":
		return clickhouse.New(db)

	case DriverCockroach:
		return cockroachdb.New(db)

	case DriverMSSQL, DriverSQLServer:
		return mssql.New(db), nil

	default:
		return nil, fmt.Errorf(
			"unsupported driver: %s (supported: postgres, mysql, sqlite, clickhouse, cockroachdb, mssql)",
			app.config.Driver,
		)
	}
}
