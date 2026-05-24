package queen_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/yaop-labs/queen"
	"github.com/yaop-labs/queen/drivers/base"
	"github.com/yaop-labs/queen/drivers/sqlite"
)

type txRecorderSQLiteDriver struct {
	*sqlite.Driver
	failRecordTx bool
	failRemoveTx bool
}

func newTxRecorderSQLiteQueen(t *testing.T, driver *txRecorderSQLiteDriver) (*queen.Queen, *sql.DB, func()) {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)

	driver.Driver = sqlite.New(db)
	q := queen.New(driver)

	return q, db, func() {
		_ = q.Close()
	}
}

func (d *txRecorderSQLiteDriver) RecordTx(ctx context.Context, tx *sql.Tx, m *queen.Migration, meta *queen.MigrationMetadata) error {
	if d.failRecordTx {
		return errors.New("record tx failed")
	}
	return base.RecordTx(ctx, &d.Driver.Driver, tx, m, meta)
}

func (d *txRecorderSQLiteDriver) RemoveTx(ctx context.Context, tx *sql.Tx, version string) error {
	if d.failRemoveTx {
		return errors.New("remove tx failed")
	}
	return base.RemoveTx(ctx, &d.Driver.Driver, tx, version)
}

func TestTransactionalRecordRollsBackMigrationBody(t *testing.T) {
	driver := &txRecorderSQLiteDriver{failRecordTx: true}
	q, db, cleanup := newTxRecorderSQLiteQueen(t, driver)
	defer cleanup()

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL:   "CREATE TABLE users (id INTEGER PRIMARY KEY)",
	})

	err := q.Up(context.Background())
	if err == nil {
		t.Fatal("Up succeeded; want transactional record failure")
	}
	if !strings.Contains(err.Error(), "record migration in transaction") {
		t.Fatalf("Up error = %v; want transactional record context", err)
	}

	if tableExists(t, db, "users") {
		t.Fatal("users table exists after RecordTx failure; migration body was not rolled back")
	}
	if migrationRecordExists(t, db, "001") {
		t.Fatal("migration record exists after RecordTx failure")
	}
}

func TestTransactionalRemoveRollsBackDownMigration(t *testing.T) {
	driver := &txRecorderSQLiteDriver{}
	q, db, cleanup := newTxRecorderSQLiteQueen(t, driver)
	defer cleanup()

	q.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL:   "CREATE TABLE users (id INTEGER PRIMARY KEY)",
		DownSQL: "DROP TABLE users",
	})

	if err := q.Up(context.Background()); err != nil {
		t.Fatalf("Up: %v", err)
	}
	driver.failRemoveTx = true

	err := q.Down(context.Background(), 1)
	if err == nil {
		t.Fatal("Down succeeded; want transactional remove failure")
	}
	if !strings.Contains(err.Error(), "remove migration in transaction") {
		t.Fatalf("Down error = %v; want transactional remove context", err)
	}

	if !tableExists(t, db, "users") {
		t.Fatal("users table was dropped despite RemoveTx failure")
	}
	if !migrationRecordExists(t, db, "001") {
		t.Fatal("migration record was removed despite RemoveTx failure")
	}
}

func tableExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()

	var count int
	err := db.QueryRow(
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
		name,
	).Scan(&count)
	if err != nil {
		t.Fatalf("query sqlite_master for %s: %v", name, err)
	}
	return count > 0
}

func migrationRecordExists(t *testing.T, db *sql.DB, version string) bool {
	t.Helper()

	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM queen_migrations WHERE version = ?", version).Scan(&count)
	if err != nil {
		t.Fatalf("query queen_migrations for %s: %v", version, err)
	}
	return count > 0
}
