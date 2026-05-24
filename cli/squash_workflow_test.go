package cli

import (
	"strings"
	"testing"

	"github.com/yaop-labs/queen"
)

func TestParseSquashVersionArgsAcceptsCommaAndSpaceSeparated(t *testing.T) {
	got := parseSquashVersionArgs([]string{"001,002", "003"})
	want := []string{"001", "002", "003"}

	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("parseSquashVersionArgs()=%v, want %v", got, want)
	}
}

func TestCombineSQLMigrationsOrdersDownSQLInReverse(t *testing.T) {
	migrations := []*queen.Migration{
		{
			Version: "001",
			Name:    "create_users",
			UpSQL:   "CREATE TABLE users (id INT);",
			DownSQL: "DROP TABLE users;",
		},
		{
			Version: "002",
			Name:    "create_posts",
			UpSQL:   "CREATE TABLE posts (id INT);",
			DownSQL: "DROP TABLE posts;",
		},
	}

	upSQL, downSQL, err := combineSQLMigrations(migrations)
	if err != nil {
		t.Fatalf("combineSQLMigrations returned error: %v", err)
	}
	if !strings.Contains(upSQL, "-- 001 create_users") || !strings.Contains(upSQL, "-- 002 create_posts") {
		t.Fatalf("upSQL missing migration headers:\n%s", upSQL)
	}
	if strings.Index(downSQL, "-- 002 create_posts") > strings.Index(downSQL, "-- 001 create_users") {
		t.Fatalf("downSQL not reversed:\n%s", downSQL)
	}
}

func TestCombineSQLMigrationsRejectsGoMigration(t *testing.T) {
	_, _, err := combineSQLMigrations([]*queen.Migration{
		{Version: "001", Name: "go_migration", UpFunc: func() queen.MigrationFunc { return nil }()},
	})
	if err == nil {
		t.Fatal("combineSQLMigrations returned nil error, want rejection")
	}
}
