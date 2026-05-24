package queen_test

import (
	"testing"

	"github.com/yaop-labs/queen"
	"github.com/yaop-labs/queen/drivers/mock"
)

func TestTestHelper_TestRollback(t *testing.T) {
	driver := mock.New()
	th := queen.NewTest(t, driver)

	th.MustAdd(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL:   `CREATE TABLE users (id INTEGER PRIMARY KEY)`,
		DownSQL: `DROP TABLE users`,
	})

	th.MustAdd(queen.M{
		Version: "002",
		Name:    "create_posts",
		UpSQL:   `CREATE TABLE posts (id INTEGER PRIMARY KEY)`,
		DownSQL: `DROP TABLE posts`,
	})

	th.MustAdd(queen.M{
		Version: "003",
		Name:    "create_comments",
		UpSQL:   `CREATE TABLE comments (id INTEGER PRIMARY KEY)`,
		DownSQL: `DROP TABLE comments`,
	})

	th.TestRollback()
}
