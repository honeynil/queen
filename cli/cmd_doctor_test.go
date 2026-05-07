package cli

import (
	"context"
	"testing"

	"github.com/honeynil/queen"
	"github.com/honeynil/queen/drivers/mock"
)

func TestCollectDoctorResults(t *testing.T) {
	t.Parallel()

	t.Run("gaps mode only returns gap checks", func(t *testing.T) {
		t.Parallel()

		driver := mock.New()
		q := queen.New(driver)
		defer func() { _ = q.Close() }()

		q.MustAdd(queen.M{
			Version: "001",
			Name:    "create_users",
			UpSQL:   "CREATE TABLE users (id INTEGER PRIMARY KEY);",
			DownSQL: "DROP TABLE users;",
		})
		q.MustAdd(queen.M{
			Version: "003",
			Name:    "create_posts",
			UpSQL:   "CREATE TABLE posts (id INTEGER PRIMARY KEY);",
			DownSQL: "DROP TABLE posts;",
		})

		results := collectDoctorResults(context.Background(), q, doctorOptions{gaps: true})
		if len(results) != 1 {
			t.Fatalf("len(results) = %d, want 1", len(results))
		}
		if results[0].Check != "Gap Detection" {
			t.Fatalf("results[0].Check = %q, want %q", results[0].Check, "Gap Detection")
		}
		if results[0].Status != statusWarning {
			t.Fatalf("results[0].Status = %q, want %q", results[0].Status, statusWarning)
		}
	})

	t.Run("default mode still includes multiple checks", func(t *testing.T) {
		t.Parallel()

		driver := mock.New()
		q := queen.New(driver)
		defer func() { _ = q.Close() }()

		q.MustAdd(queen.M{
			Version: "001",
			Name:    "create_users",
			UpSQL:   "CREATE TABLE users (id INTEGER PRIMARY KEY);",
			DownSQL: "DROP TABLE users;",
		})

		results := collectDoctorResults(context.Background(), q, doctorOptions{})
		if len(results) != 5 {
			t.Fatalf("len(results) = %d, want 5", len(results))
		}

		expected := []string{
			"Database Connection",
			"Migration Table",
			"Checksum Validation",
			"Gap Detection",
			"Registration Sync",
		}
		for i, want := range expected {
			if results[i].Check != want {
				t.Fatalf("results[%d].Check = %q, want %q", i, results[i].Check, want)
			}
		}
	})
}
