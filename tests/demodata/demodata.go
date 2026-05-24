// Package demodata registers one shared migration story for Queen demos.
package demodata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/yaop-labs/queen"
	"github.com/yaop-labs/queen/tap"
)

const IntegratedTapStartVersion = "005"

type Options struct {
	OmitVersions   []string
	IncludeFailure bool
	StepDelay      time.Duration
}

func MainTUIOptions() Options {
	return Options{
		OmitVersions: []string{"003", "007"},
		StepDelay:    140 * time.Millisecond,
	}
}

func TapLiveOptions() Options {
	return Options{
		IncludeFailure: true,
		StepDelay:      120 * time.Millisecond,
	}
}

//nolint:gocyclo // Demo data intentionally keeps related scenarios together for the TUI fixtures.
func Register(q *queen.Queen, opts Options) {
	omitted := make(map[string]bool, len(opts.OmitVersions))
	for _, version := range opts.OmitVersions {
		omitted[version] = true
	}
	add := func(m queen.M) {
		if !omitted[m.Version] {
			q.MustAdd(m)
		}
	}
	sleep := func(multiplier int) {
		if opts.StepDelay > 0 {
			time.Sleep(time.Duration(multiplier) * opts.StepDelay)
		}
	}

	add(queen.M{
		Version: "001",
		Name:    "create_users",
		UpSQL: `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				email TEXT NOT NULL,
				name TEXT,
				created_at TEXT
			)
		`,
		DownSQL: `DROP TABLE users`,
	})

	add(queen.M{
		Version: "002",
		Name:    "create_posts",
		UpSQL: `
			CREATE TABLE posts (
				id INTEGER PRIMARY KEY,
				user_id INTEGER NOT NULL,
				title TEXT NOT NULL,
				body TEXT,
				published_at TEXT
			)
		`,
		DownSQL: `DROP TABLE posts`,
	})

	add(queen.M{
		Version:        "003",
		Name:           "seed_users",
		ManualChecksum: "demo-003-v1",
		UpFunc: func(ctx context.Context, tx *sql.Tx) error {
			t := tap.ObserveTx(ctx, tx)
			users := []struct {
				email string
				name  string
			}{
				{"alice@example.com", "Alice"},
				{"bob@example.com", "Bob"},
				{"carol@example.com", "Carol"},
				{"dan@example.com", "Dan"},
				{"eve@example.com", "Eve"},
				{"frida@example.com", "Frida"},
				{"grace@example.com", "Grace"},
				{"heidi@example.com", "Heidi"},
			}
			for _, u := range users {
				if _, err := t.ExecContext(ctx,
					`INSERT INTO users (email, name, created_at) VALUES (?, ?, datetime('now'))`,
					u.email, u.name,
				); err != nil {
					return err
				}
				sleep(1)
			}
			return nil
		},
		DownFunc: func(ctx context.Context, tx *sql.Tx) error {
			t := tap.ObserveTx(ctx, tx)
			_, err := t.ExecContext(ctx, `DELETE FROM users WHERE email LIKE '%@example.com'`)
			return err
		},
	})

	add(queen.M{
		Version:        "004",
		Name:           "seed_reference_posts",
		ManualChecksum: "demo-004-v1",
		UpFunc: func(ctx context.Context, tx *sql.Tx) error {
			t := tap.ObserveTx(ctx, tx)
			if _, err := t.ExecContext(ctx, `
				INSERT INTO users (email, name, created_at) VALUES
					('support@example.com', 'Support', datetime('now')),
					('ops@example.com', 'Ops', datetime('now')),
					('billing@example.com', 'Billing', datetime('now'))
			`); err != nil {
				return err
			}
			sleep(1)

			rows, err := t.QueryContext(ctx, `SELECT id FROM users ORDER BY id LIMIT 3`)
			if err != nil {
				return err
			}
			defer func() { _ = rows.Close() }()
			var ids []int64
			for rows.Next() {
				var id int64
				if err := rows.Scan(&id); err != nil {
					return err
				}
				ids = append(ids, id)
			}
			if err := rows.Err(); err != nil {
				return err
			}

			for _, id := range ids {
				if _, err := t.ExecContext(ctx,
					`INSERT INTO posts (user_id, title, body, published_at) VALUES (?, ?, ?, datetime('now'))`,
					id, fmt.Sprintf("Post by user #%d", id), "hello from the demo",
				); err != nil {
					return err
				}
				sleep(1)
			}
			_, err = t.ExecContext(ctx, `UPDATE users SET email = LOWER(email)`)
			sleep(1)
			return err
		},
		DownFunc: func(ctx context.Context, tx *sql.Tx) error {
			t := tap.ObserveTx(ctx, tx)
			if _, err := t.ExecContext(ctx, `DELETE FROM posts WHERE body = 'hello from the demo'`); err != nil {
				return err
			}
			_, err := t.ExecContext(ctx, `DELETE FROM users WHERE email IN ('support@example.com', 'ops@example.com', 'billing@example.com')`)
			return err
		},
	})

	add(queen.M{
		Version:        IntegratedTapStartVersion,
		Name:           "add_user_profile_columns",
		ManualChecksum: "demo-005-v1",
		UpFunc: func(ctx context.Context, tx *sql.Tx) error {
			t := tap.ObserveTx(ctx, tx)
			if _, err := t.ExecContext(ctx, `ALTER TABLE users ADD COLUMN timezone TEXT DEFAULT 'UTC'`); err != nil {
				return err
			}
			sleep(1)
			if _, err := t.ExecContext(ctx, `ALTER TABLE users ADD COLUMN last_seen_at TEXT`); err != nil {
				return err
			}
			sleep(1)
			_, err := t.ExecContext(ctx, `UPDATE users SET timezone = 'Europe/Moscow', last_seen_at = datetime('now') WHERE email LIKE '%@example.com'`)
			sleep(1)
			return err
		},
	})

	add(queen.M{
		Version: "006",
		Name:    "add_unique_user_email_index",
		UpSQL:   `CREATE UNIQUE INDEX idx_users_email ON users(email)`,
		DownSQL: `DROP INDEX idx_users_email`,
	})

	add(queen.M{
		Version: "007",
		Name:    "create_comments",
		UpSQL: `
			CREATE TABLE comments (
				id INTEGER PRIMARY KEY,
				post_id INTEGER NOT NULL,
				body TEXT NOT NULL,
				created_at TEXT
			)
		`,
		DownSQL: `DROP TABLE comments`,
	})

	add(queen.M{
		Version: "008",
		Name:    "add_post_search_index",
		UpSQL:   `CREATE INDEX idx_posts_title ON posts(title)`,
		DownSQL: `DROP INDEX idx_posts_title`,
	})

	add(queen.M{
		Version:        "009",
		Name:           "backfill_post_titles",
		ManualChecksum: "demo-009-v1",
		UpFunc: func(ctx context.Context, tx *sql.Tx) error {
			t := tap.ObserveTx(ctx, tx)
			rows, err := t.QueryContext(ctx, `SELECT id, title FROM posts ORDER BY id`)
			if err != nil {
				return err
			}
			defer func() { _ = rows.Close() }()
			type post struct {
				id    int64
				title string
			}
			var posts []post
			for rows.Next() {
				var p post
				if err := rows.Scan(&p.id, &p.title); err != nil {
					return err
				}
				posts = append(posts, p)
			}
			if err := rows.Err(); err != nil {
				return err
			}
			for _, p := range posts {
				if _, err := t.ExecContext(ctx, `UPDATE posts SET title = TRIM(?) WHERE id = ?`, p.title, p.id); err != nil {
					return err
				}
				sleep(1)
			}
			return nil
		},
	})

	add(queen.M{
		Version: "010",
		Name:    "create_audit_events",
		UpSQL: `
			CREATE TABLE audit_events (
				id INTEGER PRIMARY KEY,
				actor TEXT NOT NULL,
				action TEXT NOT NULL,
				created_at TEXT NOT NULL
			)
		`,
		DownSQL: `DROP TABLE audit_events`,
	})

	add(queen.M{
		Version:        "011",
		Name:           "seed_audit_events",
		ManualChecksum: "demo-011-v1",
		UpFunc: func(ctx context.Context, tx *sql.Tx) error {
			t := tap.ObserveTx(ctx, tx)
			events := []struct {
				actor  string
				action string
			}{
				{"system", "migration_started"},
				{"system", "migration_completed"},
				{"queen", "tap_stream_rendered"},
			}
			for _, e := range events {
				if _, err := t.ExecContext(ctx,
					`INSERT INTO audit_events (actor, action, created_at) VALUES (?, ?, datetime('now'))`,
					e.actor, e.action,
				); err != nil {
					return err
				}
				sleep(1)
			}
			return nil
		},
		DownFunc: func(ctx context.Context, tx *sql.Tx) error {
			t := tap.ObserveTx(ctx, tx)
			_, err := t.ExecContext(ctx, `DELETE FROM audit_events WHERE actor IN ('system', 'queen')`)
			return err
		},
	})

	add(queen.M{
		Version:        "012",
		Name:           "cleanup_legacy_guest_users",
		ManualChecksum: "demo-012-v1",
		UpFunc: func(ctx context.Context, tx *sql.Tx) error {
			t := tap.ObserveTx(ctx, tx)
			if _, err := t.ExecContext(ctx, `INSERT INTO users (email, name, created_at) VALUES ('old@guest.local', 'Legacy Guest', datetime('now'))`); err != nil {
				return err
			}
			sleep(1)
			_, err := t.ExecContext(ctx, `DELETE FROM users WHERE email LIKE '%@guest.local'`)
			return err
		},
	})

	add(queen.M{
		Version:        "013",
		Name:           "nplus1_user_lookups",
		ManualChecksum: "demo-013-v1",
		UpFunc: func(ctx context.Context, tx *sql.Tx) error {
			t := tap.ObserveTx(ctx, tx)
			for id := 1; id <= 8; id++ {
				var email string
				if err := t.QueryRowContext(ctx, `SELECT email FROM users WHERE id = ?`, id).Scan(&email); err != nil {
					if errors.Is(err, sql.ErrNoRows) {
						continue
					}
					return err
				}
				sleep(1)
			}
			return nil
		},
	})

	add(queen.M{
		Version:        "014",
		Name:           "slow_post_counts",
		ManualChecksum: "demo-014-v1",
		UpFunc: func(ctx context.Context, tx *sql.Tx) error {
			t := tap.ObserveTx(ctx, tx)
			for userID := 1; userID <= 3; userID++ {
				var n int
				row := t.QueryRowContext(ctx, `SELECT COUNT(*) FROM posts WHERE user_id = ?`, userID)
				sleep(1)
				if err := row.Scan(&n); err != nil {
					return err
				}
				if sink := tap.SinkFromContext(ctx); sink != nil {
					sink.Emit(tap.Event{
						Kind:      tap.KindExec,
						Version:   "014",
						Name:      "slow_post_counts",
						Direction: tap.DirectionUp,
						StartedAt: time.Now(),
						SQL:       fmt.Sprintf("/* counted %d posts for user %d */", n, userID),
					})
				}
			}
			return nil
		},
	})

	if opts.IncludeFailure {
		add(queen.M{
			Version:        "015",
			Name:           "intentional_failure",
			ManualChecksum: "demo-015-v1",
			UpFunc: func(ctx context.Context, tx *sql.Tx) error {
				t := tap.ObserveTx(ctx, tx)
				if _, err := t.ExecContext(ctx,
					`INSERT INTO users (email, name, created_at) VALUES (?, ?, datetime('now'))`,
					"demo-failure@example.com", "Failure Preview",
				); err != nil {
					return err
				}
				sleep(2)
				_, err := t.ExecContext(ctx,
					`INSERT INTO users (email, name, created_at) VALUES (?, ?, datetime('now'))`,
					"support@example.com", "Duplicate Support",
				)
				sleep(1)
				return err
			},
		})
	}
}
