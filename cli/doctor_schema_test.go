package cli

import (
	"reflect"
	"strings"
	"testing"
)

func TestExtractTables(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "plain create table",
			sql:  "CREATE TABLE users (id INT)",
			want: []string{"users"},
		},
		{
			name: "if not exists",
			sql:  "CREATE TABLE IF NOT EXISTS users (id INT)",
			want: []string{"users"},
		},
		{
			name: "schema qualified quoted identifiers",
			sql:  `CREATE TABLE "public"."Users" (id INT)`,
			want: []string{"public.users"},
		},
		{
			name: "bracket quoted identifiers",
			sql:  `CREATE TABLE [dbo].[Users] (id INT)`,
			want: []string{"dbo.users"},
		},
		{
			name: "multiple statements",
			sql:  "CREATE TABLE users (id INT); CREATE TABLE posts (id INT);",
			want: []string{"users", "posts"},
		},
		{
			name: "ignores operation in string literal",
			sql:  "SELECT 'CREATE TABLE fake (id INT)'; CREATE TABLE real_table (id INT);",
			want: []string{"real_table"},
		},
		{
			name: "ignores operation in comments",
			sql:  "-- CREATE TABLE fake (id INT)\n/* CREATE TABLE other_fake (id INT) */\nCREATE TABLE real_table (id INT);",
			want: []string{"real_table"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := extractTables(tt.sql, "CREATE TABLE")
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("extractTables() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestAnalyzeSQLIgnoresDestructiveKeywordsInCommentsAndStrings(t *testing.T) {
	t.Parallel()

	var issues []string
	analyzeSQL("SELECT 'DROP TABLE users'; -- TRUNCATE accounts", "001", true, make(map[string]string), &issues)
	if len(issues) != 0 {
		t.Fatalf("analyzeSQL reported false positive issues: %v", issues)
	}

	analyzeSQL("DROP TABLE users", "002", true, make(map[string]string), &issues)
	if len(issues) != 1 || !strings.Contains(issues[0], "DROP TABLE") {
		t.Fatalf("analyzeSQL did not report real destructive SQL: %v", issues)
	}
}
