package cli

import (
	"fmt"
	"strings"
)

func migrationFilename(version, name string) string {
	return fmt.Sprintf("migrations/%s_%s.go", version, name)
}

func migrationVariableName(version, name string) string {
	return fmt.Sprintf("Migration%s%s", version, toPascalCase(name))
}

func generateSQLTemplate(version, name, variableName string) string {
	description := strings.ReplaceAll(name, "_", " ")

	return fmt.Sprintf(`package migrations

import "github.com/yaop-labs/queen"

// %s %s
var %s = queen.M{
	Version: "%s",
	Name:    "%s",
	UpSQL: `+"`"+`
		-- Write your migration here
		-- Example: CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255));
	`+"`"+`,
	DownSQL: `+"`"+`
		-- Write your rollback here
		-- Example: DROP TABLE users;
	`+"`"+`,
}
`, variableName, description, variableName, version, name)
}

func generateGoTemplate(version, name, variableName string) string {
	description := strings.ReplaceAll(name, "_", " ")
	upFuncName := fmt.Sprintf("up%s%s", version, toPascalCase(name))
	downFuncName := fmt.Sprintf("down%s%s", version, toPascalCase(name))

	return fmt.Sprintf(`package migrations

import (
	"context"
	"database/sql"

	"github.com/yaop-labs/queen"
)

// %s %s
var %s = queen.M{
	Version:        "%s",
	Name:           "%s",
	ManualChecksum: "v1", // Update this when you change the function
	UpFunc:         %s,
	DownFunc:       %s,
}

func %s(ctx context.Context, tx *sql.Tx) error {
	// TODO: Implement your migration logic
	// Example:
	// rows, err := tx.QueryContext(ctx, "SELECT id, name FROM users")
	// if err != nil {
	//     return err
	// }
	// defer rows.Close()
	//
	// for rows.Next() {
	//     var id int
	//     var name string
	//     if err := rows.Scan(&id, &name); err != nil {
	//         return err
	//     }
	//     // Process data...
	// }
	//
	// return rows.Err()
	return nil
}

func %s(ctx context.Context, tx *sql.Tx) error {
	// TODO: Implement your rollback logic
	return nil
}
`, variableName, description, variableName, version, name, upFuncName, downFuncName, upFuncName, downFuncName)
}

func toPascalCase(s string) string {
	parts := strings.Split(s, "_")
	for i, part := range parts {
		if len(part) > 0 {
			parts[i] = strings.ToUpper(part[:1]) + part[1:]
		}
	}
	return strings.Join(parts, "")
}
