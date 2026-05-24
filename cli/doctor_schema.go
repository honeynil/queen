package cli

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/yaop-labs/queen"
)

// checkSchemaConsistency looks for schema problems that need SQL inspection.
func checkSchemaConsistency(ctx context.Context, q *queen.Queen) DoctorResult {
	upPlans, err := q.DryRun(ctx, queen.DirectionUp, 0)
	if err != nil {
		// no pending migrations is acceptable; still analyze down plans
		upPlans = nil
	}

	downPlans, err := q.DryRun(ctx, queen.DirectionDown, 0)
	if err != nil {
		downPlans = nil
	}

	statuses, err := q.Status(ctx)
	if err != nil {
		return DoctorResult{
			Check:   "Schema Consistency",
			Status:  statusFail,
			Message: "Failed to get migration statuses",
			Details: err.Error(),
		}
	}

	var issues []string

	upTables := make(map[string]string)
	downTables := make(map[string]string)

	for _, plan := range upPlans {
		analyzeSQL(plan.SQL, plan.Version, true, upTables, &issues)
	}

	for _, plan := range downPlans {
		analyzeSQL(plan.SQL, plan.Version, false, downTables, &issues)
	}

	noRollbackCount := 0
	for _, s := range statuses {
		if !s.HasRollback {
			noRollbackCount++
		}
	}
	if noRollbackCount > 0 {
		issues = append(issues, fmt.Sprintf("%d migration(s) have no rollback defined", noRollbackCount))
	}

	tableCreators := make(map[string][]string)
	for _, plan := range upPlans {
		for _, table := range extractTables(plan.SQL, "CREATE TABLE") {
			tableCreators[table] = append(tableCreators[table], plan.Version)
		}
	}
	for table, versions := range tableCreators {
		if len(versions) > 1 {
			issues = append(issues, fmt.Sprintf("Table %q created in multiple migrations: %s", table, strings.Join(versions, ", ")))
		}
	}

	if len(issues) == 0 {
		return DoctorResult{
			Check:   "Schema Consistency",
			Status:  statusPass,
			Message: "No schema consistency issues found",
		}
	}

	status := statusWarning
	return DoctorResult{
		Check:   "Schema Consistency",
		Status:  status,
		Message: fmt.Sprintf("Found %d schema consistency issue(s)", len(issues)),
		Details: strings.Join(issues, "\n"),
	}
}

func analyzeSQL(sql string, version string, isUp bool, tables map[string]string, issues *[]string) {
	if sql == "" {
		return
	}

	searchSQL := sqlForKeywordSearch(sql)

	if isUp {
		destructive := []string{"DROP TABLE", "DROP DATABASE", "DROP SCHEMA", "TRUNCATE"}
		for _, keyword := range destructive {
			if containsSQLOperation(searchSQL, keyword) {
				*issues = append(*issues, fmt.Sprintf("Migration %s: destructive operation %q in up SQL", version, keyword))
			}
		}
	}

	for _, table := range extractTables(sql, "CREATE TABLE") {
		if existing, ok := tables[table]; ok && isUp {
			*issues = append(*issues, fmt.Sprintf("Migration %s: table %q already created in %s", version, table, existing))
		}
		tables[table] = version
	}
}

// extractTables extracts table names from SQL for a given operation (e.g. "CREATE TABLE").
func extractTables(sql string, operation string) []string {
	var tables []string

	for _, end := range sqlOperationEndIndexes(sql, operation) {
		rest := skipOptionalExistenceClause(sql[end:])
		tableName := extractIdentifier(rest)
		if tableName != "" {
			tables = append(tables, strings.ToLower(tableName))
		}
	}

	return tables
}

func containsSQLOperation(sql string, operation string) bool {
	return len(sqlOperationEndIndexes(sql, operation)) > 0
}

func sqlOperationEndIndexes(sql string, operation string) []int {
	searchSQL := sqlForKeywordSearch(sql)
	pattern := regexp.MustCompile(`(?i)\b` + strings.Join(strings.Fields(operation), `\s+`) + `\b`)

	matches := pattern.FindAllStringIndex(searchSQL, -1)
	indexes := make([]int, 0, len(matches))
	for _, match := range matches {
		indexes = append(indexes, match[1])
	}
	return indexes
}

func sqlForKeywordSearch(sql string) string {
	var b strings.Builder
	b.Grow(len(sql))

	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		if ch == '\'' {
			b.WriteByte(' ')
			i++
			for i < len(sql) {
				b.WriteByte(' ')
				if sql[i] == '\'' {
					if i+1 < len(sql) && sql[i+1] == '\'' {
						i++
						b.WriteByte(' ')
						i++
						continue
					}
					break
				}
				i++
			}
			continue
		}
		if ch == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			b.WriteString("  ")
			i += 2
			for i < len(sql) && sql[i] != '\n' {
				b.WriteByte(' ')
				i++
			}
			if i < len(sql) {
				b.WriteByte(sql[i])
			}
			continue
		}
		if ch == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			b.WriteString("  ")
			i += 2
			for i+1 < len(sql) && (sql[i] != '*' || sql[i+1] != '/') {
				b.WriteByte(' ')
				i++
			}
			if i+1 < len(sql) {
				b.WriteString("  ")
				i++
			}
			continue
		}
		b.WriteByte(ch)
	}
	return b.String()
}

func skipOptionalExistenceClause(s string) string {
	s = strings.TrimLeftFunc(s, unicode.IsSpace)

	fields := strings.Fields(s)
	if len(fields) >= 2 && strings.EqualFold(fields[0], "IF") && strings.EqualFold(fields[1], "EXISTS") {
		return trimLeadingFields(s, 2)
	}
	if len(fields) >= 3 && strings.EqualFold(fields[0], "IF") && strings.EqualFold(fields[1], "NOT") && strings.EqualFold(fields[2], "EXISTS") {
		return trimLeadingFields(s, 3)
	}
	return s
}

func trimLeadingFields(s string, n int) string {
	for n > 0 {
		s = strings.TrimLeftFunc(s, unicode.IsSpace)
		for len(s) > 0 {
			r, size := rune(s[0]), 1
			if r >= utf8.RuneSelf {
				r, size = utf8.DecodeRuneInString(s)
			}
			if unicode.IsSpace(r) {
				break
			}
			s = s[size:]
		}
		n--
	}
	return strings.TrimLeftFunc(s, unicode.IsSpace)
}

// extractIdentifier extracts a SQL identifier (table name) from the beginning of a string.
func extractIdentifier(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}

	parts := make([]string, 0, 2)
	for {
		part, rest, ok := consumeIdentifierPart(s)
		if !ok {
			break
		}
		parts = append(parts, part)
		rest = strings.TrimLeftFunc(rest, unicode.IsSpace)
		if !strings.HasPrefix(rest, ".") {
			break
		}
		s = strings.TrimLeftFunc(rest[1:], unicode.IsSpace)
	}

	return strings.Join(parts, ".")
}

func consumeIdentifierPart(s string) (string, string, bool) {
	s = strings.TrimLeftFunc(s, unicode.IsSpace)
	if s == "" {
		return "", "", false
	}

	switch s[0] {
	case '"', '`':
		return consumeDelimitedIdentifier(s, s[0], s[0])
	case '[':
		return consumeDelimitedIdentifier(s, '[', ']')
	default:
		return consumeBareIdentifier(s)
	}
}

func consumeDelimitedIdentifier(s string, open byte, close byte) (string, string, bool) {
	if len(s) == 0 || s[0] != open {
		return "", s, false
	}

	var b strings.Builder
	for i := 1; i < len(s); i++ {
		if s[i] == close {
			if i+1 < len(s) && s[i+1] == close {
				b.WriteByte(close)
				i++
				continue
			}
			return b.String(), s[i+1:], true
		}
		b.WriteByte(s[i])
	}
	return "", s, false
}

func consumeBareIdentifier(s string) (string, string, bool) {
	var b strings.Builder
	for i, r := range s {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' && r != '$' {
			if i == 0 {
				return "", s, false
			}
			return b.String(), s[i:], true
		}
		b.WriteRune(r)
	}
	if b.Len() == 0 {
		return "", s, false
	}
	return b.String(), "", true
}
