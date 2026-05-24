package tap

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// OperationOf returns the first SQL keyword in lower case.
func OperationOf(sql string) string {
	sql = strings.TrimSpace(stripSQLComments(sql))
	if sql == "" {
		return ""
	}
	for i, r := range sql {
		if unicode.IsSpace(r) || r == '(' {
			return strings.ToLower(sql[:i])
		}
	}
	return strings.ToLower(sql)
}

// NormalizeSQL collapses whitespace and replaces literal values with
// placeholders so structurally identical queries can be grouped.
func NormalizeSQL(sql string) string {
	sql = stripSQLComments(sql)

	var b strings.Builder
	inString := false
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		if inString {
			if ch == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					i++
					continue
				}
				inString = false
			}
			continue
		}
		if ch == '\'' {
			b.WriteByte('?')
			inString = true
			continue
		}
		b.WriteByte(ch)
	}

	return collapseSQLWhitespace(replaceNumericLiterals(b.String()))
}

// BindSQL replaces positional placeholders in sql with formatted args. It
// supports PostgreSQL-style $1 placeholders and ? placeholders.
func BindSQL(sql string, args []any) string {
	if len(args) == 0 || sql == "" {
		return sql
	}

	var b strings.Builder
	nextQuestion := 0
	inString := false
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		if inString {
			b.WriteByte(ch)
			if ch == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					i++
					b.WriteByte(sql[i])
					continue
				}
				inString = false
			}
			continue
		}
		switch ch {
		case '\'':
			inString = true
			b.WriteByte(ch)
		case '?':
			if nextQuestion < len(args) {
				b.WriteString(formatArg(args[nextQuestion]))
				nextQuestion++
			} else {
				b.WriteByte(ch)
			}
		case '$':
			j := i + 1
			for j < len(sql) && sql[j] >= '0' && sql[j] <= '9' {
				j++
			}
			if j > i+1 {
				n, err := strconv.Atoi(sql[i+1 : j])
				if err == nil && n > 0 && n <= len(args) {
					b.WriteString(formatArg(args[n-1]))
					if n > nextQuestion {
						nextQuestion = n
					}
					i = j - 1
					continue
				}
			}
			b.WriteByte(ch)
		default:
			b.WriteByte(ch)
		}
	}
	return b.String()
}

func stripSQLComments(sql string) string {
	var b strings.Builder
	inString := false
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		if inString {
			b.WriteByte(ch)
			if ch == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					i++
					b.WriteByte(sql[i])
					continue
				}
				inString = false
			}
			continue
		}
		if ch == '\'' {
			inString = true
			b.WriteByte(ch)
			continue
		}
		if ch == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			if i < len(sql) {
				b.WriteByte('\n')
			}
			continue
		}
		if ch == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			i += 2
			for i+1 < len(sql) && (sql[i] != '*' || sql[i+1] != '/') {
				i++
			}
			i++
			b.WriteByte(' ')
			continue
		}
		b.WriteByte(ch)
	}
	return b.String()
}

func replaceNumericLiterals(sql string) string {
	var b strings.Builder
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		if isNumberStart(sql, i) {
			b.WriteByte('?')
			i++
			for i < len(sql) && ((sql[i] >= '0' && sql[i] <= '9') || sql[i] == '.') {
				i++
			}
			i--
			continue
		}
		b.WriteByte(ch)
	}
	return b.String()
}

func isNumberStart(sql string, i int) bool {
	ch := sql[i]
	if ch < '0' || ch > '9' {
		return false
	}
	if i > 0 {
		prev := rune(sql[i-1])
		if unicode.IsLetter(prev) || unicode.IsDigit(prev) || prev == '_' || prev == '$' {
			return false
		}
	}
	if i+1 < len(sql) {
		next := rune(sql[i+1])
		if unicode.IsLetter(next) || next == '_' {
			return false
		}
	}
	return true
}

func collapseSQLWhitespace(sql string) string {
	return strings.Join(strings.Fields(sql), " ")
}

func formatArg(v any) string {
	if v == nil {
		return "NULL"
	}
	if rv := reflect.ValueOf(v); rv.Kind() == reflect.Pointer && rv.IsNil() {
		return "NULL"
	}
	switch x := v.(type) {
	case string:
		return quoteSQLString(x)
	case []byte:
		return quoteSQLString(string(x))
	case time.Time:
		return quoteSQLString(x.Format(time.RFC3339Nano))
	case bool:
		if x {
			return "TRUE"
		}
		return "FALSE"
	case sql.NamedArg:
		return formatArg(x.Value)
	case driver.Valuer:
		v, err := x.Value()
		if err != nil {
			return quoteSQLString(fmt.Sprint(x))
		}
		return formatArg(v)
	case fmt.Stringer:
		return quoteSQLString(x.String())
	default:
		return fmt.Sprint(v)
	}
}

func quoteSQLString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
