package tap

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// WriteJSONL writes events as newline-delimited JSON.
func WriteJSONL(w io.Writer, events []Event) error {
	enc := json.NewEncoder(w)
	for _, e := range events {
		if err := enc.Encode(e); err != nil {
			return err
		}
	}
	return nil
}

// WriteMarkdown writes a compact migration tap report.
func WriteMarkdown(w io.Writer, events []Event) error {
	s := Summarize(events)
	if _, err := fmt.Fprintf(w, "# Queen Tap Report\n\nCaptured %d events, %d execs, %d errors.\n\n", s.Total, s.Execs, s.Errors); err != nil {
		return err
	}
	if len(s.Queries) > 0 {
		if _, err := io.WriteString(w, "## Query Summary\n\n| Count | Total | Avg | Max | Flags | SQL |\n|---:|---:|---:|---:|---|---|\n"); err != nil {
			return err
		}
		for _, q := range s.Queries {
			flags := make([]string, 0, 3)
			if q.Errors > 0 {
				flags = append(flags, "error")
			}
			if q.Slow > 0 {
				flags = append(flags, "slow")
			}
			if q.NPlus1 > 0 {
				flags = append(flags, "n+1")
			}
			sql := q.SQLTemplate
			if sql == "" {
				sql = q.SQL
			}
			if _, err := fmt.Fprintf(w, "| %d | %s | %s | %s | %s | `%s` |\n",
				q.Count,
				q.TotalDuration,
				q.AvgDuration,
				q.MaxDuration,
				strings.Join(flags, ", "),
				escapeMarkdownCode(sql),
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func escapeMarkdownCode(s string) string {
	return strings.ReplaceAll(s, "`", "\\`")
}
