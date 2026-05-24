package live

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"github.com/yaop-labs/queen/tap"
)

func (m *model) renderDetail(width, height int) []string {
	if height <= 0 {
		return nil
	}
	if m.selected < 0 || m.selected >= len(m.rows) {
		return padLines([]string{mutedStyle.Render("no event selected")}, height, width)
	}

	r := m.rows[m.selected]
	ev := r.ev
	lines := []string{
		kindStyle.Render("details"),
		field("migration", fmt.Sprintf("%s %s", ev.Version, ev.Name)),
		field("kind", string(ev.Kind)+" "+string(ev.Direction)),
		field("time", fmt.Sprintf("%s  duration %s", r.atRel.Round(time.Millisecond), ev.Duration.Round(time.Microsecond))),
	}
	if ev.Kind == tap.KindExec {
		lines = append(lines,
			field("operation", blankFallback(ev.Operation, "-")),
			field("rows", fmt.Sprint(ev.RowsAffected)),
		)
		if ev.Slow || ev.NPlus1 {
			var flags []string
			if ev.Slow {
				flags = append(flags, warnStyle.Render("SLOW"))
			}
			if ev.NPlus1 {
				flags = append(flags, nplusStyle.Render(fmt.Sprintf("N+1/%d", ev.NPlus1Count)))
			}
			lines = append(lines, field("flags", strings.Join(flags, " ")))
		}
	}
	if ev.Error != "" {
		lines = append(lines, field("error", errStyle.Render(ev.Error)))
	}
	sql := ev.BoundSQL
	if sql == "" {
		sql = ev.SQL
	}
	if sql != "" {
		lines = append(lines, mutedStyle.Render("sql"))
		lines = append(lines, wrapLines(singleLine(sql), width)...)
	}

	if m.wideLayout() {
		contentWidth := max(1, width-4)
		contentHeight := max(1, height-2)
		rendered := panelStyle.Width(contentWidth).Height(contentHeight).Render(strings.Join(padLines(lines, contentHeight, contentWidth), "\n"))
		return padLines(strings.Split(rendered, "\n"), height, width)
	}
	return padLines(lines, height, width)
}

func field(label, value string) string {
	return mutedStyle.Render(label+": ") + value
}

func (m *model) renderFooter(width int) string {
	var left string
	switch {
	case m.runErr != nil:
		left = errStyle.Render("failed") + " " + footerStyle.Render(m.runErr.Error())
	case m.finished:
		left = okStyle.Render("finished") + " " + footerStyle.Render("review events, then quit")
	default:
		left = footerStyle.Render("running")
	}
	if d := m.sink.Dropped(); d > 0 {
		left += " " + errStyle.Render(fmt.Sprintf("dropped:%d", d))
	}

	keys := []string{
		keyHint("j/k", "move"),
		keyHint("f/b", "page"),
		keyHint("g/G", "top/bottom"),
		keyHint("q", "quit"),
	}
	right := strings.Join(keys, "  ")
	gap := width - lipgloss.Width(left) - lipgloss.Width(right)
	if gap < 1 {
		gap = 1
	}
	return truncate(left+strings.Repeat(" ", gap)+right, width)
}

func keyHint(key, label string) string {
	return footerKeyStyle.Render(key) + footerStyle.Render(" "+label)
}

func (m *model) state() (string, lipgloss.Style) {
	switch {
	case m.runErr != nil:
		return "FAILED", errStyle
	case m.finished:
		return "DONE", okStyle
	default:
		return "RUNNING", execStyle
	}
}
