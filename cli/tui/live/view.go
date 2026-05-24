package live

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"github.com/yaop-labs/queen/tap"
)

func (m *model) View() string {
	var b strings.Builder
	width := m.safeWidth()

	b.WriteString(m.renderHeader(width))
	b.WriteString("\n")
	b.WriteString(m.renderMetrics(width))
	b.WriteString("\n")
	b.WriteString(dimStyle.Render(strings.Repeat("─", width)))
	b.WriteString("\n")

	for _, line := range m.renderBody(width) {
		b.WriteString(line)
		b.WriteString("\n")
	}

	b.WriteString(dimStyle.Render(strings.Repeat("─", width)))
	b.WriteString("\n")
	b.WriteString(m.renderFooter(width))

	return b.String()
}

func (m *model) renderHeader(width int) string {
	stateText, stateStyle := m.state()
	left := fmt.Sprintf("%s %s",
		titleStyle.Render("queen tap"),
		stateStyle.Render(stateText),
	)
	elapsed := mutedStyle.Render("elapsed " + m.elapsed().Round(time.Millisecond).String())
	gap := width - lipgloss.Width(left) - lipgloss.Width(elapsed)
	if gap < 1 {
		gap = 1
	}
	line := left + strings.Repeat(" ", gap) + elapsed
	return truncate(line, width)
}

func (m *model) renderMetrics(width int) string {
	progressWidth := width - 68
	if progressWidth < 12 {
		progressWidth = 12
	}
	if progressWidth > 32 {
		progressWidth = 32
	}

	metrics := []string{
		metric("migrations", fmt.Sprintf("%d/%d", m.migFinished, m.migStarted), okStyle),
		metric("exec", fmt.Sprint(m.execs), execStyle),
		metric("slow", fmt.Sprint(m.slow), warnStyle),
		metric("n+1", fmt.Sprint(m.nplus1), nplusStyle),
		metric("errors", fmt.Sprint(m.errors), errStyle),
	}
	row := strings.Join(metrics, "  ")
	if m.migStarted > 0 {
		row += "  " + m.progressBar(progressWidth)
	}
	return truncate(row, width)
}

func metric(label, value string, valueStyle lipgloss.Style) string {
	return mutedStyle.Render(label) + " " + valueStyle.Render(value)
}

func (m *model) progressBar(width int) string {
	if width <= 0 {
		return ""
	}
	done := 0
	if m.migStarted > 0 {
		done = width * m.migFinished / m.migStarted
	}
	if done > width {
		done = width
	}
	bar := okStyle.Render(strings.Repeat("█", done)) + dimStyle.Render(strings.Repeat("░", width-done))
	return mutedStyle.Render("[") + bar + mutedStyle.Render("]")
}

func (m *model) renderBody(width int) []string {
	bodyHeight := m.contentHeight()
	if bodyHeight < 1 {
		bodyHeight = 1
	}

	if len(m.rows) == 0 {
		return padLines([]string{mutedStyle.Render("waiting for migration events...")}, bodyHeight, width)
	}

	if m.wideLayout() {
		detailWidth := clamp(width/3, 36, 52)
		separator := dimStyle.Render(" │ ")
		streamWidth := width - detailWidth - lipgloss.Width(separator)
		stream := m.renderStream(streamWidth, bodyHeight)
		detail := m.renderDetail(detailWidth, bodyHeight)
		out := make([]string, bodyHeight)
		for i := 0; i < bodyHeight; i++ {
			out[i] = stream[i] + separator + detail[i]
		}
		return out
	}

	streamHeight := bodyHeight
	detailHeight := m.detailHeight()
	if detailHeight > 0 {
		streamHeight = bodyHeight - detailHeight - 1
	}
	stream := m.renderStream(width, streamHeight)
	if detailHeight == 0 {
		return stream
	}
	out := append(stream, dimStyle.Render(strings.Repeat("·", width)))
	out = append(out, m.renderDetail(width, detailHeight)...)
	return out
}

func (m *model) renderStream(width, height int) []string {
	viewRows := m.renderRows(width)
	start := clamp(m.scroll, 0, len(viewRows))
	end := min(len(viewRows), start+height)
	lines := make([]string, 0, height)
	for _, vr := range viewRows[start:end] {
		marker := "  "
		line := truncate(vr.line, max(1, width-2))
		if vr.index == m.selected {
			marker = markerStyle.Render("▸ ")
			line = selectedStyle.Render(padRight(line, max(1, width-2)))
		}
		lines = append(lines, truncate(marker+line, width))
	}
	return padLines(lines, height, width)
}

func (m *model) renderRows(width int) []viewRow {
	out := make([]viewRow, 0, len(m.rows))
	for i, r := range m.rows {
		prefix := ""
		root := r.ev.Kind == tap.KindStart
		if !root {
			if r.ev.Kind == tap.KindEnd || m.isLastChild(i) {
				prefix = "  └─ "
			} else {
				prefix = "  ├─ "
			}
		}
		out = append(out, viewRow{index: i, row: r, line: m.renderRow(r, prefix, root, width)})
	}
	return out
}

func (m *model) renderRow(r row, prefix string, root bool, width int) string {
	ts := dimStyle.Render(fmt.Sprintf("%7s", r.atRel.Round(time.Millisecond)))
	versionName := fmt.Sprintf("%s %s", r.ev.Version, r.ev.Name)

	var kind, detail string
	switch r.ev.Kind {
	case tap.KindStart:
		kind = kindStyle.Render("▶ migration")
		detail = fmt.Sprintf("%s %s", versionName, dimStyle.Render("("+string(r.ev.Direction)+")"))
	case tap.KindEnd:
		if r.ev.Error != "" {
			kind = errStyle.Render("✖ end ")
			detail = fmt.Sprintf("%s  %s",
				dimStyle.Render(r.ev.Duration.Round(time.Millisecond).String()),
				errStyle.Render(r.ev.Error))
		} else {
			kind = okStyle.Render("✓ end ")
			detail = dimStyle.Render(r.ev.Duration.Round(time.Millisecond).String())
		}
	case tap.KindExec:
		kind = execStyle.Render("• sql")
		sql := singleLine(r.ev.SQL)
		flags := make([]string, 0, 2)
		if r.ev.Slow {
			flags = append(flags, warnStyle.Render("SLOW"))
		}
		if r.ev.NPlus1 {
			flags = append(flags, nplusStyle.Render(fmt.Sprintf("N+1/%d", r.ev.NPlus1Count)))
		}
		status := ""
		if len(flags) > 0 {
			status = "  " + strings.Join(flags, " ")
		}
		extras := dimStyle.Render(fmt.Sprintf("%s  rows=%d  op=%s",
			r.ev.Duration.Round(time.Microsecond),
			r.ev.RowsAffected,
			r.ev.Operation)) + status
		if r.ev.Error != "" {
			extras = errStyle.Render(r.ev.Error)
		}
		detail = fmt.Sprintf("%s  %s", extras, sql)
	default:
		kind = kindStyle.Render("? " + string(r.ev.Kind))
		if root {
			detail = versionName
		}
	}

	line := fmt.Sprintf("%s  %s%s %s", ts, dimStyle.Render(prefix), kind, detail)
	return truncate(line, width)
}
