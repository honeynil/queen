package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/yaop-labs/queen"
)

// View renders the main cockpit. The layout is intentionally component based:
// list, detail, and tap/explain each own their screen area.
func (m *Model) View() string {
	if m.quitting {
		return ""
	}
	m.ensurePanels()
	m.resizePanels()
	m.refreshRows()
	m.refreshDetail()
	m.refreshTap()

	w, h := m.effectiveSize()
	sections := []string{m.renderTopBar(w), m.renderTabs(w)}
	if m.filterVisible() {
		sections = append(sections, m.renderFilterBar(w))
	}
	if m.message != "" {
		sections = append(sections, m.renderMessageBar(w))
	}
	sections = append(sections, m.renderBody(w, h-lipgloss.Height(strings.Join(sections, "\n"))-1))
	sections = append(sections, m.renderFooter(w))
	return strings.Join(sections, "\n")
}

func (m *Model) renderTopBar(width int) string {
	if width < 1 {
		width = 1
	}
	if width >= 96 {
		return m.renderASCIITopBar(width)
	}

	state, stateStyle := m.releaseState()
	lines := []string{
		placeApartPlain(AppTitleStyle.Render("QUEEN"), stateStyle.Render(strings.ToUpper(state)), width),
		m.renderMetricLine(width),
		m.renderContextLine(width),
		m.renderTapStateLine(width),
	}
	return renderBarLines(lines, width)
}

func topBarHeight(width int) int {
	if width >= 96 {
		return len(queenBannerLines)
	}
	return 4
}

func (m *Model) renderASCIITopBar(width int) string {
	logo := strings.Split(RenderBanner(), "\n")
	lines := append([]string(nil), logo...)
	return renderBarLines(lines, width)
}

func (m *Model) releaseState() (string, lipgloss.Style) {
	_, pending, modified, destructive := m.migrationCounts()
	gapErrors := 0
	for _, g := range m.gaps {
		if strings.EqualFold(g.Severity, "error") {
			gapErrors++
		}
	}
	switch {
	case m.err != nil:
		return "error", ErrorStyle
	case modified > 0:
		return "checksum drift", ErrorStyle
	case gapErrors > 0:
		return "blocked gaps", ErrorStyle
	case destructive > 0 && pending > 0:
		return "review destructive", ErrorStyle
	case pending > 0:
		return "pending", StatusPillStyle
	case len(m.gaps) > 0:
		return "gaps", WarningMsgStyle
	default:
		return "up to date", AppliedStyle
	}
}

func (m *Model) renderMetricLine(width int) string {
	applied, pending, modified, destructive := m.migrationCounts()
	parts := []string{
		AppliedStyle.Render(fmt.Sprintf("applied %d", applied)),
		PendingStyle.Render(fmt.Sprintf("pending %d", pending)),
		DetailStyle.Render(fmt.Sprintf("gaps %d", len(m.gaps))),
	}
	if modified > 0 {
		parts = append(parts, WarningMsgStyle.Render(fmt.Sprintf("modified %d", modified)))
	}
	if destructive > 0 {
		parts = append(parts, ErrorStyle.Render(fmt.Sprintf("destructive %d", destructive)))
	}
	if m.loading {
		parts = append(parts, StatusInfoStyle.Render(m.spinner.View()+" loading"))
	}
	return truncate(strings.Join(parts, DetailStyle.Render("  |  ")), width)
}

func (m *Model) renderContextLine(width int) string {
	prefix := HelpHintStyle.Render("next ")
	var context string
	switch m.viewMode {
	case ViewHelp:
		context = "read shortcuts and workflow"
	case ViewGaps:
		if gap, ok := m.selectedGap(); ok {
			context = fmt.Sprintf("fill or ignore gap %s · %s", gap.Version, gapTypeLabel(gap.Type))
		} else {
			context = "no gaps in current filter"
		}
	default:
		if m.explainActive() {
			context = "explain is isolated in the right panel · esc returns to tap"
			break
		}
		if mig, _, ok := m.selectedMigration(); ok {
			switch mig.Status {
			case queen.StatusApplied:
				if mig.HasRollback {
					context = fmt.Sprintf("rollback from %s · enter", mig.Version)
				} else {
					context = fmt.Sprintf("%s is applied without rollback", mig.Version)
				}
			case queen.StatusModified:
				context = fmt.Sprintf("inspect checksum drift in %s", mig.Version)
			default:
				action := NameStyle.Render("apply")
				if mig.Destructive {
					action = ErrorStyle.Render("review destructive apply")
				}
				return truncate(prefix+action+NameStyle.Render(fmt.Sprintf(" %s · enter", mig.Version)), width)
			}
		} else {
			context = "no migrations in current filter"
		}
	}
	return truncate(prefix+NameStyle.Render(context), width)
}

func (m *Model) renderTapStateLine(width int) string {
	if len(m.tapEvents) == 0 && !m.showTap {
		return truncate(DetailStyle.Render("tap idle · operations will open a live SQL trace"), width)
	}
	mode := "live"
	modeStyle := AppliedStyle
	if !m.tapFollow {
		mode = "paused"
		modeStyle = WarningMsgStyle
	}
	label := "tap"
	if m.explainActive() {
		label = "explain"
		mode = "pinned"
		modeStyle = StatusInfoStyle
	}
	operation := m.operationLabel
	if operation == "" {
		operation = "no active operation"
	}
	line := StatusInfoStyle.Render(label) + DetailStyle.Render(" | ") +
		modeStyle.Render(mode) + DetailStyle.Render(fmt.Sprintf(" | events %d | %s", len(m.tapEvents), operation))
	return truncate(line, width)
}

func (m *Model) renderTabs(width int) string {
	tab := func(key, label string, active bool) string {
		text := fmt.Sprintf(" %s %s ", key, label)
		if active {
			return ActiveTabStyle.Render(text)
		}
		return InactiveTabStyle.Render(text)
	}
	parts := []string{
		tab("1", "Migrations", m.viewMode == ViewMigrations),
		tab("2", "Gaps", m.viewMode == ViewGaps),
		tab("3", "Help", m.viewMode == ViewHelp),
	}
	if m.showTap || len(m.tapEvents) > 0 {
		label := "Tap"
		if m.explainActive() {
			label = "Explain"
		}
		parts = append(parts, StatusDarkPillStyle.Render(label))
	}
	return lipgloss.NewStyle().Width(width).Render(truncate(strings.Join(parts, " "), width))
}

func (m *Model) renderFilterBar(width int) string {
	value := m.filter
	if value == "" {
		value = m.filterInput.Placeholder
	}
	if m.filterEditing {
		value = m.filterInput.View()
	}
	line := HelpKeyStyle.Render("/") + FooterDescStyle.Render(" filter  ") + NameStyle.Render(value)
	return lipgloss.NewStyle().Width(width).Background(colorPanelBg).Render(truncate(line, width))
}

func (m *Model) renderMessageBar(width int) string {
	style := DetailStyle
	switch m.messageType {
	case MessageSuccess:
		style = SuccessMsgStyle
	case MessageWarning:
		style = WarningMsgStyle
	case MessageError:
		style = ErrorMsgStyle
	}
	return lipgloss.NewStyle().Width(width).Render(style.Render(truncate(m.message, width)))
}

func (m *Model) renderBody(width, height int) string {
	if height < 6 {
		height = 6
	}
	if m.viewMode == ViewHelp {
		return m.renderHelpBody(width, height)
	}
	if m.viewMode == ViewGaps {
		return m.renderGapsBody(width, height)
	}
	if m.usingThreePane() {
		leftW, rightW := m.panelSplitWidths(width)
		detailH := height * 55 / 100
		if m.usesSQLPreview(width) {
			detailH = height * 46 / 100
		}
		detailH = clamp(detailH, 6, maxInt(6, height-5))
		tapH := height - detailH
		if tapH < 5 {
			tapH = 5
			detailH = height - tapH
		}

		list := m.renderListPanel(leftW, height)
		if m.explainActive() {
			explain := m.renderPanel("explain plan", m.tapVP.View(), rightW, height, m.focus == FocusTap || m.focus == FocusDetail)
			return lipgloss.JoinHorizontal(lipgloss.Top, list, explain)
		}
		detail := m.renderPanel(m.detailPanelTitle(), m.detailVP.View(), rightW, detailH, m.focus == FocusDetail)
		if m.usesSQLPreview(width) {
			detailW := rightW * 44 / 100
			if detailW < 24 {
				detailW = 24
			}
			if detailW > rightW-24 {
				detailW = rightW - 24
			}
			sqlW := rightW - detailW
			detail = m.renderPanel(m.detailPanelTitle(), m.detailVP.View(), detailW, detailH, m.focus == FocusDetail)
			sql := m.renderPanel(m.sqlPreviewPanelTitle(), m.renderMigrationSQLPreviewContent(maxInt(1, sqlW-2)), sqlW, detailH, false)
			topRight := lipgloss.JoinHorizontal(lipgloss.Top, detail, sql)
			tap := m.renderPanel("tap stream", m.tapVP.View(), rightW, tapH, m.focus == FocusTap)
			right := lipgloss.JoinVertical(lipgloss.Left, topRight, tap)
			return lipgloss.JoinHorizontal(lipgloss.Top, list, right)
		}
		tap := m.renderPanel("tap stream", m.tapVP.View(), rightW, tapH, m.focus == FocusTap)
		right := lipgloss.JoinVertical(lipgloss.Left, detail, tap)
		return lipgloss.JoinHorizontal(lipgloss.Top, list, right)
	}
	return m.renderListPanel(width, height)
}

func (m *Model) renderListPanel(width, height int) string {
	title := "migrations"
	content := m.renderMigrationListContent(maxInt(1, width-2), maxInt(1, height-3))
	if m.viewMode == ViewGaps {
		title = "gaps"
		content = m.renderGapListContent(maxInt(1, width-2), maxInt(1, height-3))
	}
	return m.renderPanel(title, content, width, height, m.focus == FocusList)
}

func (m *Model) detailPanelTitle() string {
	switch m.viewMode {
	case ViewGaps:
		return "gap details"
	default:
		return "migration details"
	}
}

func (m *Model) renderPanel(title, content string, width, height int, active bool) string {
	if width < 8 {
		width = 8
	}
	if height < 3 {
		height = 3
	}
	style := PanelStyle
	if active {
		style = PanelActiveStyle
	}
	innerW := maxInt(1, width-2)
	innerH := maxInt(1, height-2)
	header := PanelTitleStyle.Render(" " + title + " ")
	lines := make([]string, 1, innerH)
	lines[0] = truncate(header, innerW)
	lines = append(lines, fitBlock(content, innerW, innerH-1)...)
	return style.Width(innerW).Height(innerH).Render(strings.Join(lines, "\n"))
}

func (m *Model) renderFooter(width int) string {
	helpLine := m.contextualHelpLine()
	if helpLine == "" {
		helpLine = m.helpView.View(m.keys)
	}
	if helpLine == "" {
		helpLine = footerHelp("tab", "focus") + footerSep() +
			footerHelp("j/k", "move") + footerSep() +
			footerHelp("enter", "action") + footerSep() +
			footerHelp("/", "filter") + footerSep() +
			footerHelp("q", "quit")
	}
	return FooterStyle.Width(width).Render(truncate(helpLine, maxInt(1, width-4)))
}

func (m *Model) contextualHelpLine() string {
	if m.explainActive() {
		return footerHelp("j/k", "scroll explain") + footerSep() +
			footerHelp("esc", "back to tap") + footerSep() +
			footerHelp("tab", "focus") + footerSep() +
			footerHelp("q", "quit")
	}
	if m.viewMode == ViewHelp {
		return footerHelp("tab", "focus") + footerSep() +
			footerHelp("j/k", "navigate") + footerSep() +
			footerHelp("q", "back")
	}
	if m.viewMode == ViewGaps {
		return footerHelp("tab", "focus") + footerSep() +
			footerHelp("j/k", "navigate") + footerSep() +
			footerHelp("r", "refresh") + footerSep() +
			footerHelp("enter/f", "fill") + footerSep() +
			footerHelp("i", "ignore") + footerSep() +
			footerHelp("q", "back")
	}
	switch m.focus {
	case FocusDetail:
		return footerHelp("j/k", "scroll details") + footerSep() +
			footerHelp("tab", "tap/list") + footerSep() +
			footerHelp("enter", "apply/fill") + footerSep() +
			footerHelp("q", "quit")
	case FocusTap:
		return footerHelp("j/k", "select SQL") + footerSep() +
			footerHelp("x", "explain") + footerSep() +
			footerHelp("X", "analyze") + footerSep() +
			footerHelp("tab", "details/list") + footerSep() +
			footerHelp("q", "quit")
	default:
		return footerHelp("tab", "focus") + footerSep() +
			footerHelp("j/k", "navigate") + footerSep() +
			footerHelp("enter", "apply/rollback") + footerSep() +
			footerHelp("r", "refresh") + footerSep() +
			footerHelp("/", "filter") + footerSep() +
			footerHelp("q", "quit")
	}
}

func footerHelp(key, desc string) string {
	return FooterKeyStyle.Render(key) + FooterDescStyle.Render(" "+desc)
}

func footerSep() string {
	return FooterDescStyle.Render("  |  ")
}

func fitBlock(s string, width, height int) []string {
	if height <= 0 {
		return nil
	}
	raw := strings.Split(s, "\n")
	out := make([]string, 0, height)
	for _, line := range raw {
		if len(out) == height {
			break
		}
		out = append(out, padRight(truncate(line, width), width))
	}
	for len(out) < height {
		out = append(out, strings.Repeat(" ", width))
	}
	return out
}

func placeApartPlain(left, right string, width int) string {
	gap := width - lipgloss.Width(left) - lipgloss.Width(right)
	if gap < 1 {
		gap = 1
	}
	return truncate(left+strings.Repeat(" ", gap)+right, width)
}

func renderBarLines(lines []string, width int) string {
	rendered := make([]string, 0, len(lines))
	style := lipgloss.NewStyle().Width(width)
	for _, line := range lines {
		rendered = append(rendered, style.Render(padRight(truncate(line, width), width)))
	}
	return strings.Join(rendered, "\n")
}
