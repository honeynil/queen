// Package tui provides a terminal UI for Queen migrations.
package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/honeynil/queen"
)

// renderHeader renders the top header bar with app title and status.
func (m *Model) renderHeader(width int) string {
	title := AppTitleStyle.Render("♛ Queen")
	subtitle := AppSubtitleStyle.Render(" Migration Manager")
	left := title + subtitle

	right := ""
	if m.loading {
		right = m.spinner.View() + " Loading..."
	}

	gap := width - lipgloss.Width(left) - lipgloss.Width(right) - 4
	if gap < 1 {
		gap = 1
	}

	line := left + strings.Repeat(" ", gap) + right
	return HeaderStyle.Width(width).Render(line)
}

// renderTabBar renders the view-switching tab bar.
func (m *Model) renderTabBar(width int) string {
	type tab struct {
		key  string
		name string
		mode ViewMode
	}

	tabs := []tab{
		{"1", "Migrations", ViewMigrations},
		{"2", "Gaps", ViewGaps},
		{"3", "Help", ViewHelp},
	}

	var parts []string
	for _, t := range tabs {
		label := fmt.Sprintf(" %s %s ", t.key, t.name)
		if m.viewMode == t.mode {
			parts = append(parts, ActiveTabStyle.Render(label))
		} else {
			parts = append(parts, InactiveTabStyle.Render(label))
		}
	}

	// Badge for gap count
	if len(m.gaps) > 0 {
		badge := TabBadgeStyle.Render(fmt.Sprintf(" %d", len(m.gaps)))
		parts[1] = parts[1] + badge
	}

	tabLine := lipgloss.JoinHorizontal(lipgloss.Top, parts...)
	return lipgloss.NewStyle().Width(width).Padding(0, 1).Render(tabLine)
}

// renderFooter renders the bottom keybinding bar.
func (m *Model) renderFooter(width int) string {
	type binding struct {
		key  string
		desc string
	}

	var bindings []binding
	switch m.viewMode {
	case ViewMigrations:
		bindings = []binding{
			{"↑↓", "navigate"},
			{"enter", "apply/rollback"},
			{"u", "up"},
			{"d", "down"},
			{"r", "refresh"},
			{"q", "quit"},
		}
	case ViewGaps:
		bindings = []binding{
			{"↑↓", "navigate"},
			{"f", "fill"},
			{"i", "ignore"},
			{"r", "refresh"},
			{"q", "quit"},
		}
	case ViewHelp:
		bindings = []binding{
			{"1", "migrations"},
			{"2", "gaps"},
			{"q", "quit"},
		}
	}

	parts := make([]string, 0, len(bindings))
	for _, b := range bindings {
		parts = append(parts, FooterKeyStyle.Render(b.key)+" "+FooterDescStyle.Render(b.desc))
	}

	line := strings.Join(parts, FooterDescStyle.Render("  ·  "))
	return FooterStyle.Width(width).Render(line)
}

// renderMessageBar renders the status message.
func (m *Model) renderMessageBar(width int) string {
	style := lipgloss.NewStyle().Width(width).Padding(0, 2)
	switch m.messageType {
	case MessageSuccess:
		return style.Render(SuccessMsgStyle.Render("✓ " + m.message))
	case MessageWarning:
		return style.Render(WarningMsgStyle.Render("⚠ " + m.message))
	case MessageError:
		return style.Render(ErrorMsgStyle.Render("✗ " + m.message))
	default:
		return style.Render("  " + m.message)
	}
}

// renderMigrationsView renders the full migrations view.
func (m *Model) renderMigrationsView() string {
	width := m.width
	if width == 0 {
		width = 80
	}

	var sections []string
	sections = append(sections, m.renderHeader(width))
	sections = append(sections, m.renderTabBar(width))
	sections = append(sections, separator(width))

	if m.loading {
		sections = append(sections, fmt.Sprintf("\n  %s Loading migrations...\n", m.spinner.View()))
		sections = append(sections, m.renderFooter(width))
		return strings.Join(sections, "\n")
	}

	if m.err != nil {
		sections = append(sections, ErrorStyle.Render(fmt.Sprintf("\n  Error: %v\n", m.err)))
		sections = append(sections, m.renderFooter(width))
		return strings.Join(sections, "\n")
	}

	// Stats + progress bar
	applied, pending := 0, 0
	for _, mig := range m.migrations {
		if mig.Status == queen.StatusApplied {
			applied++
		} else {
			pending++
		}
	}
	total := len(m.migrations)

	barWidth := 30
	if width > 100 {
		barWidth = 40
	}
	filledWidth := 0
	if total > 0 {
		filledWidth = (applied * barWidth) / total
	}
	bar := ProgressFilledStyle.Render(strings.Repeat("█", filledWidth)) +
		ProgressEmptyStyle.Render(strings.Repeat("░", barWidth-filledWidth))

	statsLine := fmt.Sprintf("  %s  %s/%d applied  %s pending",
		bar,
		AppliedStyle.Render(fmt.Sprintf("%d", applied)),
		total,
		PendingStyle.Render(fmt.Sprintf("%d", pending)),
	)
	sections = append(sections, statsLine)
	sections = append(sections, separator(width))

	if total == 0 {
		sections = append(sections, "")
		sections = append(sections, DetailStyle.Render("  No migrations found."))
		sections = append(sections, "")
	} else {
		sections = append(sections, m.renderMigrationsList(width))
	}

	if m.message != "" {
		sections = append(sections, m.renderMessageBar(width))
	}

	sections = append(sections, m.renderFooter(width))
	return strings.Join(sections, "\n")
}

// renderMigrationsList renders the scrollable migrations list.
func (m *Model) renderMigrationsList(width int) string {
	var s strings.Builder
	total := len(m.migrations)
	visible := m.contentHeight()

	start := m.scrollOffset
	end := start + visible
	if end > total {
		end = total
	}

	// Scroll indicator (top)
	if start > 0 {
		s.WriteString(ScrollIndicatorStyle.Render(fmt.Sprintf("  ↑ %d more above", start)))
		s.WriteString("\n")
	}

	for i := start; i < end; i++ {
		mig := m.migrations[i]

		cursor := "  "
		if i == m.cursor {
			cursor = iconCursor
		}

		// Status icon
		statusIcon := iconEmpty
		statusStyle := PendingStyle
		if mig.Status == queen.StatusApplied {
			statusIcon = iconSelected
			statusStyle = AppliedStyle
		}

		// Badges
		badges := ""
		if mig.Destructive {
			badges += " " + DestructiveBadgeStyle.Render("destructive")
		}
		if mig.HasRollback {
			badges += " " + RollbackBadgeStyle.Render("↩")
		}

		line := fmt.Sprintf("%s%s %s %s%s",
			cursor,
			statusStyle.Render(statusIcon),
			VersionStyle.Render(mig.Version),
			NameStyle.Render(mig.Name),
			badges,
		)

		if i == m.cursor {
			// Pad to full width for selected background
			lineWidth := lipgloss.Width(line)
			if lineWidth < width-2 {
				line += strings.Repeat(" ", width-2-lineWidth)
			}
			line = SelectedStyle.Render(line)
		}

		s.WriteString(line)
		s.WriteString("\n")

		// Detail line for selected applied item
		if i == m.cursor && mig.Status == queen.StatusApplied && mig.AppliedAt != nil {
			detail := fmt.Sprintf("       Applied: %s", mig.AppliedAt.Format("2006-01-02 15:04:05"))
			s.WriteString(DetailStyle.Render(detail))
			s.WriteString("\n")
		}
	}

	// Scroll indicator (bottom)
	if end < total {
		s.WriteString(ScrollIndicatorStyle.Render(fmt.Sprintf("  ↓ %d more below", total-end)))
		s.WriteString("\n")
	}

	return s.String()
}

// renderGapsView renders the full gaps detection view.
func (m *Model) renderGapsView() string {
	width := m.width
	if width == 0 {
		width = 80
	}

	var sections []string
	sections = append(sections, m.renderHeader(width))
	sections = append(sections, m.renderTabBar(width))
	sections = append(sections, separator(width))

	if m.loading {
		sections = append(sections, fmt.Sprintf("\n  %s Detecting gaps...\n", m.spinner.View()))
		sections = append(sections, m.renderFooter(width))
		return strings.Join(sections, "\n")
	}

	if m.err != nil {
		sections = append(sections, ErrorStyle.Render(fmt.Sprintf("\n  Error: %v\n", m.err)))
		sections = append(sections, m.renderFooter(width))
		return strings.Join(sections, "\n")
	}

	// Stats line
	warnings, errors := 0, 0
	for _, gap := range m.gaps {
		switch gap.Severity {
		case "warning":
			warnings++
		case "error":
			errors++
		}
	}

	if len(m.gaps) == 0 {
		statsLine := "  " + AppliedStyle.Render("✓ No gaps detected — migrations are clean!")
		sections = append(sections, statsLine)
	} else {
		statsLine := fmt.Sprintf("  Total: %d  ·  Warnings: %s  ·  Errors: %s",
			len(m.gaps),
			PendingStyle.Render(fmt.Sprintf("%d", warnings)),
			ErrorStyle.Render(fmt.Sprintf("%d", errors)),
		)
		sections = append(sections, statsLine)
	}
	sections = append(sections, separator(width))

	if len(m.gaps) > 0 {
		sections = append(sections, m.renderGapsList(width))
	} else {
		sections = append(sections, "")
	}

	if m.message != "" {
		sections = append(sections, m.renderMessageBar(width))
	}

	sections = append(sections, m.renderFooter(width))
	return strings.Join(sections, "\n")
}

// renderGapsList renders the scrollable gaps list.
func (m *Model) renderGapsList(width int) string {
	var s strings.Builder
	total := len(m.gaps)
	visible := m.contentHeight()

	start := m.scrollOffset
	end := start + visible
	if end > total {
		end = total
	}

	if start > 0 {
		s.WriteString(ScrollIndicatorStyle.Render(fmt.Sprintf("  ↑ %d more above", start)))
		s.WriteString("\n")
	}

	for i := start; i < end; i++ {
		gap := m.gaps[i]

		cursor := "  "
		if i == m.cursor {
			cursor = iconCursor
		}

		icon := "⚠"
		iconStyle := PendingStyle
		if gap.Severity == "error" {
			icon = "✗"
			iconStyle = ErrorStyle
		}

		typeLabel := string(gap.Type)
		switch gap.Type {
		case queen.GapTypeNumbering:
			typeLabel = "numbering"
		case queen.GapTypeApplication:
			typeLabel = "application"
		case queen.GapTypeUnregistered:
			typeLabel = "unregistered"
		}

		typeBadge := GapTypeBadgeStyle.Render("[" + typeLabel + "]")

		line := fmt.Sprintf("%s%s %s %s",
			cursor,
			iconStyle.Render(icon),
			typeBadge,
			VersionStyle.Render(gap.Version),
		)

		if gap.Name != "" {
			line += " " + NameStyle.Render(gap.Name)
		}

		if i == m.cursor {
			lineWidth := lipgloss.Width(line)
			if lineWidth < width-2 {
				line += strings.Repeat(" ", width-2-lineWidth)
			}
			line = SelectedStyle.Render(line)
		}

		s.WriteString(line)
		s.WriteString("\n")

		// Detail for selected gap
		if i == m.cursor {
			desc := "       " + gap.Description
			if len(gap.BlockedBy) > 0 {
				desc += "\n       Blocks: " + strings.Join(gap.BlockedBy, ", ")
			}
			s.WriteString(DetailStyle.Render(desc))
			s.WriteString("\n")
		}
	}

	if end < total {
		s.WriteString(ScrollIndicatorStyle.Render(fmt.Sprintf("  ↓ %d more below", total-end)))
		s.WriteString("\n")
	}

	return s.String()
}

// renderHelpView renders the help view.
func (m *Model) renderHelpView() string {
	width := m.width
	if width == 0 {
		width = 80
	}

	sections := make([]string, 0, 7)
	sections = append(sections, m.renderHeader(width))
	sections = append(sections, m.renderTabBar(width))
	sections = append(sections, separator(width))
	sections = append(sections, "")

	var help strings.Builder

	help.WriteString(HelpSectionStyle.Render("Navigation"))
	help.WriteString("\n")
	help.WriteString(helpLine("↑/k", "Move cursor up"))
	help.WriteString(helpLine("↓/j", "Move cursor down"))
	help.WriteString(helpLine("g", "Jump to top"))
	help.WriteString(helpLine("G", "Jump to bottom"))
	help.WriteString("\n")

	help.WriteString(HelpSectionStyle.Render("Views"))
	help.WriteString("\n")
	help.WriteString(helpLine("1", "Migrations view"))
	help.WriteString(helpLine("2", "Gaps detection view"))
	help.WriteString(helpLine("3 / ?", "Help view"))
	help.WriteString("\n")

	help.WriteString(HelpSectionStyle.Render("Migrations Actions"))
	help.WriteString("\n")
	help.WriteString(helpLine("enter", "Apply pending / rollback applied"))
	help.WriteString(helpLine("u", "Apply migration up to cursor"))
	help.WriteString(helpLine("d", "Rollback migration from cursor"))
	help.WriteString("\n")

	help.WriteString(HelpSectionStyle.Render("Gaps Actions"))
	help.WriteString("\n")
	help.WriteString(helpLine("enter / f", "Fill the selected gap"))
	help.WriteString(helpLine("i", "Ignore gap (add to .queenignore)"))
	help.WriteString("\n")

	help.WriteString(HelpSectionStyle.Render("General"))
	help.WriteString("\n")
	help.WriteString(helpLine("r", "Refresh data"))
	help.WriteString(helpLine("q / Ctrl+C", "Quit"))
	help.WriteString("\n\n")

	help.WriteString(HelpSectionStyle.Render("Tips"))
	help.WriteString("\n")
	help.WriteString(DetailStyle.Render("  ● Applied migrations are shown in green"))
	help.WriteString("\n")
	help.WriteString(DetailStyle.Render("  ○ Pending migrations are shown in yellow"))
	help.WriteString("\n")
	help.WriteString(DetailStyle.Render("  ↩ Indicates rollback script is available"))
	help.WriteString("\n")

	maxWidth := 64
	if width < maxWidth+6 {
		maxWidth = width - 6
	}
	if maxWidth < 30 {
		maxWidth = 30
	}

	helpBox := InfoBoxStyle.Width(maxWidth).Render(help.String())
	centered := lipgloss.PlaceHorizontal(width, lipgloss.Center, helpBox)
	sections = append(sections, centered)
	sections = append(sections, "")

	sections = append(sections, m.renderFooter(width))
	return strings.Join(sections, "\n")
}

func helpLine(key, desc string) string {
	return fmt.Sprintf("  %s  %s\n",
		HelpKeyStyle.Render(fmt.Sprintf("%-12s", key)),
		HelpDescStyle.Render(desc),
	)
}
