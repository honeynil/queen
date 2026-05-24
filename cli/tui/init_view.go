package tui

import (
	"fmt"
	"os"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

func (m *InitModel) View() string {
	if m.quitting {
		return ""
	}

	width := m.width
	if width == 0 {
		width = 80
	}

	var sections []string

	title := AppTitleStyle.Render("♛ Queen")
	subtitle := AppSubtitleStyle.Render(" Interactive Setup")
	headerLine := title + subtitle
	sections = append(sections, HeaderStyle.Width(width).Render(headerLine))

	sections = append(sections, m.renderProgress(width))
	sections = append(sections, separator(width))
	sections = append(sections, "")

	switch m.step {
	case stepDriver:
		sections = append(sections, m.renderDriverStep())
	case stepMigrationsDir:
		sections = append(sections, m.renderDirStep())
	case stepConfig:
		sections = append(sections, m.renderConfigStep())
	case stepConfirm:
		sections = append(sections, m.renderConfirmStep())
	}

	sections = append(sections, "", FooterStyle.Width(width).Render(m.renderStepFooter()))

	return strings.Join(sections, "\n")
}

func (m *InitModel) renderProgress(width int) string {
	steps := []string{"Driver", "Directory", "Config", "Confirm"}
	var parts []string
	for i, name := range steps {
		label := fmt.Sprintf(" %d. %s ", i+1, name)
		if initStep(i) == m.step {
			parts = append(parts, ActiveTabStyle.Render(label))
		} else if initStep(i) < m.step {
			parts = append(parts, AppliedStyle.Render(label))
		} else {
			parts = append(parts, InactiveTabStyle.Render(label))
		}
	}
	line := lipgloss.JoinHorizontal(lipgloss.Top, parts...)
	return lipgloss.NewStyle().Width(width).Padding(0, 1).Render(line)
}

func (m *InitModel) renderStepFooter() string {
	switch m.step {
	case stepDriver:
		return FooterKeyStyle.Render("↑↓") + FooterDescStyle.Render(" select  ") +
			FooterKeyStyle.Render("enter") + FooterDescStyle.Render(" confirm  ") +
			FooterKeyStyle.Render("esc") + FooterDescStyle.Render(" quit")
	case stepMigrationsDir:
		return FooterKeyStyle.Render("enter") + FooterDescStyle.Render(" confirm  ") +
			FooterKeyStyle.Render("esc") + FooterDescStyle.Render(" back")
	case stepConfig:
		return FooterKeyStyle.Render("↑↓") + FooterDescStyle.Render(" toggle  ") +
			FooterKeyStyle.Render("enter") + FooterDescStyle.Render(" confirm  ") +
			FooterKeyStyle.Render("esc") + FooterDescStyle.Render(" back")
	case stepConfirm:
		return FooterKeyStyle.Render("enter/y") + FooterDescStyle.Render(" create  ") +
			FooterKeyStyle.Render("n") + FooterDescStyle.Render(" cancel  ") +
			FooterKeyStyle.Render("esc") + FooterDescStyle.Render(" back")
	default:
		return ""
	}
}

func (m *InitModel) renderDriverStep() string {
	var s strings.Builder
	s.WriteString(TitleStyle.Render("  Select your database driver"))
	s.WriteString("\n\n")

	for i, d := range m.drivers {
		cursor := "  "
		if i == m.cursor {
			cursor = iconCursor
		}

		icon := iconEmpty
		nameStyle := NameStyle
		if i == m.cursor {
			icon = iconSelected
			nameStyle = lipgloss.NewStyle().Foreground(colorAccent).Bold(true)
		}

		line := fmt.Sprintf("%s%s %s  %s",
			cursor,
			AppliedStyle.Render(icon),
			nameStyle.Render(d.name),
			DetailStyle.Render(d.desc),
		)
		s.WriteString(line)
		s.WriteString("\n")
	}

	return s.String()
}

func (m *InitModel) renderDirStep() string {
	var s strings.Builder
	s.WriteString(TitleStyle.Render("  Migrations directory name"))
	s.WriteString("\n\n")
	s.WriteString("  " + m.dirInput.View())
	s.WriteString("\n\n")
	s.WriteString(DetailStyle.Render("  This directory will contain your migration Go files"))
	return s.String()
}

func (m *InitModel) renderConfigStep() string {
	var s strings.Builder
	s.WriteString(TitleStyle.Render("  Create .queen.yaml configuration file?"))
	s.WriteString("\n\n")

	yesIcon, noIcon := iconEmpty, iconEmpty
	yesStyle, noStyle := NameStyle, NameStyle
	if m.withConfig {
		yesIcon = iconSelected
		yesStyle = lipgloss.NewStyle().Foreground(colorAccent).Bold(true)
	} else {
		noIcon = iconSelected
		noStyle = lipgloss.NewStyle().Foreground(colorAccent).Bold(true)
	}

	fmt.Fprintf(&s, "  %s %s  %s\n",
		AppliedStyle.Render(yesIcon),
		yesStyle.Render("Yes"),
		DetailStyle.Render("Create config with environment settings (dev/staging/production)"))
	fmt.Fprintf(&s, "  %s %s  %s\n",
		AppliedStyle.Render(noIcon),
		noStyle.Render("No"),
		DetailStyle.Render("Skip config file, configure programmatically"))

	return s.String()
}

func (m *InitModel) renderConfirmStep() string {
	var s strings.Builder
	s.WriteString(TitleStyle.Render("  Review your setup"))
	s.WriteString("\n\n")

	driver := m.drivers[m.selectedDriver]
	fmt.Fprintf(&s, "  %s  %s\n", AppliedStyle.Render("Driver:"), VersionStyle.Render(driver.name))
	fmt.Fprintf(&s, "  %s  %s\n", AppliedStyle.Render("Directory:"), VersionStyle.Render(m.dirInput.Value()+"/"))

	configStr := "No"
	if m.withConfig {
		configStr = "Yes (.queen.yaml)"
	}
	fmt.Fprintf(&s, "  %s  %s\n", AppliedStyle.Render("Config:"), VersionStyle.Render(configStr))

	s.WriteString("\n")
	s.WriteString("  Files to create:\n")
	s.WriteString(DetailStyle.Render(fmt.Sprintf("    %s/migrations.go\n", m.dirInput.Value())))
	s.WriteString(DetailStyle.Render(fmt.Sprintf("    %s/001_initial_schema.go\n", m.dirInput.Value())))
	s.WriteString(DetailStyle.Render("    cmd/migrate/main.go\n"))
	if m.withConfig {
		s.WriteString(DetailStyle.Render("    .queen.yaml\n"))
	}

	s.WriteString("\n")
	if warning := existingDirWarning(m.dirInput.Value()); warning != "" {
		s.WriteString(WarningMsgStyle.Render("  ⚠ " + warning))
		s.WriteString("\n")
	}
	s.WriteString(WarningMsgStyle.Render("  Press Enter to create, n to cancel"))

	return s.String()
}

func existingDirWarning(dir string) string {
	if dir == "" {
		return ""
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return ""
	}
	if len(entries) == 0 {
		return ""
	}
	return fmt.Sprintf("Directory %q already exists and contains %d file(s); existing files may be overwritten", dir, len(entries))
}
