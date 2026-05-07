package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type initStep int

const (
	stepDriver initStep = iota
	stepMigrationsDir
	stepConfig
	stepConfirm
)

type InitResult struct {
	Driver        string
	MigrationsDir string
	WithConfig    bool
	Confirmed     bool
}

type driverOption struct {
	name string
	desc string
}

type InitModel struct {
	step           initStep
	cursor         int
	drivers        []driverOption
	selectedDriver int
	dirInput       textinput.Model
	withConfig     bool
	width          int
	height         int
	quitting       bool
	result         *InitResult
}

func NewInitModel() *InitModel {
	ti := textinput.New()
	ti.Placeholder = "migrations"
	ti.SetValue("migrations")
	ti.CharLimit = 64
	ti.Width = 40

	return &InitModel{
		step:   stepDriver,
		cursor: 0,
		drivers: []driverOption{
			{"postgres", "PostgreSQL (recommended)"},
			{"mysql", "MySQL / MariaDB"},
			{"sqlite", "SQLite (file-based)"},
			{"clickhouse", "ClickHouse (analytics)"},
		},
		selectedDriver: 0,
		dirInput:       ti,
		withConfig:     true,
		result:         nil,
	}
}

func (m *InitModel) Result() *InitResult {
	return m.result
}

func (m *InitModel) Init() tea.Cmd {
	return textinput.Blink
}

func (m *InitModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c":
			m.quitting = true
			return m, tea.Quit

		case "esc":
			if m.step > stepDriver {
				m.step--
				if m.step == stepMigrationsDir {
					m.dirInput.Focus()
				}
				return m, nil
			}
			m.quitting = true
			return m, tea.Quit
		}

		switch m.step {
		case stepDriver:
			return m.updateDriverStep(msg)
		case stepMigrationsDir:
			return m.updateDirStep(msg)
		case stepConfig:
			return m.updateConfigStep(msg)
		case stepConfirm:
			return m.updateConfirmStep(msg)
		}
	}

	if m.step == stepMigrationsDir {
		var cmd tea.Cmd
		m.dirInput, cmd = m.dirInput.Update(msg)
		return m, cmd
	}

	return m, nil
}

func (m *InitModel) updateDriverStep(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "up", "k":
		if m.cursor > 0 {
			m.cursor--
		}
	case keyDown, "j":
		if m.cursor < len(m.drivers)-1 {
			m.cursor++
		}
	case keyEnter:
		m.selectedDriver = m.cursor
		m.step = stepMigrationsDir
		m.dirInput.Focus()
		return m, textinput.Blink
	}
	return m, nil
}

func (m *InitModel) updateDirStep(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case keyEnter:
		if m.dirInput.Value() == "" {
			m.dirInput.SetValue("migrations")
		}
		m.step = stepConfig
		m.cursor = 0
		m.dirInput.Blur()
		return m, nil
	}

	var cmd tea.Cmd
	m.dirInput, cmd = m.dirInput.Update(msg)
	return m, cmd
}

func (m *InitModel) updateConfigStep(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "up", "k", keyDown, "j":
		m.withConfig = !m.withConfig
	case keyEnter:
		m.step = stepConfirm
		return m, nil
	}
	return m, nil
}

func (m *InitModel) updateConfirmStep(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case keyEnter, "y":
		m.result = &InitResult{
			Driver:        m.drivers[m.selectedDriver].name,
			MigrationsDir: m.dirInput.Value(),
			WithConfig:    m.withConfig,
			Confirmed:     true,
		}
		m.quitting = true
		return m, tea.Quit
	case "n":
		m.quitting = true
		return m, tea.Quit
	}
	return m, nil
}

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

	sections = append(sections, "")

	var footer string
	switch m.step {
	case stepDriver:
		footer = FooterKeyStyle.Render("↑↓") + FooterDescStyle.Render(" select  ") +
			FooterKeyStyle.Render("enter") + FooterDescStyle.Render(" confirm  ") +
			FooterKeyStyle.Render("esc") + FooterDescStyle.Render(" quit")
	case stepMigrationsDir:
		footer = FooterKeyStyle.Render("enter") + FooterDescStyle.Render(" confirm  ") +
			FooterKeyStyle.Render("esc") + FooterDescStyle.Render(" back")
	case stepConfig:
		footer = FooterKeyStyle.Render("↑↓") + FooterDescStyle.Render(" toggle  ") +
			FooterKeyStyle.Render("enter") + FooterDescStyle.Render(" confirm  ") +
			FooterKeyStyle.Render("esc") + FooterDescStyle.Render(" back")
	case stepConfirm:
		footer = FooterKeyStyle.Render("enter/y") + FooterDescStyle.Render(" create  ") +
			FooterKeyStyle.Render("n") + FooterDescStyle.Render(" cancel  ") +
			FooterKeyStyle.Render("esc") + FooterDescStyle.Render(" back")
	}
	sections = append(sections, FooterStyle.Width(width).Render(footer))

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
			nameStyle = lipgloss.NewStyle().Foreground(accentColor).Bold(true)
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
		yesStyle = lipgloss.NewStyle().Foreground(accentColor).Bold(true)
	} else {
		noIcon = iconSelected
		noStyle = lipgloss.NewStyle().Foreground(accentColor).Bold(true)
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
	s.WriteString(WarningMsgStyle.Render("  Press Enter to create, n to cancel"))

	return s.String()
}
