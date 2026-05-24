package tui

import (
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
)

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
