package tui

import (
	"fmt"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/yaop-labs/queen"
)

func (m *Model) handleAction() (tea.Model, tea.Cmd) {
	switch m.viewMode {
	case ViewMigrations:
		migration, _, ok := m.selectedMigration()
		if !ok {
			return m, nil
		}
		if migration.Status == queen.StatusPending {
			return m.applyMigration()
		}
		return m.rollbackMigration()

	case ViewGaps:
		return m.fillGap()
	}

	return m, nil
}

func (m *Model) applyMigration() (tea.Model, tea.Cmd) {
	migration, selectedIndex, ok := m.selectedMigration()
	if !ok {
		return m, nil
	}

	if migration.Status != queen.StatusPending {
		m.message = "Migration already applied"
		m.messageType = MessageWarning
		return m, nil
	}

	steps := 0
	for i := 0; i <= selectedIndex; i++ {
		if m.migrations[i].Status == queen.StatusPending {
			steps++
		}
	}
	version := migration.Version

	return m.startTappedOperation(fmt.Sprintf("apply up to %s", version), func() operationCompleteMsg {
		if err := m.queen.UpSteps(m.ctx, steps); err != nil {
			return operationCompleteMsg{
				message:     fmt.Sprintf("Failed to apply migration: %v", err),
				messageType: MessageError,
			}
		}

		return operationCompleteMsg{
			message:     fmt.Sprintf("Applied migration %s", version),
			messageType: MessageSuccess,
		}
	})
}

func (m *Model) rollbackMigration() (tea.Model, tea.Cmd) {
	migration, selectedIndex, ok := m.selectedMigration()
	if !ok {
		return m, nil
	}

	if migration.Status != queen.StatusApplied {
		m.message = "Migration not applied yet"
		m.messageType = MessageWarning
		return m, nil
	}

	for i := selectedIndex; i < len(m.migrations); i++ {
		candidate := m.migrations[i]
		if candidate.Status == queen.StatusApplied && !candidate.HasRollback {
			m.message = fmt.Sprintf("Cannot rollback: migration %s has no down migration", candidate.Version)
			m.messageType = MessageWarning
			return m, nil
		}
	}

	steps := 0
	for i := selectedIndex; i < len(m.migrations); i++ {
		if m.migrations[i].Status == queen.StatusApplied {
			steps++
		}
	}
	version := migration.Version

	return m.startTappedOperation(fmt.Sprintf("rollback from %s", version), func() operationCompleteMsg {
		if err := m.queen.Down(m.ctx, steps); err != nil {
			return operationCompleteMsg{
				message:     fmt.Sprintf("Failed to rollback migration: %v", err),
				messageType: MessageError,
			}
		}

		return operationCompleteMsg{
			message:     fmt.Sprintf("Rolled back %d migration(s)", steps),
			messageType: MessageSuccess,
		}
	})
}

func (m *Model) fillGap() (tea.Model, tea.Cmd) {
	gap, ok := m.selectedGap()
	if !ok {
		return m, nil
	}

	return m.startTappedOperation(fmt.Sprintf("fill gap %s", gap.Version), func() operationCompleteMsg {
		statuses, err := m.queen.Status(m.ctx)
		if err != nil {
			return operationCompleteMsg{
				message:     fmt.Sprintf("Failed to get status: %v", err),
				messageType: MessageError,
			}
		}

		targetIndex := -1
		for i, s := range statuses {
			if s.Version == gap.Version {
				targetIndex = i
				break
			}
		}

		if targetIndex == -1 {
			return operationCompleteMsg{
				message:     fmt.Sprintf("Migration %s not found", gap.Version),
				messageType: MessageError,
			}
		}

		stepsToApply := 0
		for i := 0; i <= targetIndex; i++ {
			if statuses[i].Status == queen.StatusPending {
				stepsToApply++
			}
		}

		if err := m.queen.UpSteps(m.ctx, stepsToApply); err != nil {
			return operationCompleteMsg{
				message:     fmt.Sprintf("Failed to fill gap: %v", err),
				messageType: MessageError,
			}
		}

		return operationCompleteMsg{
			message:     fmt.Sprintf("Filled gap: %s", gap.Version),
			messageType: MessageSuccess,
		}
	})
}

func (m *Model) ignoreGap() (tea.Model, tea.Cmd) {
	gap, ok := m.selectedGap()
	if !ok {
		return m, nil
	}

	m.loading = true
	m.spinnerActive = true

	return m, tea.Batch(func() tea.Msg {
		qi, err := queen.LoadQueenIgnore()
		if err != nil {
			qi = &queen.QueenIgnore{}
		}

		if err := qi.AddIgnore(gap.Version, gap.Description, "tui"); err != nil {
			return operationCompleteMsg{
				message:     fmt.Sprintf("Failed to ignore gap: %v", err),
				messageType: MessageError,
			}
		}

		return operationCompleteMsg{
			message:     fmt.Sprintf("Ignored gap: %s", gap.Version),
			messageType: MessageSuccess,
		}
	}, m.spinner.Tick)
}
