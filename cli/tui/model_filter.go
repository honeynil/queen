package tui

import (
	"fmt"
	"strings"

	"github.com/yaop-labs/queen"
)

func (m *Model) filterVisible() bool {
	return m.viewMode != ViewHelp && (m.filterEditing || m.filter != "")
}

func (m *Model) clearFilter() {
	m.filter = ""
	m.filterEditing = false
	m.resetCursor()
}

func (m *Model) resetCursor() {
	m.cursor = 0
	m.refreshRows()
}

func (m *Model) focusFirstPendingMigration() {
	if m.viewMode != ViewMigrations {
		return
	}
	indexes := m.filteredMigrationIndexes()
	for position, index := range indexes {
		if m.migrations[index].Status == queen.StatusPending {
			m.cursor = position
			m.refreshRows()
			return
		}
	}
}

func (m *Model) ensureCursorInRange() {
	count := m.currentItemCount()
	if count == 0 {
		m.resetCursor()
		return
	}
	if m.cursor >= count {
		m.cursor = count - 1
	}
	if m.cursor < 0 {
		m.cursor = 0
	}
	m.refreshRows()
}

func (m *Model) currentItemCount() int {
	switch m.viewMode {
	case ViewMigrations:
		return len(m.filteredMigrationIndexes())
	case ViewGaps:
		return len(m.filteredGapIndexes())
	default:
		return 0
	}
}

func (m *Model) filteredMigrationIndexes() []int {
	indexes := make([]int, 0, len(m.migrations))
	for i, mig := range m.migrations {
		if m.migrationMatchesFilter(mig) {
			indexes = append(indexes, i)
		}
	}
	return indexes
}

func (m *Model) filteredGapIndexes() []int {
	indexes := make([]int, 0, len(m.gaps))
	for i, gap := range m.gaps {
		if m.gapMatchesFilter(gap) {
			indexes = append(indexes, i)
		}
	}
	return indexes
}

func (m *Model) selectedMigrationIndex() (int, bool) {
	indexes := m.filteredMigrationIndexes()
	if len(indexes) == 0 {
		return 0, false
	}
	cursor := clamp(m.cursor, 0, len(indexes)-1)
	return indexes[cursor], true
}

func (m *Model) selectedMigration() (queen.MigrationStatus, int, bool) {
	index, ok := m.selectedMigrationIndex()
	if !ok {
		return queen.MigrationStatus{}, 0, false
	}
	return m.migrations[index], index, true
}

func (m *Model) selectedGapIndex() (int, bool) {
	indexes := m.filteredGapIndexes()
	if len(indexes) == 0 {
		return 0, false
	}
	cursor := clamp(m.cursor, 0, len(indexes)-1)
	return indexes[cursor], true
}

func (m *Model) selectedGap() (queen.Gap, bool) {
	index, ok := m.selectedGapIndex()
	if !ok {
		return queen.Gap{}, false
	}
	return m.gaps[index], true
}

func (m *Model) migrationMatchesFilter(mig queen.MigrationStatus) bool {
	query := normalizedFilter(m.filter)
	if query == "" {
		return true
	}

	flags := make([]string, 0, 2)
	if mig.Destructive {
		flags = append(flags, "destructive")
	}
	if mig.HasRollback {
		flags = append(flags, "rollback down")
	}

	haystack := strings.ToLower(fmt.Sprintf("%s %s %s %s",
		mig.Version,
		mig.Name,
		mig.Status.String(),
		strings.Join(flags, " "),
	))
	return strings.Contains(haystack, query)
}

func (m *Model) gapMatchesFilter(gap queen.Gap) bool {
	query := normalizedFilter(m.filter)
	if query == "" {
		return true
	}

	haystack := strings.ToLower(fmt.Sprintf("%s %s %s %s %s %s",
		gap.Version,
		gap.Name,
		gap.Description,
		gap.Type,
		gap.Severity,
		strings.Join(gap.BlockedBy, " "),
	))
	return strings.Contains(haystack, query)
}

func normalizedFilter(filter string) string {
	return strings.ToLower(strings.TrimSpace(filter))
}
