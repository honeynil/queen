package live

import (
	"time"

	"github.com/yaop-labs/queen/tap"
)

func (m *model) elapsed() time.Duration {
	if !m.finishedAt.IsZero() {
		return m.finishedAt.Sub(m.startAt)
	}
	return time.Since(m.startAt)
}

func (m *model) isLastChild(idx int) bool {
	if idx < 0 || idx >= len(m.rows) {
		return false
	}
	ev := m.rows[idx].ev
	for i := idx + 1; i < len(m.rows); i++ {
		next := m.rows[i].ev
		if sameMigration(ev, next) {
			return false
		}
	}
	return true
}

func sameMigration(a, b tap.Event) bool {
	return a.Version == b.Version && a.Name == b.Name && a.Direction == b.Direction
}

func (m *model) moveSelection(delta int) {
	if len(m.rows) == 0 {
		m.selected = -1
		m.scroll = 0
		return
	}
	if m.selected < 0 {
		m.selected = 0
	}
	m.selected = clamp(m.selected+delta, 0, len(m.rows)-1)
	m.ensureSelectionVisible()
}

func (m *model) ensureSelectionVisible() {
	if len(m.rows) == 0 {
		m.selected = -1
		m.scroll = 0
		return
	}
	m.selected = clamp(m.selected, 0, len(m.rows)-1)
	height := m.visibleStreamHeight()
	if height < 1 {
		height = 1
	}
	if m.selected < m.scroll {
		m.scroll = m.selected
	}
	if m.selected >= m.scroll+height {
		m.scroll = m.selected - height + 1
	}
	m.scroll = clamp(m.scroll, 0, m.maxScroll())
}

func (m *model) isFollowingTail() bool {
	return len(m.rows) == 0 || m.selected >= len(m.rows)-1 || m.scroll >= m.maxScroll()
}

func (m *model) maxScroll() int {
	body := m.visibleStreamHeight()
	if body < 1 {
		body = 1
	}
	if len(m.rows) <= body {
		return 0
	}
	return len(m.rows) - body
}
