package live

func (m *model) wideLayout() bool {
	return m.safeWidth() >= 112 && m.height >= 18
}

func (m *model) visibleStreamHeight() int {
	height := m.contentHeight()
	if d := m.detailHeight(); d > 0 {
		height -= d + 1
	}
	if height < 4 {
		return 4
	}
	return height
}

func (m *model) contentHeight() int {
	height := m.height - 6
	if height < 4 {
		return 4
	}
	return height
}

func (m *model) detailHeight() int {
	if m.wideLayout() || m.height < 24 {
		return 0
	}
	if m.height < 30 {
		return 5
	}
	return 7
}

func (m *model) safeWidth() int {
	if m.width <= 0 {
		return 80
	}
	return m.width
}
