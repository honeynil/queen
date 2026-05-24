package queen

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	naturalsort "github.com/yaop-labs/queen/internal/sort"
)

type IgnoredGap struct {
	Version   string
	Reason    string
	IgnoredAt time.Time
	IgnoredBy string
}

type QueenIgnore struct {
	filePath string
	ignored  map[string]*IgnoredGap
}

func LoadQueenIgnore() (*QueenIgnore, error) {
	return LoadQueenIgnoreFrom(".queenignore")
}

func LoadQueenIgnoreFrom(path string) (*QueenIgnore, error) {
	qi := &QueenIgnore{
		filePath: path,
		ignored:  make(map[string]*IgnoredGap),
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		return qi, nil
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open .queenignore: %w", err)
	}
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "#", 2)
		version := strings.TrimSpace(parts[0])
		reason := ""
		if len(parts) > 1 {
			reason = strings.TrimSpace(parts[1])
		}

		if version == "" {
			continue
		}

		qi.ignored[version] = &IgnoredGap{
			Version: version,
			Reason:  reason,
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read .queenignore: %w", err)
	}

	return qi, nil
}

func (qi *QueenIgnore) IsIgnored(version string) bool {
	if qi.ignored == nil {
		return false
	}
	_, exists := qi.ignored[version]
	return exists
}

func (qi *QueenIgnore) GetReason(version string) string {
	if qi.ignored == nil {
		return ""
	}
	if gap, exists := qi.ignored[version]; exists {
		return gap.Reason
	}
	return ""
}

func (qi *QueenIgnore) AddIgnore(version, reason, ignoredBy string) error {
	qi.ensureInitialized()

	qi.ignored[version] = &IgnoredGap{
		Version:   version,
		Reason:    reason,
		IgnoredAt: time.Now(),
		IgnoredBy: ignoredBy,
	}
	return qi.Save()
}

func (qi *QueenIgnore) RemoveIgnore(version string) error {
	qi.ensureInitialized()

	delete(qi.ignored, version)
	return qi.Save()
}

func (qi *QueenIgnore) Save() error {
	qi.ensureInitialized()

	file, err := os.Create(qi.filePath)
	if err != nil {
		return fmt.Errorf("failed to create .queenignore: %w", err)
	}
	defer func() { _ = file.Close() }()

	header := `# .queenignore - Ignored migration gaps
# Format: version # reason
#
# This file tracks migration gaps that should be ignored.
# Only application gaps (skipped migrations) should be listed here.

`
	if _, err := file.WriteString(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	for _, gap := range qi.ListIgnored() {
		var line string
		if gap.Reason != "" {
			line = fmt.Sprintf("%s # %s\n", gap.Version, gap.Reason)
		} else {
			line = fmt.Sprintf("%s\n", gap.Version)
		}
		if _, err := file.WriteString(line); err != nil {
			return fmt.Errorf("failed to write ignored gap: %w", err)
		}
	}

	return nil
}

func (qi *QueenIgnore) ListIgnored() []*IgnoredGap {
	qi.ensureInitialized()

	result := make([]*IgnoredGap, 0, len(qi.ignored))
	for _, gap := range qi.ignored {
		result = append(result, gap)
	}
	sortIgnoredGaps(result)
	return result
}

func (qi *QueenIgnore) ensureInitialized() {
	if qi.filePath == "" {
		qi.filePath = ".queenignore"
	}
	if qi.ignored == nil {
		qi.ignored = make(map[string]*IgnoredGap)
	}
}

func sortIgnoredGaps(gaps []*IgnoredGap) {
	sort.Slice(gaps, func(i, j int) bool {
		return naturalsort.Compare(gaps[i].Version, gaps[j].Version) < 0
	})
}
