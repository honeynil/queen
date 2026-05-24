package queen

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	naturalsort "github.com/yaop-labs/queen/internal/sort"
)

type GapType string

const (
	GapTypeNumbering    GapType = "numbering"
	GapTypeApplication  GapType = "application"
	GapTypeUnregistered GapType = "unregistered"
)

type Gap struct {
	Type        GapType  `json:"type"`
	Version     string   `json:"version"`
	Name        string   `json:"name,omitempty"`
	Description string   `json:"description"`
	Severity    string   `json:"severity"`
	AppliedAt   *string  `json:"applied_at,omitempty"`
	BlockedBy   []string `json:"blocked_by,omitempty"`
}

// DetectGaps analyzes migrations and returns any detected gaps.
func (q *Queen) DetectGaps(ctx context.Context) ([]Gap, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.driver == nil {
		return nil, ErrNoDriver
	}

	if err := q.driver.Init(ctx); err != nil {
		return nil, err
	}

	if err := q.loadApplied(ctx); err != nil {
		return nil, err
	}

	gaps := make([]Gap, 0)

	appGaps, unregGaps := q.detectApplicationGaps()
	gaps = append(gaps, appGaps...)
	gaps = append(gaps, unregGaps...)

	numGaps := q.detectNumberingGaps()
	gaps = append(gaps, numGaps...)

	gaps = q.filterIgnoredGaps(gaps)

	return gaps, nil
}

func (q *Queen) detectApplicationGaps() ([]Gap, []Gap) {
	applicationGaps := make([]Gap, 0)
	unregisteredGaps := make([]Gap, 0)

	registered := make(map[string]*Migration)
	for _, m := range q.migrations {
		registered[m.Version] = m
	}

	appliedVersions := make([]string, 0, len(q.applied))
	for version := range q.applied {
		appliedVersions = append(appliedVersions, version)
	}
	sort.Slice(appliedVersions, func(i, j int) bool {
		return naturalsort.Compare(appliedVersions[i], appliedVersions[j]) < 0
	})

	for _, version := range appliedVersions {
		applied := q.applied[version]

		if _, exists := registered[version]; !exists {
			unregisteredGaps = append(unregisteredGaps, Gap{
				Type:        GapTypeUnregistered,
				Version:     version,
				Name:        applied.Name,
				Description: fmt.Sprintf("Migration %s (%s) is applied in database but not registered in code", version, applied.Name),
				Severity:    "error",
				AppliedAt:   new(applied.AppliedAt.Format("2006-01-02 15:04:05")),
			})
		}
	}

	registeredVersions := make([]string, 0, len(q.migrations))
	for _, m := range q.migrations {
		registeredVersions = append(registeredVersions, m.Version)
	}
	sort.Slice(registeredVersions, func(i, j int) bool {
		return naturalsort.Compare(registeredVersions[i], registeredVersions[j]) < 0
	})

	lastAppliedIndex := -1
	for i, version := range registeredVersions {
		if _, applied := q.applied[version]; applied {
			lastAppliedIndex = i
		}
	}

	if lastAppliedIndex >= 0 {
		for i := 0; i < lastAppliedIndex; i++ {
			version := registeredVersions[i]
			migration := registered[version]

			if _, applied := q.applied[version]; !applied {
				blockedBy := make([]string, 0)
				for j := i + 1; j <= lastAppliedIndex; j++ {
					if _, ok := q.applied[registeredVersions[j]]; ok {
						blockedBy = append(blockedBy, registeredVersions[j])
						if len(blockedBy) >= 3 {
							blockedBy = append(blockedBy, "...")
							break
						}
					}
				}

				applicationGaps = append(applicationGaps, Gap{
					Type:        GapTypeApplication,
					Version:     version,
					Name:        migration.Name,
					Description: fmt.Sprintf("Migration %s (%s) was skipped but later migrations are applied", version, migration.Name),
					Severity:    "warning",
					BlockedBy:   blockedBy,
				})
			}
		}
	}

	return applicationGaps, unregisteredGaps
}

func (q *Queen) detectNumberingGaps() []Gap {
	gaps := make([]Gap, 0)

	numericVersions := make([]int, 0)
	versionMap := make(map[int]string)

	for _, m := range q.migrations {
		num, err := numericVersion(m.Version)
		if err != nil {
			continue
		}
		numericVersions = append(numericVersions, num)
		versionMap[num] = m.Version
	}

	if len(numericVersions) < 2 {
		return gaps
	}

	sort.Ints(numericVersions)

	for i := 0; i < len(numericVersions)-1; i++ {
		current := numericVersions[i]
		next := numericVersions[i+1]

		if next-current > 1 {
			for missing := current + 1; missing < next; missing++ {
				sampleVersion := versionMap[current]
				missingVersion := fmt.Sprintf("%0*d", len(sampleVersion), missing)

				gaps = append(gaps, Gap{
					Type:        GapTypeNumbering,
					Version:     missingVersion,
					Description: fmt.Sprintf("Sequential gap: version %s is missing between %s and %s", missingVersion, versionMap[current], versionMap[next]),
					Severity:    "warning",
				})
			}
		}
	}

	return gaps
}

func numericVersion(version string) (int, error) {
	trimmed := strings.TrimLeft(version, "0")
	if trimmed == "" {
		trimmed = "0"
	}
	return strconv.Atoi(trimmed)
}

func (q *Queen) filterIgnoredGaps(gaps []Gap) []Gap {
	qi, err := LoadQueenIgnore()
	if err != nil {
		return gaps
	}

	filtered := make([]Gap, 0, len(gaps))
	for _, gap := range gaps {
		if !qi.IsIgnored(gap.Version) {
			filtered = append(filtered, gap)
		}
	}

	return filtered
}
