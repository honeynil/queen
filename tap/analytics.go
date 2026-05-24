package tap

import (
	"sort"
	"time"
)

// Summary is an aggregate view of recorded tap events.
type Summary struct {
	Total         int
	Execs         int
	Errors        int
	Slow          int
	NPlus1        int
	TotalDuration time.Duration
	Queries       []QueryStat
}

// QueryStat aggregates events by normalized SQL template.
type QueryStat struct {
	SQL           string
	SQLTemplate   string
	Operation     string
	Count         int
	Errors        int
	Slow          int
	NPlus1        int
	TotalDuration time.Duration
	AvgDuration   time.Duration
	MaxDuration   time.Duration
}

func Summarize(events []Event) Summary {
	var s Summary
	byTemplate := make(map[string]*QueryStat)
	for _, e := range events {
		s.Total++
		if e.Error != "" {
			s.Errors++
		}
		if e.Slow {
			s.Slow++
		}
		if e.NPlus1 {
			s.NPlus1++
		}
		if e.Kind != KindExec {
			continue
		}
		s.Execs++
		s.TotalDuration += e.Duration

		template := e.SQLTemplate
		if template == "" {
			template = NormalizeSQL(e.SQL)
		}
		stat := byTemplate[template]
		if stat == nil {
			stat = &QueryStat{
				SQL:         e.SQL,
				SQLTemplate: template,
				Operation:   e.Operation,
			}
			if stat.Operation == "" {
				stat.Operation = OperationOf(e.SQL)
			}
			byTemplate[template] = stat
		}
		stat.Count++
		stat.TotalDuration += e.Duration
		stat.AvgDuration = stat.TotalDuration / time.Duration(stat.Count)
		if e.Duration > stat.MaxDuration {
			stat.MaxDuration = e.Duration
		}
		if e.Error != "" {
			stat.Errors++
		}
		if e.Slow {
			stat.Slow++
		}
		if e.NPlus1 {
			stat.NPlus1++
		}
	}

	s.Queries = make([]QueryStat, 0, len(byTemplate))
	for _, stat := range byTemplate {
		s.Queries = append(s.Queries, *stat)
	}
	sort.SliceStable(s.Queries, func(i, j int) bool {
		if s.Queries[i].TotalDuration == s.Queries[j].TotalDuration {
			return s.Queries[i].Count > s.Queries[j].Count
		}
		return s.Queries[i].TotalDuration > s.Queries[j].TotalDuration
	})
	return s
}

// TopQueries returns up to limit query stats sorted by total, count, avg, or max.
func TopQueries(events []Event, sortBy string, limit int) []QueryStat {
	stats := Summarize(events).Queries
	sort.SliceStable(stats, func(i, j int) bool {
		switch sortBy {
		case "count":
			return stats[i].Count > stats[j].Count
		case "avg":
			return stats[i].AvgDuration > stats[j].AvgDuration
		case "max":
			return stats[i].MaxDuration > stats[j].MaxDuration
		default:
			return stats[i].TotalDuration > stats[j].TotalDuration
		}
	})
	if limit > 0 && limit < len(stats) {
		return stats[:limit]
	}
	return stats
}
