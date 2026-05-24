package tap

import (
	"strings"
	"sync"
	"time"
)

// AnalyzerConfig controls enrichment performed by AnalyzerSink.
type AnalyzerConfig struct {
	SlowThreshold   time.Duration
	NPlus1Threshold int
	NPlus1Window    time.Duration
	NPlus1Cooldown  time.Duration
	BindArgs        bool
}

// DefaultAnalyzerConfig is tuned for interactive migration runs.
func DefaultAnalyzerConfig() AnalyzerConfig {
	return AnalyzerConfig{
		SlowThreshold:   100 * time.Millisecond,
		NPlus1Threshold: 5,
		NPlus1Window:    time.Second,
		NPlus1Cooldown:  10 * time.Second,
		BindArgs:        true,
	}
}

// AnalyzerSink enriches exec events with operation, normalized SQL, bound SQL,
// slow-query markers, and N+1 detection before forwarding them.
type AnalyzerSink struct {
	next     Sink
	cfg      AnalyzerConfig
	detector *NPlus1Detector

	mu     sync.Mutex
	nextID int64
}

// NewAnalyzerSink adds SQL summaries before forwarding events to next.
func NewAnalyzerSink(next Sink, cfg AnalyzerConfig) *AnalyzerSink {
	if next == nil {
		next = NopSink{}
	}
	s := &AnalyzerSink{next: next, cfg: cfg}
	if cfg.NPlus1Threshold > 0 {
		s.detector = NewNPlus1Detector(cfg.NPlus1Threshold, cfg.NPlus1Window, cfg.NPlus1Cooldown)
	}
	return s
}

func (s *AnalyzerSink) Emit(e Event) {
	s.mu.Lock()
	s.nextID++
	e.ID = s.nextID
	s.mu.Unlock()

	if e.Kind == KindExec {
		e.Operation = OperationOf(e.SQL)
		e.SQLTemplate = NormalizeSQL(e.SQL)
		if s.cfg.BindArgs {
			e.BoundSQL = BindSQL(e.SQL, e.Args)
		}
		if s.cfg.SlowThreshold > 0 && e.Duration >= s.cfg.SlowThreshold {
			e.Slow = true
		}
		if s.detector != nil && shouldDetectNPlus1(e.Operation, e.SQLTemplate) {
			t := e.StartedAt
			if t.IsZero() {
				t = time.Now()
			}
			res := s.detector.Record(e.SQLTemplate, t)
			e.NPlus1 = res.Matched
			e.NPlus1Count = res.Count
			e.NPlus1Alert = res.Alert
		}
	}

	s.next.Emit(e)
}

// NPlus1Result is the outcome of recording a query template occurrence.
type NPlus1Result struct {
	Matched bool
	Count   int
	Alert   bool
}

// NPlus1Detector tracks repeated SELECT templates in a sliding window.
type NPlus1Detector struct {
	threshold int
	window    time.Duration
	cooldown  time.Duration

	mu     sync.Mutex
	events map[string][]time.Time
	alerts map[string]time.Time
}

func NewNPlus1Detector(threshold int, window, cooldown time.Duration) *NPlus1Detector {
	if window <= 0 {
		window = time.Second
	}
	if cooldown <= 0 {
		cooldown = 10 * time.Second
	}
	return &NPlus1Detector{
		threshold: threshold,
		window:    window,
		cooldown:  cooldown,
		events:    make(map[string][]time.Time),
		alerts:    make(map[string]time.Time),
	}
}

func (d *NPlus1Detector) Record(query string, t time.Time) NPlus1Result {
	if d == nil || d.threshold <= 0 || query == "" {
		return NPlus1Result{}
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	cutoff := t.Add(-d.window)
	seen := d.events[query]
	keep := seen[:0]
	for _, ts := range seen {
		if !ts.Before(cutoff) {
			keep = append(keep, ts)
		}
	}
	keep = append(keep, t)
	d.events[query] = keep

	res := NPlus1Result{Count: len(keep)}
	if res.Count >= d.threshold {
		res.Matched = true
		last := d.alerts[query]
		if last.IsZero() || !t.Before(last.Add(d.cooldown)) {
			res.Alert = true
			d.alerts[query] = t
		}
	}
	return res
}

func shouldDetectNPlus1(operation, template string) bool {
	if operation != "select" {
		return false
	}
	t := strings.ToLower(template)
	return strings.Contains(t, " from ")
}
