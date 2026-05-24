package tap

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRecorderSink(t *testing.T) {
	r := NewRecorderSink()
	r.Emit(Event{Kind: KindStart, Version: "001"})
	r.Emit(Event{Kind: KindEnd, Version: "001"})

	got := r.Events()
	if len(got) != 2 {
		t.Fatalf("expected 2 events, got %d", len(got))
	}
	if got[0].Kind != KindStart || got[1].Kind != KindEnd {
		t.Errorf("unexpected kinds: %+v", got)
	}

	r.Reset()
	if len(r.Events()) != 0 {
		t.Errorf("Reset did not clear events")
	}
}

func TestFuncSink(t *testing.T) {
	var got Event
	var s Sink = FuncSink(func(e Event) { got = e })
	s.Emit(Event{Version: "v"})
	if got.Version != "v" {
		t.Errorf("FuncSink not invoked")
	}
}

func TestMultiSinkFanOut(t *testing.T) {
	r1, r2 := NewRecorderSink(), NewRecorderSink()
	m := MultiSink{r1, nil, r2}
	m.Emit(Event{Version: "x"})
	if len(r1.Events()) != 1 || len(r2.Events()) != 1 {
		t.Errorf("fan-out failed")
	}
}

func TestChannelSinkBufferAndDrop(t *testing.T) {
	c := NewChannelSink(2)
	c.Emit(Event{Version: "1"})
	c.Emit(Event{Version: "2"})
	c.Emit(Event{Version: "3"}) // dropped
	c.Emit(Event{Version: "4"}) // dropped

	if d := c.Dropped(); d != 2 {
		t.Errorf("expected 2 dropped, got %d", d)
	}
	if len(c.Ch) != 2 {
		t.Errorf("expected buffer len 2, got %d", len(c.Ch))
	}
	c.Close()
}

func TestChannelSinkNonBlocking(t *testing.T) {
	c := NewChannelSink(1)
	done := make(chan struct{})
	go func() {
		for range 100 {
			c.Emit(Event{})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ChannelSink blocked")
	}
}

func TestChannelSinkCloseConcurrentEmit(t *testing.T) {
	c := NewChannelSink(8)

	const emitters = 16
	const emits = 512

	var wg sync.WaitGroup
	panics := make(chan any, emitters)
	for range emitters {
		wg.Go(func() {
			defer func() {
				if r := recover(); r != nil {
					panics <- r
				}
			}()
			for range emits {
				c.Emit(Event{Version: "001"})
			}
		})
	}

	wg.Go(func() {
		c.Close()
		c.Close()
	})

	wg.Wait()
	close(panics)

	for p := range panics {
		t.Fatalf("ChannelSink Emit/Close panicked: %v", p)
	}

	c.Emit(Event{Version: "after-close"})
	if c.Dropped() == 0 {
		t.Fatal("Emit after Close should be counted as dropped")
	}
}

func TestJSONSink(t *testing.T) {
	var buf bytes.Buffer
	s := NewJSONSink(&buf)
	s.Emit(Event{Kind: KindExec, Version: "001", SQL: "SELECT 1"})
	s.Emit(Event{Kind: KindEnd, Version: "001"})

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d: %q", len(lines), buf.String())
	}
	var e Event
	if err := json.Unmarshal([]byte(lines[0]), &e); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if e.SQL != "SELECT 1" {
		t.Errorf("SQL not serialized: %+v", e)
	}
}

func TestJSONSinkConcurrent(t *testing.T) {
	var buf bytes.Buffer
	s := NewJSONSink(&buf)
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			s.Emit(Event{SQL: "x"})
		})
	}
	wg.Wait()
	if c := strings.Count(buf.String(), "\n"); c != 50 {
		t.Errorf("expected 50 lines, got %d", c)
	}
}

func TestScopeContext(t *testing.T) {
	r := NewRecorderSink()
	ctx := WithScope(context.Background(), r, "001", "test", DirectionUp)

	if SinkFromContext(ctx) != r {
		t.Errorf("sink not retrievable from context")
	}
	if SinkFromContext(context.Background()) != nil {
		t.Errorf("expected nil sink on empty ctx")
	}

	// nil sink shouldn't poison the ctx
	ctx2 := WithScope(context.Background(), nil, "", "", "")
	if SinkFromContext(ctx2) != nil {
		t.Errorf("nil sink should not be stored")
	}
}

func TestNopSink(t *testing.T) {
	NopSink{}.Emit(Event{}) // must not panic
}

func TestSQLHelpers(t *testing.T) {
	sql := "SELECT * FROM users WHERE id = 42 AND email = 'a@b.test'"
	if got := OperationOf(sql); got != "select" {
		t.Fatalf("OperationOf = %q, want select", got)
	}
	if got := NormalizeSQL(sql); got != "SELECT * FROM users WHERE id = ? AND email = ?" {
		t.Fatalf("NormalizeSQL = %q", got)
	}
	if got := BindSQL("SELECT * FROM users WHERE id = $1 AND email = ?", []any{42, "a'b"}); got != "SELECT * FROM users WHERE id = 42 AND email = 'a''b'" {
		t.Fatalf("BindSQL = %q", got)
	}
}

func TestAnalyzerSinkSlowAndNPlus1(t *testing.T) {
	rec := NewRecorderSink()
	cfg := AnalyzerConfig{
		SlowThreshold:   time.Millisecond,
		NPlus1Threshold: 3,
		NPlus1Window:    time.Second,
		NPlus1Cooldown:  time.Second,
		BindArgs:        true,
	}
	sink := NewAnalyzerSink(rec, cfg)
	for i := range 3 {
		sink.Emit(Event{
			Kind:      KindExec,
			StartedAt: time.Now().Add(time.Duration(i) * time.Millisecond),
			Duration:  2 * time.Millisecond,
			SQL:       "SELECT * FROM users WHERE id = ?",
			Args:      []any{i + 1},
		})
	}

	events := rec.Events()
	if len(events) != 3 {
		t.Fatalf("events len=%d", len(events))
	}
	last := events[2]
	if last.ID != 3 {
		t.Fatalf("ID = %d, want 3", last.ID)
	}
	if !last.Slow {
		t.Fatalf("expected slow marker")
	}
	if !last.NPlus1 || last.NPlus1Count != 3 || !last.NPlus1Alert {
		t.Fatalf("bad N+1 marker: %+v", last)
	}
	if last.Operation != "select" {
		t.Fatalf("operation=%q", last.Operation)
	}
	if last.BoundSQL != "SELECT * FROM users WHERE id = 3" {
		t.Fatalf("BoundSQL=%q", last.BoundSQL)
	}
}

func TestFilter(t *testing.T) {
	f, err := ParseFilter("op:select d>10ms slow users")
	if err != nil {
		t.Fatalf("ParseFilter: %v", err)
	}
	if !f.Match(Event{
		Kind:      KindExec,
		Operation: "select",
		Duration:  20 * time.Millisecond,
		SQL:       "SELECT * FROM users",
		Slow:      true,
	}) {
		t.Fatalf("expected filter match")
	}
	if f.Match(Event{
		Kind:      KindExec,
		Operation: "insert",
		Duration:  20 * time.Millisecond,
		SQL:       "INSERT INTO users",
		Slow:      true,
	}) {
		t.Fatalf("expected filter miss")
	}
}

func TestSummaryAndExport(t *testing.T) {
	events := []Event{
		{Kind: KindExec, SQL: "SELECT * FROM users WHERE id = 1", Duration: 10 * time.Millisecond, SQLTemplate: "SELECT * FROM users WHERE id = ?"},
		{Kind: KindExec, SQL: "SELECT * FROM users WHERE id = 2", Duration: 20 * time.Millisecond, SQLTemplate: "SELECT * FROM users WHERE id = ?", Slow: true},
		{Kind: KindEnd, Error: "boom"},
	}
	s := Summarize(events)
	if s.Total != 3 || s.Execs != 2 || s.Errors != 1 || len(s.Queries) != 1 {
		t.Fatalf("bad summary: %+v", s)
	}
	if s.Queries[0].Count != 2 || s.Queries[0].AvgDuration != 15*time.Millisecond {
		t.Fatalf("bad query stat: %+v", s.Queries[0])
	}

	var jsonl bytes.Buffer
	if err := WriteJSONL(&jsonl, events); err != nil {
		t.Fatalf("WriteJSONL: %v", err)
	}
	if strings.Count(jsonl.String(), "\n") != 3 {
		t.Fatalf("bad JSONL: %q", jsonl.String())
	}

	var md bytes.Buffer
	if err := WriteMarkdown(&md, events); err != nil {
		t.Fatalf("WriteMarkdown: %v", err)
	}
	if !strings.Contains(md.String(), "Queen Tap Report") {
		t.Fatalf("bad markdown: %q", md.String())
	}
}
