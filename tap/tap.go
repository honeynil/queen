// Package tap provides real-time observability of SQL executed during
// queen migrations. It captures per-statement events (SQL text, args,
// duration, rows affected, errors) and dispatches them to a Sink for
// rendering, logging, or assertion in tests.
//
// Tap is scoped to migration execution — it does not proxy or wrap your
// application's database/sql traffic. For SQL migrations Queen emits a
// single event with the full SQL text. For Go-function migrations users
// opt in by wrapping the transaction with Tx(ctx, tx) and calling
// ExecContext/QueryContext through the wrapper.
package tap

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"time"
)

// Kind classifies a tap event.
type Kind string

const (
	// KindStart fires once before a migration runs.
	KindStart Kind = "start"
	// KindTxBegin fires after a migration transaction begins.
	KindTxBegin Kind = "tx_begin"
	// KindTxCommit fires after a migration transaction commits.
	KindTxCommit Kind = "tx_commit"
	// KindTxRollback fires after a migration transaction rolls back.
	KindTxRollback Kind = "tx_rollback"
	// KindExec fires for each captured SQL statement.
	KindExec Kind = "exec"
	// KindEnd fires once after a migration finishes (success or failure).
	KindEnd Kind = "end"
)

// Direction matches queen.DirectionUp / queen.DirectionDown.
type Direction string

const (
	DirectionUp   Direction = "up"
	DirectionDown Direction = "down"
)

// Event is a single tap record.
type Event struct {
	ID           int64         `json:"id,omitempty"`
	Kind         Kind          `json:"kind"`
	Version      string        `json:"version"`
	Name         string        `json:"name"`
	Direction    Direction     `json:"direction"`
	Index        int           `json:"index,omitempty"`
	StartedAt    time.Time     `json:"started_at"`
	Duration     time.Duration `json:"duration_ns"`
	Operation    string        `json:"operation,omitempty"`
	SQL          string        `json:"sql,omitempty"`
	SQLTemplate  string        `json:"sql_template,omitempty"`
	BoundSQL     string        `json:"bound_sql,omitempty"`
	Args         []any         `json:"args,omitempty"`
	RowsAffected int64         `json:"rows_affected,omitempty"`
	Error        string        `json:"error,omitempty"`
	Slow         bool          `json:"slow,omitempty"`
	NPlus1       bool          `json:"n_plus_1,omitempty"`
	NPlus1Count  int           `json:"n_plus_1_count,omitempty"`
	NPlus1Alert  bool          `json:"n_plus_1_alert,omitempty"`
}

// Sink receives tap events. Implementations should be concurrency-safe and quick.
type Sink interface {
	Emit(Event)
}

// NopSink discards every event.
type NopSink struct{}

func (NopSink) Emit(Event) {}

// FuncSink adapts a function into a Sink.
type FuncSink func(Event)

func (f FuncSink) Emit(e Event) { f(e) }

// MultiSink fans events out to multiple sinks.
type MultiSink []Sink

func (m MultiSink) Emit(e Event) {
	for _, s := range m {
		if s != nil {
			s.Emit(e)
		}
	}
}

// ChannelSink is a non-blocking buffered event queue.
type ChannelSink struct {
	Ch      chan Event
	dropped int64
	mu      sync.Mutex
	closed  bool
}

// NewChannelSink creates a buffered channel sink.
func NewChannelSink(buffer int) *ChannelSink {
	if buffer <= 0 {
		buffer = 256
	}
	return &ChannelSink{Ch: make(chan Event, buffer)}
}

func (c *ChannelSink) Emit(e Event) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		c.dropped++
		return
	}

	select {
	case c.Ch <- e:
	default:
		c.dropped++
	}
}

// Dropped returns the number of events dropped due to a full buffer.
func (c *ChannelSink) Dropped() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dropped
}

// Close closes the underlying channel. Call after migrations finish.
func (c *ChannelSink) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return
	}
	c.closed = true
	close(c.Ch)
}

// JSONSink writes each event as a JSON line to w. Safe for concurrent use.
type JSONSink struct {
	w  io.Writer
	mu sync.Mutex
}

// NewJSONSink wraps w in a line-delimited JSON sink.
func NewJSONSink(w io.Writer) *JSONSink { return &JSONSink{w: w} }

// Emit writes one JSON line. Writer errors are ignored by design.
func (j *JSONSink) Emit(e Event) {
	j.mu.Lock()
	defer j.mu.Unlock()
	b, err := json.Marshal(e)
	if err != nil {
		return
	}
	b = append(b, '\n')
	_, _ = j.w.Write(b)
}

// RecorderSink collects all events in memory. Intended for tests.
type RecorderSink struct {
	mu     sync.Mutex
	events []Event
}

// NewRecorderSink returns an empty recorder.
func NewRecorderSink() *RecorderSink { return &RecorderSink{} }

// Emit appends e.
func (r *RecorderSink) Emit(e Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

// Events returns a copy of recorded events.
func (r *RecorderSink) Events() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Event, len(r.events))
	copy(out, r.events)
	return out
}

// Reset clears recorded events.
func (r *RecorderSink) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = nil
}

type ctxKey struct{}

// scope carries the active sink plus the migration coordinates needed to
// stamp Exec events emitted from inside a Go-function migration.
type scope struct {
	sink      Sink
	version   string
	name      string
	direction Direction
	mu        sync.Mutex
	nextIndex int
}

// WithScope returns a context carrying the given sink and migration metadata.
// Queen calls this internally before invoking a Go-function migration so that
// tap.ObserveTx(ctx, tx) can find the active sink.
func WithScope(ctx context.Context, sink Sink, version, name string, dir Direction) context.Context {
	if sink == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKey{}, &scope{
		sink:      sink,
		version:   version,
		name:      name,
		direction: dir,
	})
}

// fromContext retrieves the active scope, or nil if none is set.
func fromContext(ctx context.Context) *scope {
	if ctx == nil {
		return nil
	}
	s, _ := ctx.Value(ctxKey{}).(*scope)
	return s
}

// SinkFromContext returns the sink registered with WithScope, or nil.
// Useful when a migration wants to emit custom events.
func SinkFromContext(ctx context.Context) Sink {
	if s := fromContext(ctx); s != nil {
		return s.sink
	}
	return nil
}

func (s *scope) statementIndex() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextIndex++
	return s.nextIndex
}
