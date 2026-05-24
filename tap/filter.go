package tap

import (
	"fmt"
	"strings"
	"time"
)

type tokenKind int

const (
	tokenText tokenKind = iota
	tokenDurationGT
	tokenDurationLT
	tokenError
	tokenSlow
	tokenNPlus1
	tokenOperation
)

type filterToken struct {
	kind tokenKind
	text string
	dur  time.Duration
}

// Filter matches tap events using terms such as "op:select d>100ms error".
type Filter struct {
	tokens []filterToken
}

// ParseFilter turns a compact filter string into matchable tokens.
func ParseFilter(expr string) (Filter, error) {
	parts := strings.Fields(expr)
	tokens := make([]filterToken, 0, len(parts))
	for _, part := range parts {
		switch {
		case part == "error":
			tokens = append(tokens, filterToken{kind: tokenError})
		case part == "slow":
			tokens = append(tokens, filterToken{kind: tokenSlow})
		case part == "n+1" || part == "nplus1":
			tokens = append(tokens, filterToken{kind: tokenNPlus1})
		case strings.HasPrefix(part, "op:"):
			op := strings.TrimSpace(strings.TrimPrefix(part, "op:"))
			if op == "" {
				return Filter{}, fmt.Errorf("empty op filter")
			}
			tokens = append(tokens, filterToken{kind: tokenOperation, text: strings.ToLower(op)})
		case strings.HasPrefix(part, "d>"):
			d, err := time.ParseDuration(strings.TrimPrefix(part, "d>"))
			if err != nil {
				return Filter{}, fmt.Errorf("bad duration filter %q: %w", part, err)
			}
			tokens = append(tokens, filterToken{kind: tokenDurationGT, dur: d})
		case strings.HasPrefix(part, "d<"):
			d, err := time.ParseDuration(strings.TrimPrefix(part, "d<"))
			if err != nil {
				return Filter{}, fmt.Errorf("bad duration filter %q: %w", part, err)
			}
			tokens = append(tokens, filterToken{kind: tokenDurationLT, dur: d})
		default:
			tokens = append(tokens, filterToken{kind: tokenText, text: strings.ToLower(part)})
		}
	}
	return Filter{tokens: tokens}, nil
}

func (f Filter) Match(e Event) bool {
	for _, tok := range f.tokens {
		switch tok.kind {
		case tokenText:
			haystack := strings.ToLower(strings.Join([]string{
				e.Version,
				e.Name,
				e.SQL,
				e.SQLTemplate,
				e.BoundSQL,
				e.Error,
			}, " "))
			if !strings.Contains(haystack, tok.text) {
				return false
			}
		case tokenDurationGT:
			if e.Duration <= tok.dur {
				return false
			}
		case tokenDurationLT:
			if e.Duration >= tok.dur {
				return false
			}
		case tokenError:
			if e.Error == "" {
				return false
			}
		case tokenSlow:
			if !e.Slow {
				return false
			}
		case tokenNPlus1:
			if !e.NPlus1 {
				return false
			}
		case tokenOperation:
			op := e.Operation
			if op == "" {
				op = OperationOf(e.SQL)
			}
			if strings.ToLower(op) != tok.text {
				return false
			}
		}
	}
	return true
}
