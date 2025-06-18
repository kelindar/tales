package tales

import (
	"iter"
	"time"
)

// Logger provides the capability to log events.
type Logger interface {
	Log(text string, actors ...uint32) error
}

// Querier provides the capability to query logged events.
type Querier interface {
	Query(from, to time.Time, actors ...uint32) iter.Seq2[time.Time, string]
}

// Manager combines logging and querying capabilities and allows closing the service.
type Manager interface {
	Logger
	Querier
	Close() error
}

var (
	_ Logger  = (*Service)(nil)
	_ Querier = (*Service)(nil)
	_ Manager = (*Service)(nil)
)
