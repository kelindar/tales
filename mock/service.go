package mock

import (
	"context"
	"fmt"
	"iter"
	"sync"
	"time"

	"github.com/kelindar/tales"
	"github.com/kelindar/tales/internal/codec"
)

// Service is an in-memory implementation of tales.Manager for tests.
type Service struct {
	mu       sync.RWMutex
	capacity int
	buf      []logEntry
	next     int
	size     int
	closed   bool
}

type logEntry struct {
	day   time.Time
	entry codec.LogEntry
}

// NewService creates a fixed-capacity in-memory event log.
func NewService(capacity int) *Service {
	if capacity <= 0 {
		capacity = 1
	}
	return &Service{capacity: capacity, buf: make([]logEntry, capacity)}
}

func (s *Service) Log(text string, actors ...uint32) error {
	if text == "" || len(actors) == 0 {
		return fmt.Errorf("invalid event")
	}
	now := time.Now().UTC()
	day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	entry, err := codec.NewLogEntry(uint32(now.Sub(day)/time.Millisecond), text, actors)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("tales service is closed")
	}
	s.buf[s.next] = logEntry{day: day, entry: entry}
	s.next = (s.next + 1) % s.capacity
	if s.size < s.capacity {
		s.size++
	}
	return nil
}

func (s *Service) Query(ctx context.Context, from, to time.Time, actors ...uint32) iter.Seq2[tales.Event, error] {
	return func(yield func(tales.Event, error) bool) {
		if ctx == nil || from.After(to) || len(actors) == 0 {
			yield(tales.Event{}, fmt.Errorf("invalid query arguments"))
			return
		}
		s.mu.RLock()
		if s.closed {
			s.mu.RUnlock()
			yield(tales.Event{}, fmt.Errorf("tales service is closed"))
			return
		}
		index := s.next - s.size
		if index < 0 {
			index += s.capacity
		}
		entries := make([]logEntry, 0, s.size)
		for range s.size {
			entries = append(entries, s.buf[index])
			index = (index + 1) % s.capacity
		}
		s.mu.RUnlock()
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				yield(tales.Event{}, err)
				return
			}
			event := codec.NewEvent(entry.day, entry.entry)
			if event.Time().Before(from) || event.Time().After(to) || !containsAll(entry.entry, actors) {
				continue
			}
			if !yield(event, nil) {
				return
			}
		}
	}
}

func containsAll(entry codec.LogEntry, actors []uint32) bool {
	have := make(map[uint32]struct{})
	for actor := range entry.Actors() {
		have[actor] = struct{}{}
	}
	for _, actor := range actors {
		if _, ok := have[actor]; !ok {
			return false
		}
	}
	return true
}

func (s *Service) Sync(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return fmt.Errorf("tales service is closed")
	}
	return nil
}

func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("tales service is closed")
	}
	s.buf = nil
	s.next = 0
	s.size = 0
	s.closed = true
	return nil
}

var _ tales.Manager = (*Service)(nil)
