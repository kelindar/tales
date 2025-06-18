package mock

import (
	"iter"
	"sync"
	"time"
)

// Service provides an in-memory circular buffer based logger for testing.
type Service struct {
	mu       sync.RWMutex
	capacity int
	buf      []logEntry
	next     int
	size     int
}

type logEntry struct {
	ts     time.Time
	text   string
	actors []uint32
}

// NewService creates a new in-memory service with the given capacity.
func NewService(capacity int) *Service {
	if capacity <= 0 {
		capacity = 1
	}
	return &Service{capacity: capacity, buf: make([]logEntry, capacity)}
}

// Log stores a log entry in the circular buffer.
func (s *Service) Log(text string, actors ...uint32) error {
	if len(text) == 0 || len(actors) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := logEntry{ts: time.Now(), text: text, actors: append([]uint32(nil), actors...)}
	s.buf[s.next] = entry
	s.next = (s.next + 1) % s.capacity
	if s.size < s.capacity {
		s.size++
	}
	return nil
}

// Query returns entries containing all specified actors within the time range.
func (s *Service) Query(from, to time.Time, actors ...uint32) iter.Seq2[time.Time, string] {
	return func(yield func(time.Time, string) bool) {
		if len(actors) == 0 {
			return
		}

		s.mu.RLock()
		defer s.mu.RUnlock()

		if s.size == 0 {
			return
		}

		// Determine the oldest entry in the circular buffer
		idx := s.next - s.size
		if idx < 0 {
			idx += s.capacity
		}

		for i := 0; i < s.size; i++ {
			e := s.buf[idx]
			idx = (idx + 1) % s.capacity

			if e.ts.Before(from) || e.ts.After(to) {
				continue
			}
			if containsAll(e.actors, actors) {
				if !yield(e.ts, e.text) {
					return
				}
			}
		}
	}
}

func containsAll(have, want []uint32) bool {
	if len(want) == 0 {
		return false
	}
	for _, w := range want {
		found := false
		for _, h := range have {
			if h == w {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// Close resets the buffer and releases any held resources.
func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf = nil
	s.next = 0
	s.size = 0
	s.capacity = 0
	return nil
}
