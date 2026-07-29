package mock

import (
	"cmp"
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"iter"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/kelindar/tales"
	"github.com/kelindar/tales/internal/codec"
)

// Service is an in-memory implementation of tales.Manager for tests.
type Service struct {
	mu        sync.RWMutex
	capacity  int
	buf       []logEntry
	next      int
	size      int
	serialDay time.Time
	serial    uint32
	closed    bool
}

type logEntry struct {
	day      time.Time
	entry    codec.LogEntry
	position uint32
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
	if !s.serialDay.Equal(day) {
		s.serialDay, s.serial = day, 0
	}
	if s.serial == math.MaxUint32 {
		return fmt.Errorf("writer-day position exceeds cursor capacity")
	}
	s.buf[s.next] = logEntry{day: day, entry: entry, position: s.serial}
	s.serial++
	s.next = (s.next + 1) % s.capacity
	if s.size < s.capacity {
		s.size++
	}
	return nil
}

func (s *Service) Scan(ctx context.Context, from, to time.Time, actors ...uint32) iter.Seq2[tales.Event, error] {
	return func(yield func(tales.Event, error) bool) {
		refs, err := s.query(ctx, from, to, actors)
		if err != nil {
			yield(tales.Event{}, err)
			return
		}
		for _, ref := range refs {
			if !yield(ref.event, nil) {
				return
			}
		}
	}
}

func (s *Service) Page(ctx context.Context, from, to time.Time, cursor tales.Cursor, limit int, actors ...uint32) ([]tales.Event, tales.Cursor, error) {
	if limit < 1 || limit > 1000 {
		return nil, tales.Zero, fmt.Errorf("invalid limit")
	}
	refs, err := s.query(ctx, from, to, actors)
	if err != nil {
		return nil, tales.Zero, err
	}
	ascending := !from.After(to)
	var cursorTime int64
	var cursorPosition uint32
	if cursor != tales.Zero {
		if len(cursor) != 27 {
			return nil, tales.Zero, fmt.Errorf("invalid cursor")
		}
		var data [20]byte
		n, err := base64.RawURLEncoding.Decode(data[:], []byte(cursor))
		switch {
		case err != nil, n != len(data):
			return nil, tales.Zero, fmt.Errorf("invalid cursor")
		}
		cursorTime = int64(binary.BigEndian.Uint64(data[0:8]))
		encoded := binary.BigEndian.Uint32(data[16:20])
		lower, upper := from, to
		if from.After(to) {
			lower, upper = to, from
		}
		at := time.UnixMilli(cursorTime)
		switch {
		case encoded == 0:
			return nil, tales.Zero, fmt.Errorf("invalid cursor")
		case at.Before(lower), at.After(upper):
			return nil, tales.Zero, fmt.Errorf("cursor timestamp outside query range")
		}
		cursorPosition = encoded - 1
	}
	filtered := refs[:0]
	for _, ref := range refs {
		if cursor != tales.Zero {
			order := cmp.Compare(ref.event.Time().UnixMilli(), cursorTime)
			if order == 0 {
				order = cmp.Compare(ref.position, cursorPosition)
			}
			if ascending && order <= 0 || !ascending && order >= 0 {
				continue
			}
		}
		filtered = append(filtered, ref)
		if len(filtered) == limit+1 {
			break
		}
	}
	events := make([]tales.Event, min(limit, len(filtered)))
	for i := range events {
		events[i] = filtered[i].event
	}
	next := tales.Zero
	if len(filtered) > limit {
		last := filtered[limit-1]
		var data [20]byte
		binary.BigEndian.PutUint64(data[0:8], uint64(last.event.Time().UnixMilli()))
		binary.BigEndian.PutUint32(data[16:20], last.position+1)
		next = tales.Cursor(base64.RawURLEncoding.EncodeToString(data[:]))
	}
	return events, next, nil
}

type eventRef struct {
	event    tales.Event
	position uint32
}

func (s *Service) query(ctx context.Context, from, to time.Time, actors []uint32) ([]eventRef, error) {
	switch {
	case ctx == nil || len(actors) == 0:
		return nil, fmt.Errorf("invalid query arguments")
	case ctx.Err() != nil:
		return nil, ctx.Err()
	}
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, fmt.Errorf("tales service is closed")
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

	lower, upper := from, to
	ascending := !from.After(to)
	if !ascending {
		lower, upper = to, from
	}
	refs := make([]eventRef, 0, len(entries))
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		event := codec.NewEvent(entry.day, entry.entry)
		if event.Time().Before(lower) || event.Time().After(upper) || !containsAll(entry.entry, actors) {
			continue
		}
		refs = append(refs, eventRef{event: event, position: entry.position})
	}
	sort.Slice(refs, func(i, j int) bool {
		order := refs[i].event.Time().Compare(refs[j].event.Time())
		if order == 0 {
			order = cmp.Compare(refs[i].position, refs[j].position)
		}
		return ascending && order < 0 || !ascending && order > 0
	})
	return refs, nil
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
	switch {
	case ctx == nil:
		return fmt.Errorf("nil context")
	case ctx.Err() != nil:
		return ctx.Err()
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
