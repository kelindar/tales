// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package codec

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"iter"
	"math"
	"slices"
	"time"
	"unsafe"
)

const (
	headerSize = 8
	MaxMillis  = 86_399_999
)

// LogEntry is one validated on-disk event frame.
type LogEntry []byte

func NewLogEntry(millis uint32, text string, actors []uint32) (LogEntry, error) {
	size := headerSize + len(actors)*4 + len(text)
	switch {
	case millis > MaxMillis:
		return nil, fmt.Errorf("timestamp offset %d exceeds UTC day", millis)
	case size > math.MaxUint16:
		return nil, fmt.Errorf("event frame too large: %d bytes", size)
	}

	entry := make([]byte, 0, size)
	entry = binary.LittleEndian.AppendUint32(entry, millis)
	entry = binary.LittleEndian.AppendUint16(entry, uint16(size))
	entry = binary.LittleEndian.AppendUint16(entry, uint16(headerSize+len(actors)*4))
	for _, actor := range actors {
		entry = binary.LittleEndian.AppendUint32(entry, actor)
	}
	return append(entry, text...), nil
}

// ValidateEntry validates the first frame in data and returns its exact view.
func ValidateEntry(data []byte) (LogEntry, int, error) {
	if len(data) < headerSize {
		return nil, 0, fmt.Errorf("truncated event header")
	}
	size := int(binary.LittleEndian.Uint16(data[4:6]))
	actorsEnd := int(binary.LittleEndian.Uint16(data[6:8]))
	switch {
	case size < headerSize || size > len(data):
		return nil, 0, fmt.Errorf("invalid event size %d", size)
	case actorsEnd < headerSize || actorsEnd > size || (actorsEnd-headerSize)%4 != 0:
		return nil, 0, fmt.Errorf("invalid actor boundary %d", actorsEnd)
	case binary.LittleEndian.Uint32(data[:4]) > MaxMillis:
		return nil, 0, fmt.Errorf("invalid millisecond offset")
	}
	return LogEntry(data[:size]), size, nil
}

// ValidateEntries validates an entire decompressed chunk once at its boundary.
func ValidateEntries(data []byte, expected uint32) ([]LogEntry, error) {
	entries := make([]LogEntry, 0, expected)
	for len(data) > 0 {
		entry, size, err := ValidateEntry(data)
		if err != nil {
			return nil, fmt.Errorf("event %d: %w", len(entries), err)
		}
		entries = append(entries, entry)
		data = data[size:]
	}
	if uint64(len(entries)) != uint64(expected) {
		return nil, fmt.Errorf("entry count mismatch: got %d, want %d", len(entries), expected)
	}
	return entries, nil
}

func (e LogEntry) Millis() uint32 { return binary.LittleEndian.Uint32(e[:4]) }

func (e LogEntry) payload() []byte {
	return e[binary.LittleEndian.Uint16(e[6:8]):binary.LittleEndian.Uint16(e[4:6])]
}

func (e LogEntry) Actors() iter.Seq[uint32] {
	return func(yield func(uint32) bool) {
		end := int(binary.LittleEndian.Uint16(e[6:8]))
		for offset := headerSize; offset < end; offset += 4 {
			if !yield(binary.LittleEndian.Uint32(e[offset : offset+4])) {
				return
			}
		}
	}
}

// Event is a zero-copy view over a validated event frame. Returned slices are
// read-only by contract; Clone creates independent storage.
type Event struct {
	day  time.Time
	data LogEntry
}

func NewEvent(day time.Time, entry LogEntry) Event {
	return Event{day: day, data: entry}
}

func (e Event) Time() time.Time {
	return e.day.Add(time.Duration(e.data.Millis()) * time.Millisecond)
}

func (e Event) Bytes() []byte { return e.data.payload() }

func (e Event) Text() string {
	payload := e.Bytes()
	return unsafe.String(unsafe.SliceData(payload), len(payload))
}

func (e Event) JSON() json.RawMessage { return json.RawMessage(e.Bytes()) }

func (e Event) Actors() iter.Seq[uint32] { return e.data.Actors() }

func (e Event) Clone() Event {
	return Event{day: e.day, data: slices.Clone(e.data)}
}
