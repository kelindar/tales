// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package codec

import (
	"encoding/binary"
	"iter"
	"time"
	"unsafe"

	"github.com/kelindar/tales/internal/seq"
)

// LogEntry represents a single log entry as raw bytes
type LogEntry []byte

// NewLogEntry creates a new log entry from components
func NewLogEntry(sequenceID uint32, text string, actors []uint32) (LogEntry, error) {
	actlen := len(actors) * 4
	strlen := len(text)
	length := 4 + 2 + 2 + actlen + strlen // sequenceID + size + cutoff + actors + text
	cutoff := 8 + actlen                  // offset where text starts (after sequenceID + size + midpoint + actors)

	buf := make([]byte, 0, length)
	buf = binary.LittleEndian.AppendUint32(buf, sequenceID)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(length))
	buf = binary.LittleEndian.AppendUint16(buf, uint16(cutoff))

	for _, actor := range actors {
		buf = binary.LittleEndian.AppendUint32(buf, actor)
	}

	buf = append(buf, text...)
	return LogEntry(buf), nil
}

// ID extracts the sequence ID from a log entry
// Time reconstructs the timestamp from the day-start and sequence ID.
func (e LogEntry) Time(dayStart time.Time) time.Time {
	return seq.TimeOf(e.ID(), dayStart)
}

// ID extracts the sequence ID from a log entry
// Size returns the total size of the log entry in bytes.
func (e LogEntry) Size() uint32 {
	if len(e) < 6 {
		return 0
	}
	return uint32(binary.LittleEndian.Uint16(e[4:6]))
}

// ID extracts the sequence ID from a log entry
func (e LogEntry) ID() uint32 {
	if len(e) < 4 {
		return 0
	}
	return binary.LittleEndian.Uint32(e[:4])
}

// Text extracts the text from a log entry
func (e LogEntry) Text() string {
	if len(e) < 8 { // 4 bytes sequence ID + 2 bytes size + 2 bytes midpoint
		return ""
	}

	size := binary.LittleEndian.Uint16(e[4:6])
	midpoint := binary.LittleEndian.Uint16(e[6:8])

	// Validate bounds
	if int(size) > len(e) || int(midpoint) > len(e) || midpoint > size {
		return ""
	}

	textStart := int(midpoint)
	textEnd := int(size)

	if textStart >= textEnd {
		return ""
	}

	return unsafe.String(unsafe.SliceData(e[textStart:textEnd]), textEnd-textStart)
}

// Actors extracts the actor IDs from a log entry as an iterator
func (e LogEntry) Actors() iter.Seq[uint32] {
	const from = 8
	return func(yield func(uint32) bool) {
		if len(e) < 8 { // 4 bytes sequence ID + 2 bytes size + 2 bytes midpoint
			return
		}

		// Validate bounds
		cutoff := binary.LittleEndian.Uint16(e[6:8])
		if int(cutoff) > len(e) || cutoff < 8 {
			return
		}

		// Must be aligned to 4-byte boundaries
		until := int(cutoff)
		if (until-from)%4 != 0 {
			return
		}

		// Yield actor IDs (4 bytes each)
		for pos := from; pos < until; pos += 4 {
			if !yield(binary.LittleEndian.Uint32(e[pos : pos+4])) {
				return
			}
		}
	}
}
