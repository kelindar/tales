package codec

import (
	"encoding/binary"
	"time"
	"unsafe"

	"github.com/kelindar/tales/internal/seq"
)

// LogEntry represents a single log entry as raw bytes
type LogEntry []byte

// NewLogEntry creates a new log entry from components
func NewLogEntry(sequenceID uint32, text string, actors []uint32) (LogEntry, error) {
	// Pre-allocate with exact size
	size := 4 + // sequence ID
		2 + // text length (uint16)
		2 + // actor count (uint16)
		len(text) + // text bytes
		len(actors)*4 // actor IDs (4 bytes each)

	buf := make([]byte, 0, size)

	// Write sequence ID (4 bytes)
	buf = binary.LittleEndian.AppendUint32(buf, sequenceID)

	// Write text length (uint16)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(text)))

	// Write actor count (uint16)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(actors)))

	// Write text (UTF-8)
	buf = append(buf, text...)

	// Write actor IDs (4 bytes each)
	for _, actor := range actors {
		buf = binary.LittleEndian.AppendUint32(buf, actor)
	}

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
	if len(e) < 8 {
		return 0
	}
	textLen := binary.LittleEndian.Uint16(e[4:6])
	actorCount := binary.LittleEndian.Uint16(e[6:8])
	return 8 + uint32(textLen) + uint32(actorCount)*4
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
	if len(e) < 8 { // 4 bytes sequence ID + 2 bytes text length + 2 bytes actor count
		return ""
	}

	// Text starts after sequence ID (4) + text length (2) + actor count (2)
	i0 := 8
	i1 := i0 + int(binary.LittleEndian.Uint16(e[4:6]))
	if i1 > len(e) {
		return ""
	}

	return unsafe.String(unsafe.SliceData(e[i0:i1]), i1-i0)
}

// Actors extracts the actor IDs from a log entry
func (e LogEntry) Actors() []uint32 {
	if len(e) < 8 { // 4 bytes sequence ID + 2 bytes text length + 2 bytes actor count
		return nil
	}

	// Read text length and actor count (uint16 each)
	textLen := binary.LittleEndian.Uint16(e[4:6])
	actorCount := binary.LittleEndian.Uint16(e[6:8])

	// Actors start after sequence ID (4) + text length (2) + actor count (2) + text
	actorsStart := 8 + int(textLen)
	actorsEnd := actorsStart + int(actorCount)*4

	// Check bounds
	if actorsEnd > len(e) {
		return nil
	}

	// Read actor IDs (4 bytes each)
	actors := make([]uint32, actorCount)
	pos := actorsStart
	for i := uint16(0); i < actorCount; i++ {
		actors[i] = binary.LittleEndian.Uint32(e[pos : pos+4])
		pos += 4
	}

	return actors
}
