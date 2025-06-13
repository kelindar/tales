package codec

import (
	"encoding/binary"
	"time"
)

const (
	TailMagic      = "TAIL"
	ChunkEntrySize = 16 // 8 bytes offset + 4 bytes compressed size + 4 bytes uncompressed size
	IndexEntrySize = 24 // 4 bytes timestamp + 4 bytes actor_id + 8 bytes offset + 4 bytes compressed size + 4 bytes uncompressed size
)

// LogEntry represents a single log entry as raw bytes
type LogEntry []byte

// IndexEntry represents an index entry as raw bytes
type IndexEntry []byte

// ChunkEntry represents a chunk entry as raw bytes
type ChunkEntry []byte

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
	return dayStart.Add(time.Duration(e.ID()) * time.Second)
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

	// Read text length (uint16)
	textLen := binary.LittleEndian.Uint16(e[4:6])

	// Text starts after sequence ID (4) + text length (2) + actor count (2)
	textStart := 8
	textEnd := textStart + int(textLen)

	// Check bounds
	if textEnd > len(e) {
		return ""
	}

	return string(e[textStart:textEnd])
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

// NewIndexEntry creates a new index entry
func NewIndexEntry(timestamp, actorID uint32, offset uint64, compressedSize, uncompressedSize uint32) IndexEntry {
	buf := make([]byte, IndexEntrySize)
	binary.LittleEndian.PutUint32(buf[0:4], timestamp)
	binary.LittleEndian.PutUint32(buf[4:8], actorID)
	binary.LittleEndian.PutUint64(buf[8:16], offset)
	binary.LittleEndian.PutUint32(buf[16:20], compressedSize)
	binary.LittleEndian.PutUint32(buf[20:24], uncompressedSize)
	return IndexEntry(buf)
}

// Time extracts the timestamp from an index entry
func (e IndexEntry) Time() uint32 {
	if len(e) < 4 {
		return 0
	}
	return binary.LittleEndian.Uint32(e[0:4])
}

// Actor extracts the actor ID from an index entry
func (e IndexEntry) Actor() uint32 {
	if len(e) < 8 {
		return 0
	}
	return binary.LittleEndian.Uint32(e[4:8])
}

// Offset extracts the offset from an index entry
func (e IndexEntry) Offset() uint64 {
	if len(e) < 16 {
		return 0
	}
	return binary.LittleEndian.Uint64(e[8:16])
}

// CompressedSize extracts the compressed size from an index entry
func (e IndexEntry) CompressedSize() uint32 {
	if len(e) < 20 {
		return 0
	}
	return binary.LittleEndian.Uint32(e[16:20])
}

// UncompressedSize extracts the uncompressed size from an index entry
func (e IndexEntry) UncompressedSize() uint32 {
	if len(e) < 24 {
		return 0
	}
	return binary.LittleEndian.Uint32(e[20:24])
}

// NewChunkEntry creates a new chunk entry
func NewChunkEntry(offset uint64, compressedSize, uncompressedSize uint32) ChunkEntry {
	buf := make([]byte, ChunkEntrySize)
	binary.LittleEndian.PutUint64(buf[0:8], offset)
	binary.LittleEndian.PutUint32(buf[8:12], compressedSize)
	binary.LittleEndian.PutUint32(buf[12:16], uncompressedSize)
	return ChunkEntry(buf)
}

// Offset extracts the offset from a chunk entry
func (e ChunkEntry) Offset() uint64 {
	if len(e) < 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(e[0:8])
}

// CompressedSize extracts the compressed size from a chunk entry
func (e ChunkEntry) CompressedSize() uint32 {
	if len(e) < 12 {
		return 0
	}
	return binary.LittleEndian.Uint32(e[8:12])
}

// UncompressedSize extracts the uncompressed size from a chunk entry
func (e ChunkEntry) UncompressedSize() uint32 {
	if len(e) < 16 {
		return 0
	}
	return binary.LittleEndian.Uint32(e[12:16])
}
