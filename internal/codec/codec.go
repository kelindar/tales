package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	// Magic constants for file format validation
	TailMagic   = "TAIL"
	FileVersion = uint32(1)

	// Fixed sizes for binary structures
	ChunkEntrySize = 16 // 8 bytes offset + 4 bytes compressed size + 4 bytes uncompressed size
	IndexEntrySize = 20 // 4 bytes timestamp + 4 bytes actor_id + 8 bytes offset + 4 bytes size
)

// LogEntry represents a single log entry as raw bytes
type LogEntry []byte

// IndexEntry represents an index entry as raw bytes
type IndexEntry []byte

// ChunkEntry represents a chunk entry as raw bytes
type ChunkEntry []byte

// TailMetadata represents tail metadata as raw bytes
type TailMetadata []byte

// NewLogEntry creates a new log entry from components
func NewLogEntry(sequenceID uint32, text string, actors []uint32) (LogEntry, error) {
	buf := &bytes.Buffer{}

	// Write sequence ID (4 bytes)
	if err := binary.Write(buf, binary.LittleEndian, sequenceID); err != nil {
		return nil, fmt.Errorf("failed to write sequence ID: %w", err)
	}

	// Write text length (varint)
	if err := writeVarint(buf, uint64(len(text))); err != nil {
		return nil, fmt.Errorf("failed to write text length: %w", err)
	}

	// Write actor count (varint)
	if err := writeVarint(buf, uint64(len(actors))); err != nil {
		return nil, fmt.Errorf("failed to write actor count: %w", err)
	}

	// Write text (UTF-8)
	if _, err := buf.WriteString(text); err != nil {
		return nil, fmt.Errorf("failed to write text: %w", err)
	}

	// Write actor IDs (4 bytes each)
	for _, actor := range actors {
		if err := binary.Write(buf, binary.LittleEndian, actor); err != nil {
			return nil, fmt.Errorf("failed to write actor ID: %w", err)
		}
	}

	return LogEntry(buf.Bytes()), nil
}

// SequenceID extracts the sequence ID from a log entry
func (e LogEntry) SequenceID() uint32 {
	if len(e) < 4 {
		return 0
	}
	return binary.LittleEndian.Uint32(e[:4])
}

// Text extracts the text from a log entry
func (e LogEntry) Text() string {
	if len(e) < 4 {
		return ""
	}

	pos := 4 // Skip sequence ID

	// Read text length (varint)
	textLen, consumed := readVarintFromBytes(e[pos:])
	if consumed == 0 {
		return ""
	}
	pos += consumed

	// Skip actor count (varint)
	_, consumed = readVarintFromBytes(e[pos:])
	if consumed == 0 {
		return ""
	}
	pos += consumed

	// Read text (UTF-8)
	if pos+int(textLen) > len(e) {
		return ""
	}
	return string(e[pos : pos+int(textLen)])
}

// Actors extracts the actor IDs from a log entry
func (e LogEntry) Actors() []uint32 {
	if len(e) < 4 {
		return nil
	}

	pos := 4 // Skip sequence ID

	// Read text length (varint)
	textLen, consumed := readVarintFromBytes(e[pos:])
	if consumed == 0 {
		return nil
	}
	pos += consumed

	// Read actor count (varint)
	actorCount, consumed := readVarintFromBytes(e[pos:])
	if consumed == 0 {
		return nil
	}
	pos += consumed

	// Skip text
	pos += int(textLen)

	// Read actor IDs (4 bytes each)
	if pos+int(actorCount)*4 > len(e) {
		return nil
	}
	actors := make([]uint32, actorCount)
	for i := uint64(0); i < actorCount; i++ {
		actors[i] = binary.LittleEndian.Uint32(e[pos : pos+4])
		pos += 4
	}

	return actors
}

// NewIndexEntry creates a new index entry
func NewIndexEntry(timestamp, actorID uint32, offset uint64, size uint32) IndexEntry {
	buf := make([]byte, IndexEntrySize)
	binary.LittleEndian.PutUint32(buf[0:4], timestamp)
	binary.LittleEndian.PutUint32(buf[4:8], actorID)
	binary.LittleEndian.PutUint64(buf[8:16], offset)
	binary.LittleEndian.PutUint32(buf[16:20], size)
	return IndexEntry(buf)
}

// Timestamp extracts the timestamp from an index entry
func (e IndexEntry) Timestamp() uint32 {
	if len(e) < 4 {
		return 0
	}
	return binary.LittleEndian.Uint32(e[0:4])
}

// ActorID extracts the actor ID from an index entry
func (e IndexEntry) ActorID() uint32 {
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

// Size extracts the size from an index entry
func (e IndexEntry) Size() uint32 {
	if len(e) < 20 {
		return 0
	}
	return binary.LittleEndian.Uint32(e[16:20])
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

// writeVarint writes a varint to the buffer
func writeVarint(buf *bytes.Buffer, value uint64) error {
	for value >= 0x80 {
		if err := buf.WriteByte(byte(value) | 0x80); err != nil {
			return err
		}
		value >>= 7
	}

	return buf.WriteByte(byte(value))
}

// readVarintFromBytes reads a varint from a byte slice and returns the value and bytes consumed
func readVarintFromBytes(data []byte) (uint64, int) {
	var result uint64
	var shift uint
	var consumed int

	for i, b := range data {
		result |= uint64(b&0x7F) << shift
		consumed = i + 1
		if b&0x80 == 0 {
			return result, consumed
		}
		shift += 7
		if shift >= 64 {
			return 0, 0 // Invalid varint
		}
	}

	return 0, 0 // Incomplete varint
}
