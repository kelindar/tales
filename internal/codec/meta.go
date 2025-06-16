package codec

import (
	"encoding/json"
	"time"
)

// IndexEntry represents metadata about an actor bitmap within a chunk.
type IndexEntry [3]uint

// ChunkEntry represents a chunk entry stored in metadata.
type ChunkEntry struct {
	Location [3]uint               `json:"loc"`
	Actors   map[uint32]IndexEntry `json:"act,omitempty"`
}

// Metadata represents the metadata structure for log files.
type Metadata struct {
	Date   int64        `json:"date"`
	Length uint32       `json:"length"`
	Chunks []ChunkEntry `json:"chunks"`
}

// NewMetadata creates a new metadata instance with default values.
func NewMetadata(dayStart time.Time) *Metadata {
	return &Metadata{
		Date: dayStart.UnixNano(),
	}
}

// EncodeMetadata encodes metadata into JSON format.
func EncodeMetadata(meta *Metadata) ([]byte, error) {
	return json.Marshal(meta)
}

// DecodeMetadata decodes metadata from JSON format.
func DecodeMetadata(data []byte) (*Metadata, error) {
	var meta Metadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

// Append adds a new chunk with section sizes.
func (m *Metadata) Append(offset uint64, bitmapSize, logSize uint32, actors map[uint32]IndexEntry) *Metadata {
	newChunk := NewChunkEntry(offset, bitmapSize, logSize, actors)
	m.Chunks = append(m.Chunks, newChunk)
	m.Length = uint32(len(m.Chunks))
	return m
}

// NewIndexEntry allocates a new index entry.
func NewIndexEntry(timestamp uint32, offset uint64, size uint32) IndexEntry {
	return IndexEntry{uint(timestamp), uint(offset), uint(size)}
}

// NewChunkEntry creates a new chunk entry.
func NewChunkEntry(offset uint64, bitmapSize, logSize uint32, actors map[uint32]IndexEntry) ChunkEntry {
	return ChunkEntry{Location: [3]uint{uint(offset), uint(bitmapSize), uint(logSize)}, Actors: actors}
}

// BitmapOffset calculates the offset to the bitmap section within the merged file.
func (e ChunkEntry) BitmapOffset() uint32 { return 0 }

// LogOffset calculates the offset to the log section within the merged file.
func (e ChunkEntry) LogOffset() uint32 { return uint32(e.Location[1]) }

// TotalSize calculates the total size of the merged file.
func (e ChunkEntry) TotalSize() uint32 { return uint32(e.Location[1]) + uint32(e.Location[2]) }
