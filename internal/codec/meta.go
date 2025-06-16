package codec

import (
	"encoding/json"
	"time"
)

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
