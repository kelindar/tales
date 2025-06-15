package codec

import (
	"encoding/json"
	"time"
)

// Metadata represents the metadata structure for log files.
type Metadata struct {
	DayStart   int64        `json:"day_start"`
	ChunkCount uint32       `json:"chunk_count"`
	Chunks     []ChunkEntry `json:"chunks"`
}

// NewMetadata creates a new metadata instance with default values.
func NewMetadata(dayStart time.Time) *Metadata {
	return &Metadata{
		DayStart: dayStart.UnixNano(),
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
func (m *Metadata) Append(offset uint64, indexSize, bitmapSize, logSize uint32) *Metadata {
	newChunk := NewChunkEntry(offset, indexSize, bitmapSize, logSize)
	m.Chunks = append(m.Chunks, newChunk)
	m.ChunkCount = uint32(len(m.Chunks))
	return m
}
