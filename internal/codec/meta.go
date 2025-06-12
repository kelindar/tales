package codec

import (
	"encoding/json"
	"time"
)

// Metadata represents the metadata structure for log files.
type Metadata struct {
	Magic           string       `json:"magic"`
	Version         uint32       `json:"version"`
	DayStart        int64        `json:"day_start"`
	ChunkCount      uint32       `json:"chunk_count"`
	Chunks          []ChunkEntry `json:"chunks"`
	TailSize        uint32       `json:"tail_size"`
	TotalDataSize   uint32       `json:"total_data_size"`
	TotalBitmapSize uint32       `json:"total_bitmap_size"`
}

// NewMetadata creates a new metadata instance with default values.
func NewMetadata(dayStart time.Time) *Metadata {
	return &Metadata{
		Magic:    "TAIL",
		Version:  1,
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

// Update adds a new chunk and updates bitmap size.
func (m *Metadata) Update(offset uint64, compressedSize, uncompressedSize, bitmapSize uint32) {
	newChunk := NewChunkEntry(offset, compressedSize, uncompressedSize)
	m.Chunks = append(m.Chunks, newChunk)
	m.TotalDataSize += compressedSize
	m.ChunkCount = uint32(len(m.Chunks))
	m.TotalBitmapSize = bitmapSize
}
