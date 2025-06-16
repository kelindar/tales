package codec

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetadata(t *testing.T) {
	t.Run("NewMetadata", func(t *testing.T) {
		dayStart := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		meta := NewMetadata(dayStart)

		assert.Equal(t, dayStart.UnixNano(), meta.Date)
		assert.Equal(t, uint32(0), meta.Length)
		assert.Empty(t, meta.Chunks)
	})

	t.Run("Update", func(t *testing.T) {
		meta := NewMetadata(time.Now())

		// Add first chunk
		meta.Append(0, 200, 300, nil)
		assert.Equal(t, uint32(1), meta.Length)
		assert.Len(t, meta.Chunks, 1)

		// Verify chunk data
		chunk := meta.Chunks[0]
		assert.Equal(t, uint64(0), chunk.Offset())
		assert.Equal(t, uint32(200), chunk.BitmapSize())
		assert.Equal(t, uint32(300), chunk.LogSize())

		// Add second chunk
		meta.Append(1, 250, 350, nil)
		assert.Equal(t, uint32(2), meta.Length)
		assert.Len(t, meta.Chunks, 2)
	})

	t.Run("EncodeDecodeMetadata", func(t *testing.T) {
		// Create metadata with some data
		dayStart := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		original := NewMetadata(dayStart)
		original.Append(0, 200, 300, nil)
		original.Append(1, 250, 350, nil)

		// Encode to JSON
		encoded, err := EncodeMetadata(original)
		require.NoError(t, err)
		assert.NotEmpty(t, encoded)

		// Verify it's valid JSON by checking it contains expected fields
		jsonStr := string(encoded)
		assert.Contains(t, jsonStr, "chunks")

		// Decode back
		decoded, err := DecodeMetadata(encoded)
		require.NoError(t, err)

		// Verify all fields match
		assert.Equal(t, original.Date, decoded.Date)
		assert.Equal(t, original.Length, decoded.Length)
		assert.Len(t, decoded.Chunks, len(original.Chunks))

		// Verify chunks
		for i, originalChunk := range original.Chunks {
			decodedChunk := decoded.Chunks[i]
			assert.Equal(t, originalChunk.Offset(), decodedChunk.Offset())
			assert.Equal(t, originalChunk.BitmapSize(), decodedChunk.BitmapSize())
			assert.Equal(t, originalChunk.LogSize(), decodedChunk.LogSize())
		}
	})

	t.Run("DecodeInvalidJSON", func(t *testing.T) {
		invalidJSON := []byte(`{"invalid": json}`)
		_, err := DecodeMetadata(invalidJSON)
		assert.Error(t, err)
	})
}
