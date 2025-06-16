package codec

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogEntry(t *testing.T) {
	t.Run("CreateAndAccess", func(t *testing.T) {
		// Create a log entry
		entry, err := NewLogEntry(12345, "Hello, world!", []uint32{100, 200, 300})
		require.NoError(t, err)

		// Test accessors
		assert.Equal(t, uint32(12345), entry.ID())
		assert.Equal(t, "Hello, world!", entry.Text())
		assert.Equal(t, []uint32{100, 200, 300}, entry.Actors())
	})

	t.Run("EmptyActors", func(t *testing.T) {
		entry, err := NewLogEntry(999, "No actors", []uint32{})
		require.NoError(t, err)

		assert.Equal(t, uint32(999), entry.ID())
		assert.Equal(t, "No actors", entry.Text())
		assert.Empty(t, entry.Actors())
	})

	t.Run("UnicodeText", func(t *testing.T) {
		entry, err := NewLogEntry(555, "Hello 世界! 🌍", []uint32{42})
		require.NoError(t, err)

		assert.Equal(t, uint32(555), entry.ID())
		assert.Equal(t, "Hello 世界! 🌍", entry.Text())
		assert.Equal(t, []uint32{42}, entry.Actors())
	})
}

func TestIndexEntry(t *testing.T) {
	t.Run("CreateAndAccess", func(t *testing.T) {
		entry := NewIndexEntry(1234567890, 9876543210, 1024)

		assert.Equal(t, uint32(1234567890), uint32(entry[0]))
		assert.Equal(t, uint64(9876543210), uint64(entry[1]))
		assert.Equal(t, uint32(1024), uint32(entry[2]))
	})

	t.Run("ZeroValues", func(t *testing.T) {
		entry := NewIndexEntry(0, 0, 0)

		assert.Equal(t, uint32(0), uint32(entry[0]))
		assert.Equal(t, uint64(0), uint64(entry[1]))
		assert.Equal(t, uint32(0), uint32(entry[2]))
	})
}

func TestChunkEntry(t *testing.T) {
	t.Run("CreateAndAccess", func(t *testing.T) {
		entry := NewChunkEntry(1234567890123, 2048, 4096, nil)

		assert.Equal(t, uint64(1234567890123), uint64(entry.Location[0]))
		assert.Equal(t, uint32(2048), uint32(entry.Location[1]))
		assert.Equal(t, uint32(4096), uint32(entry.Location[2]))

		// Test calculated offsets
		assert.Equal(t, uint32(0), entry.BitmapOffset())
		assert.Equal(t, uint32(2048), entry.LogOffset())
		assert.Equal(t, uint32(6144), entry.TotalSize())
	})

	t.Run("MaxValues", func(t *testing.T) {
		entry := NewChunkEntry(^uint64(0), ^uint32(0), ^uint32(0), nil)

		assert.Equal(t, ^uint64(0), uint64(entry.Location[0]))
		assert.Equal(t, ^uint32(0), uint32(entry.Location[1]))
		assert.Equal(t, ^uint32(0), uint32(entry.Location[2]))
	})
}

func TestEdgeCases(t *testing.T) {
	t.Run("ShortLogEntry", func(t *testing.T) {
		// Create a short byte slice that's too small to be a valid entry
		shortEntry := LogEntry([]byte{1, 2, 3})

		assert.Equal(t, uint32(0), shortEntry.ID())
		assert.Equal(t, "", shortEntry.Text())
		assert.Nil(t, shortEntry.Actors())
	})

}

func TestCompression(t *testing.T) {
	t.Run("CompressAndDecompress", func(t *testing.T) {
		codec, err := NewCodec()
		require.NoError(t, err)
		defer codec.Close()

		originalData := []byte("Hello, world! This is a test string for compression.")

		// Compress
		compressed, err := codec.Compress(originalData)
		require.NoError(t, err)
		assert.NotEmpty(t, compressed)

		// Decompress
		decompressed, err := codec.Decompress(compressed)
		require.NoError(t, err)
		assert.Equal(t, originalData, decompressed)
	})

	t.Run("EmptyData", func(t *testing.T) {
		codec, err := NewCodec()
		require.NoError(t, err)
		defer codec.Close()

		// Compress empty data
		compressed, err := codec.Compress([]byte{})
		require.NoError(t, err)

		// Decompress empty data
		decompressed, err := codec.Decompress(compressed)
		require.NoError(t, err)
		assert.Empty(t, decompressed)
	})

	t.Run("LargeData", func(t *testing.T) {
		codec, err := NewCodec()
		require.NoError(t, err)
		defer codec.Close()

		// Create large data
		largeData := make([]byte, 10000)
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		// Compress
		compressed, err := codec.Compress(largeData)
		require.NoError(t, err)
		assert.Less(t, len(compressed), len(largeData)) // Should be compressed

		// Decompress
		decompressed, err := codec.Decompress(compressed)
		require.NoError(t, err)
		assert.Equal(t, largeData, decompressed)
	})
}
