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
		entry := NewIndexEntry(1234567890, 42, 9876543210, 1024, 2048)

		assert.Equal(t, uint32(1234567890), entry.Time())
		assert.Equal(t, uint32(42), entry.Actor())
		assert.Equal(t, uint64(9876543210), entry.Offset())
		assert.Equal(t, uint32(1024), entry.CompressedSize())
		assert.Equal(t, uint32(2048), entry.UncompressedSize())
	})

	t.Run("ZeroValues", func(t *testing.T) {
		entry := NewIndexEntry(0, 0, 0, 0, 0)

		assert.Equal(t, uint32(0), entry.Time())
		assert.Equal(t, uint32(0), entry.Actor())
		assert.Equal(t, uint64(0), entry.Offset())
		assert.Equal(t, uint32(0), entry.CompressedSize())
		assert.Equal(t, uint32(0), entry.UncompressedSize())
	})
}

func TestChunkEntry(t *testing.T) {
	t.Run("CreateAndAccess", func(t *testing.T) {
		entry := NewChunkEntry(1234567890123, 1024, 2048, 4096)

		assert.Equal(t, uint64(1234567890123), entry.Offset())
		assert.Equal(t, uint32(1024), entry.IndexSize())
		assert.Equal(t, uint32(2048), entry.BitmapSize())
		assert.Equal(t, uint32(4096), entry.LogSize())

		// Test calculated offsets
		assert.Equal(t, uint32(1024), entry.BitmapOffset())
		assert.Equal(t, uint32(3072), entry.LogOffset()) // 1024 + 2048
		assert.Equal(t, uint32(7168), entry.TotalSize()) // 1024 + 2048 + 4096
	})

	t.Run("MaxValues", func(t *testing.T) {
		entry := NewChunkEntry(^uint64(0), ^uint32(0), ^uint32(0), ^uint32(0))

		assert.Equal(t, ^uint64(0), entry.Offset())
		assert.Equal(t, ^uint32(0), entry.IndexSize())
		assert.Equal(t, ^uint32(0), entry.BitmapSize())
		assert.Equal(t, ^uint32(0), entry.LogSize())
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

	t.Run("ShortIndexEntry", func(t *testing.T) {
		shortEntry := IndexEntry([]byte{1, 2, 3})

		assert.Equal(t, uint32(0), shortEntry.Time())
		assert.Equal(t, uint32(0), shortEntry.Actor())
		assert.Equal(t, uint64(0), shortEntry.Offset())
		assert.Equal(t, uint32(0), shortEntry.CompressedSize())
		assert.Equal(t, uint32(0), shortEntry.UncompressedSize())
	})

	t.Run("ShortChunkEntry", func(t *testing.T) {
		shortEntry := ChunkEntry([]byte{1, 2, 3})

		assert.Equal(t, uint64(0), shortEntry.Offset())
		assert.Equal(t, uint32(0), shortEntry.IndexSize())
		assert.Equal(t, uint32(0), shortEntry.BitmapSize())
		assert.Equal(t, uint32(0), shortEntry.LogSize())
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

func TestConstants(t *testing.T) {
	t.Run("Sizes", func(t *testing.T) {
		assert.Equal(t, 20, ChunkEntrySize)
		assert.Equal(t, 24, IndexEntrySize)
	})

}
