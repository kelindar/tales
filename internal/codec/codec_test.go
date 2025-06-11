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
		entry := NewIndexEntry(1234567890, 42, 9876543210, 1024)

		assert.Equal(t, uint32(1234567890), entry.Time())
		assert.Equal(t, uint32(42), entry.Actor())
		assert.Equal(t, uint64(9876543210), entry.Offset())
		assert.Equal(t, uint32(1024), entry.Size())
	})

	t.Run("ZeroValues", func(t *testing.T) {
		entry := NewIndexEntry(0, 0, 0, 0)

		assert.Equal(t, uint32(0), entry.Time())
		assert.Equal(t, uint32(0), entry.Actor())
		assert.Equal(t, uint64(0), entry.Offset())
		assert.Equal(t, uint32(0), entry.Size())
	})
}

func TestChunkEntry(t *testing.T) {
	t.Run("CreateAndAccess", func(t *testing.T) {
		entry := NewChunkEntry(1234567890123, 2048, 4096)

		assert.Equal(t, uint64(1234567890123), entry.Offset())
		assert.Equal(t, uint32(2048), entry.CompressedSize())
		assert.Equal(t, uint32(4096), entry.UncompressedSize())
	})

	t.Run("MaxValues", func(t *testing.T) {
		entry := NewChunkEntry(^uint64(0), ^uint32(0), ^uint32(0))

		assert.Equal(t, ^uint64(0), entry.Offset())
		assert.Equal(t, ^uint32(0), entry.CompressedSize())
		assert.Equal(t, ^uint32(0), entry.UncompressedSize())
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
		assert.Equal(t, uint32(0), shortEntry.Size())
	})

	t.Run("ShortChunkEntry", func(t *testing.T) {
		shortEntry := ChunkEntry([]byte{1, 2, 3})

		assert.Equal(t, uint64(0), shortEntry.Offset())
		assert.Equal(t, uint32(0), shortEntry.CompressedSize())
		assert.Equal(t, uint32(0), shortEntry.UncompressedSize())
	})
}
