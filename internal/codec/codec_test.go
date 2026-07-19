// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package codec

import (
	"encoding/binary"
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

		// Collect actors from iterator
		var actors []uint32
		for actor := range entry.Actors() {
			actors = append(actors, actor)
		}
		assert.Equal(t, []uint32{100, 200, 300}, actors)
	})

	t.Run("EmptyActors", func(t *testing.T) {
		entry, err := NewLogEntry(999, "No actors", []uint32{})
		require.NoError(t, err)

		assert.Equal(t, uint32(999), entry.ID())
		assert.Equal(t, "No actors", entry.Text())

		// Check that no actors are yielded
		count := 0
		for range entry.Actors() {
			count++
		}
		assert.Equal(t, 0, count)
	})

	t.Run("UnicodeText", func(t *testing.T) {
		entry, err := NewLogEntry(555, "Hello 世界! 🌍", []uint32{42})
		require.NoError(t, err)

		assert.Equal(t, uint32(555), entry.ID())
		assert.Equal(t, "Hello 世界! 🌍", entry.Text())

		// Collect actors from iterator
		var actors []uint32
		for actor := range entry.Actors() {
			actors = append(actors, actor)
		}
		assert.Equal(t, []uint32{42}, actors)
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
		entry := NewChunkEntry(1234567890123, 2048, 4096, 1000, 2000, nil)

		assert.Equal(t, uint64(1234567890123), entry.Offset())
		assert.Equal(t, uint32(2048), entry.BitmapSize())
		assert.Equal(t, uint32(4096), entry.DataSize())

		// Test calculated offsets
		assert.Equal(t, uint32(0), entry.BitmapAt())
		assert.Equal(t, uint32(2048), entry.DataAt())
		assert.Equal(t, uint32(6144), entry.Size())
	})

	t.Run("MaxValues", func(t *testing.T) {
		entry := NewChunkEntry(^uint64(0), ^uint32(0), ^uint32(0), 1000, 2000, nil)

		assert.Equal(t, ^uint64(0), entry.Offset())
		assert.Equal(t, ^uint32(0), entry.BitmapSize())
		assert.Equal(t, ^uint32(0), entry.DataSize())
	})
}

func TestEdgeCases(t *testing.T) {
	t.Run("ShortLogEntry", func(t *testing.T) {
		// Create a short byte slice that's too small to be a valid entry
		shortEntry := LogEntry([]byte{1, 2, 3})

		assert.Equal(t, uint32(0), shortEntry.ID())
		assert.Equal(t, "", shortEntry.Text())

		// Check that no actors are yielded for invalid entry
		count := 0
		for range shortEntry.Actors() {
			count++
		}
		assert.Equal(t, 0, count)
	})

	t.Run("TextInvalidBounds", func(t *testing.T) {
		badSize := make([]byte, 10)
		binary.LittleEndian.PutUint16(badSize[4:6], 100) // size > len
		binary.LittleEndian.PutUint16(badSize[6:8], 8)
		assert.Empty(t, LogEntry(badSize).Text())

		emptyText := make([]byte, 16)
		binary.LittleEndian.PutUint16(emptyText[4:6], 16)
		binary.LittleEndian.PutUint16(emptyText[6:8], 16) // midpoint == size
		assert.Empty(t, LogEntry(emptyText).Text())
	})

	t.Run("ActorsInvalidBounds", func(t *testing.T) {
		badCutoff := make([]byte, 12)
		binary.LittleEndian.PutUint16(badCutoff[4:6], 12)
		binary.LittleEndian.PutUint16(badCutoff[6:8], 100) // cutoff > len
		count := 0
		for range LogEntry(badCutoff).Actors() {
			count++
		}
		assert.Equal(t, 0, count)

		misaligned := make([]byte, 13)
		binary.LittleEndian.PutUint16(misaligned[4:6], 13)
		binary.LittleEndian.PutUint16(misaligned[6:8], 9) // (9-8)%4 != 0
		for range LogEntry(misaligned).Actors() {
			count++
		}
		assert.Equal(t, 0, count)
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
