package threads

import (
	"context"
	"testing"
	"time"

	"github.com/kelindar/threads/internal/codec"
	"github.com/kelindar/threads/internal/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration(t *testing.T) {
	// Create a mock S3 server
	mockS3 := s3.NewMockS3Server()
	defer mockS3.Close()

	// Create S3 config for mock server
	s3Config := s3.CreateConfigForMock(mockS3, "test-bucket", "test-prefix")

	// Create logger config
	config := Config{
		ChunkInterval: 1 * time.Minute,
		BufferSize:    1024 * 1024, // 1MB
		S3Config:      s3Config,
		NewS3Client: func(ctx context.Context, config s3.Config) (s3.Client, error) {
			return s3.NewMockClient(ctx, mockS3, config)
		},
	}

	logger, err := New(config)
	require.NoError(t, err)
	defer logger.Close()

	// Log some messages
	logger.Log("hello world 1", []uint32{1})
	logger.Log("hello world 2", []uint32{2})
	logger.Log("hello world 3", []uint32{1, 3})

	// Wait for flush to happen
	done := make(chan struct{})
	logger.commands <- flushCmd{done: done}
	<-done // wait for flush to complete

	from := time.Now().Add(-1 * time.Hour)
	to := time.Now().Add(1 * time.Hour)

	// Query for actor 1
	var results1 []string
	for _, msg := range logger.Query(1, from, to) {
		results1 = append(results1, msg)
	}
	assert.Contains(t, results1, "hello world 1")
	assert.Contains(t, results1, "hello world 3")

	// Query for actor 2
	var results2 []string
	for _, msg := range logger.Query(2, from, to) {
		results2 = append(results2, msg)
	}
	assert.Contains(t, results2, "hello world 2")

	// Query for actor 3
	var results3 []string
	for _, msg := range logger.Query(3, from, to) {
		results3 = append(results3, msg)
	}
	assert.Contains(t, results3, "hello world 3")

	// Query for actor 4 (no logs)
	var results4 []string
	for _, msg := range logger.Query(4, from, to) {
		results4 = append(results4, msg)
	}
	assert.Empty(t, results4)
}

func TestSequenceIDGeneration(t *testing.T) {
	dayStart := getDayStart(time.Now())
	var counter uint32

	t.Run("SequentialIDs", func(t *testing.T) {
		now := time.Now()

		id1 := generateSequenceID(dayStart, now, &counter)
		id2 := generateSequenceID(dayStart, now, &counter)
		id3 := generateSequenceID(dayStart, now, &counter)

		// IDs should be sequential
		assert.Greater(t, id2, id1)
		assert.Greater(t, id3, id2)

		// Should be able to reconstruct timestamps
		ts1 := reconstructTimestamp(id1, dayStart)
		ts2 := reconstructTimestamp(id2, dayStart)
		ts3 := reconstructTimestamp(id3, dayStart)

		// Timestamps should be close to the original time (within same minute)
		assert.WithinDuration(t, now, ts1, time.Minute)
		assert.WithinDuration(t, now, ts2, time.Minute)
		assert.WithinDuration(t, now, ts3, time.Minute)
	})

	t.Run("DifferentMinutes", func(t *testing.T) {
		now1 := time.Now()
		now2 := now1.Add(2 * time.Minute)

		id1 := generateSequenceID(dayStart, now1, &counter)
		id2 := generateSequenceID(dayStart, now2, &counter)

		// IDs from different minutes should be different
		assert.NotEqual(t, id1>>20, id2>>20) // Different minute parts
	})
}

func TestFileFormats(t *testing.T) {
	t.Run("LogEntryEncoding", func(t *testing.T) {
		// Create entry using codec
		entry, err := codec.NewLogEntry(12345, "Test message", []uint32{100, 200, 300})
		require.NoError(t, err)

		// Test accessors
		assert.Equal(t, uint32(12345), entry.ID())
		assert.Equal(t, "Test message", entry.Text())
		assert.Equal(t, []uint32{100, 200, 300}, entry.Actors())
	})

	t.Run("TailMetadataEncoding", func(t *testing.T) {
		chunks := []codec.ChunkEntry{
			codec.NewChunkEntry(0, 100, 200),
			codec.NewChunkEntry(100, 150, 300),
		}

		metadata := &TailMetadata{
			Magic:      [4]byte{'T', 'A', 'I', 'L'},
			Version:    1,
			DayStart:   time.Now().UnixNano(),
			ChunkCount: 2,
			Chunks:     chunks,
			TailSize:   0, // Will be set during encoding
		}

		// Encode
		encoded, err := encodeTailMetadata(metadata)
		require.NoError(t, err)

		// The encoded data should contain the magic bytes
		assert.Contains(t, string(encoded[:4]), "TAIL")

		// Test chunk accessors
		for i, chunk := range chunks {
			assert.Equal(t, uint64(i*100), chunk.Offset())
			assert.Equal(t, uint32(100+i*50), chunk.CompressedSize())
			assert.Equal(t, uint32(200+i*100), chunk.UncompressedSize())
		}
	})
}

