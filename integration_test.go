package threads

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kelindar/threads/internal/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestThreadsIntegration(t *testing.T) {
	// Start mock S3 server
	mockS3 := s3.NewMockS3Server()
	defer mockS3.Close()

	// Create logger with mock S3
	logger, err := createLoggerWithMockS3(mockS3)
	require.NoError(t, err)
	defer logger.Close()

	// Test basic logging
	t.Run("BasicLogging", func(t *testing.T) {
		err := logger.Log("Hello, world!", []uint32{12345, 67890})
		assert.NoError(t, err)

		err = logger.Log("Second message", []uint32{12345})
		assert.NoError(t, err)

		err = logger.Log("Third message", []uint32{67890, 11111})
		assert.NoError(t, err)
	})

	// Test buffer queries (recent data)
	t.Run("BufferQueries", func(t *testing.T) {
		from := time.Now().Add(-1 * time.Minute)
		to := time.Now().Add(1 * time.Minute)

		// Query for actor 12345
		var messages []string
		for timestamp, text := range logger.Query(12345, from, to) {
			t.Logf("Found message at %s: %s", timestamp.Format(time.RFC3339), text)
			messages = append(messages, text)
		}

		// Should find 2 messages for actor 12345
		assert.Len(t, messages, 2)
		assert.Contains(t, messages, "Hello, world!")
		assert.Contains(t, messages, "Second message")
	})

	// Test forced flush to S3
	t.Run("FlushToS3", func(t *testing.T) {
		// Force a flush by calling the internal method
		err := logger.flushBuffer()
		assert.NoError(t, err)

		// Check that files were created in mock S3
		objects := mockS3.ListObjects()
		t.Logf("Objects in mock S3: %v", objects)

		// Should have created log, bitmap, and index files
		var hasLog, hasBitmap, hasIndex bool
		for _, obj := range objects {
			if strings.Contains(obj, "threads.log") {
				hasLog = true
			}
			if strings.Contains(obj, "threads.rbm") {
				hasBitmap = true
			}
			if strings.Contains(obj, "threads.idx") {
				hasIndex = true
			}
		}

		assert.True(t, hasLog, "Should have created log file")
		assert.True(t, hasBitmap, "Should have created bitmap file")
		assert.True(t, hasIndex, "Should have created index file")
	})

	// Test historical queries (S3 data)
	t.Run("HistoricalQueries", func(t *testing.T) {
		// Add more data after flush
		err := logger.Log("Post-flush message", []uint32{12345})
		assert.NoError(t, err)

		// Query a wide time range that should include both buffer and S3 data
		from := time.Now().Add(-1 * time.Hour)
		to := time.Now().Add(1 * time.Hour)

		var messages []string
		for timestamp, text := range logger.Query(12345, from, to) {
			t.Logf("Historical query found: %s at %s", text, timestamp.Format(time.RFC3339))
			messages = append(messages, text)
		}

		// Should find messages from both buffer and S3
		assert.GreaterOrEqual(t, len(messages), 1)
	})
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
		entry := &LogEntry{
			SequenceID: 12345,
			Text:       "Test message",
			Actors:     []uint32{100, 200, 300},
		}

		// Encode and decode
		encoded, err := encodeLogEntry(entry)
		require.NoError(t, err)

		decoded, err := decodeLogEntry(encoded)
		require.NoError(t, err)

		// Should match original
		assert.Equal(t, entry.SequenceID, decoded.SequenceID)
		assert.Equal(t, entry.Text, decoded.Text)
		assert.Equal(t, entry.Actors, decoded.Actors)
	})

	t.Run("TailMetadataEncoding", func(t *testing.T) {
		metadata := &TailMetadata{
			Magic:      [4]byte{'T', 'A', 'I', 'L'},
			Version:    1,
			DayStart:   time.Now().UnixNano(),
			ChunkCount: 2,
			Chunks: []ChunkEntry{
				{Offset: 0, CompressedSize: 100, UncompressedSize: 200},
				{Offset: 100, CompressedSize: 150, UncompressedSize: 300},
			},
			TailSize: 0, // Will be set during encoding
		}

		// Encode
		encoded, err := encodeTailMetadata(metadata)
		require.NoError(t, err)

		// The encoded data should contain the magic bytes
		assert.Contains(t, string(encoded[:4]), "TAIL")
	})
}

// createLoggerWithMockS3 creates a logger configured to use the mock S3 server
func createLoggerWithMockS3(mockS3 *s3.MockS3Server) (*Logger, error) {
	// Create S3 config for mock server
	s3Config := s3.CreateConfigForMock(mockS3, "test-bucket", "test-prefix")

	// Create mock S3 client
	customS3Client, err := s3.NewMockClient(context.Background(), mockS3, s3Config)
	if err != nil {
		return nil, err
	}

	// Create logger config
	config := Config{
		S3Config:      s3Config,
		ChunkInterval: 5 * time.Minute,
		BufferSize:    1000,
	}

	// Initialize compression
	if err := initCompression(); err != nil {
		return nil, err
	}

	// Create context for background operations
	ctx, cancel := context.WithCancel(context.Background())

	// Create buffer
	buffer := NewBuffer(config.BufferSize)

	// Initialize day start (ensure UTC)
	dayStart := getDayStart(time.Now().UTC())

	logger := &Logger{
		config:   config,
		s3Client: customS3Client,
		buffer:   buffer,
		dayStart: dayStart,
		ctx:      ctx,
		cancel:   cancel,
	}

	// Start background flush timer
	logger.startFlushTimer()

	return logger, nil
}
