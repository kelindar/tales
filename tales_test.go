package threads

import (
	"context"
	"testing"
	"time"

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
	logger.Log("hello world 1", 1)
	logger.Log("hello world 2", 2)
	logger.flush()
	logger.Log("hello world 3", 1, 3)
	logger.flush()
	logger.Log("hello world 4", 1, 2)

	from := time.Now().Add(-1 * time.Hour)
	to := time.Now().Add(1 * time.Hour)

	// Query for actor 1
	var results1 []string
	for timestamp, msg := range logger.Query(1, from, to) {
		_ = timestamp // ignore timestamp for this test
		results1 = append(results1, msg)
	}

	assert.Equal(t, []string{
		"hello world 1",
		"hello world 3",
		"hello world 4",
	}, results1)

	// Query for actor 2
	var results2 []string
	for _, msg := range logger.Query(2, from, to) {
		results2 = append(results2, msg)
	}
	assert.Equal(t, []string{
		"hello world 2",
		"hello world 4",
	}, results2)

	// Query for actor 3
	var results3 []string
	for _, msg := range logger.Query(3, from, to) {
		results3 = append(results3, msg)
	}
	assert.Equal(t, []string{
		"hello world 3",
	}, results3)

	// Query for actor 4 (no logs)
	var results4 []string
	for _, msg := range logger.Query(4, from, to) {
		results4 = append(results4, msg)
	}
	assert.Empty(t, results4)
}
