package threads

import (
	"context"
	"testing"
	"time"

	"github.com/kelindar/threads/internal/s3"
	"github.com/stretchr/testify/assert"
)

// Test that defaults are applied when options are not provided
func TestOptionsDefaults(t *testing.T) {
	cfg := S3Config{Bucket: "b", Region: "r"}
	svc, err := New(cfg)
	assert.NoError(t, err)
	defer svc.Close()

	assert.Equal(t, 5*time.Minute, svc.config.ChunkInterval)
	assert.Equal(t, 1000, svc.config.BufferSize)
	assert.Equal(t, 10, svc.config.S3Config.Concurrency)
	assert.Equal(t, 3, svc.config.S3Config.Retries)
}

// Test that options override defaults
func TestOptionsOverride(t *testing.T) {
	cfg := S3Config{Bucket: "b", Region: "r"}
	var called bool
	custom := func(ctx context.Context, c S3Config) (s3.Client, error) {
		called = true
		return s3.NewMockClient(ctx, s3.NewMockS3Server(), c.toInternal())
	}
	svc, err := New(
		cfg,
		WithChunkInterval(2*time.Minute),
		WithBufferSize(42),
		WithS3Client(custom),
	)
	assert.NoError(t, err)
	defer svc.Close()

	assert.Equal(t, 2*time.Minute, svc.config.ChunkInterval)
	assert.Equal(t, 42, svc.config.BufferSize)
	assert.True(t, called)
}
