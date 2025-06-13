package threads

import (
	"context"
	"fmt"
	"time"

	"github.com/kelindar/threads/internal/s3"
)

// Config represents the configuration for the threads logger.
type Config struct {
	S3Config      s3.Config     // S3 configuration (required)
	ChunkInterval time.Duration // Chunk completion interval (default: 5 minutes)
	BufferSize    int           // Memory buffer size (default: 1000 entries)
	NewS3Client   func(context.Context, s3.Config) (s3.Client, error)
}

// setDefaults applies default values to the configuration.
func (c *Config) setDefaults() {
	if c.ChunkInterval == 0 {
		c.ChunkInterval = 5 * time.Minute
	}
	if c.BufferSize == 0 {
		c.BufferSize = 1000
	}
	if c.S3Config.Concurrency == 0 {
		c.S3Config.Concurrency = 10
	}
	if c.S3Config.Retries == 0 {
		c.S3Config.Retries = 3
	}
}

// validate checks if the configuration is valid.
func (c *Config) validate() error {
	switch {
	case c.S3Config.Bucket == "":
		return fmt.Errorf("S3 bucket is required")
	case c.S3Config.Region == "":
		return fmt.Errorf("S3 region is required")
	case c.ChunkInterval < time.Minute:
		return fmt.Errorf("chunk interval must be at least 1 minute")
	case c.BufferSize < 1:
		return fmt.Errorf("buffer size must be at least 1")
	}
	return nil
}
