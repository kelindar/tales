package threads

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/kelindar/threads/internal/s3"
)

// Option configures optional parameters for the logger service.
type Option func(*Config)

// S3Config holds S3 configuration parameters used by the service.
type S3Config struct {
	Bucket      string // S3 bucket name
	Region      string // AWS region
	Prefix      string // Key prefix
	Concurrency int    // Max concurrent S3 requests (default: 10)
	Retries     int    // Retry attempts for failed requests (default: 3)
}

// toInternal converts the public S3Config to the internal representation.
func (c S3Config) toInternal() s3.Config {
	return s3.Config{
		Bucket:      c.Bucket,
		Region:      c.Region,
		Prefix:      c.Prefix,
		Concurrency: c.Concurrency,
		Retries:     c.Retries,
	}
}

// Config represents the configuration for the threads logger.
type Config struct {
	S3Config      S3Config      // S3 configuration (required)
	ChunkInterval time.Duration // Chunk completion interval (default: 5 minutes)
	BufferSize    int           // Memory buffer size (default: 1000 entries)
	NewS3Client   func(context.Context, S3Config) (s3.Client, error)
}

// WithChunkInterval sets the chunk interval.
func WithChunkInterval(d time.Duration) Option { return func(c *Config) { c.ChunkInterval = d } }

// WithBufferSize sets the memory buffer size.
func WithBufferSize(size int) Option { return func(c *Config) { c.BufferSize = size } }

// WithS3Client overrides the S3 client creation function.
func WithS3Client(fn func(context.Context, S3Config) (s3.Client, error)) Option {
	return func(c *Config) { c.NewS3Client = fn }
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
	if c.S3Config.Region == "" {
		if v := os.Getenv("AWS_REGION"); v != "" {
			c.S3Config.Region = v
		} else if v := os.Getenv("AWS_DEFAULT_REGION"); v != "" {
			c.S3Config.Region = v
		}
	}
	if c.S3Config.Bucket == "" {
		if v := os.Getenv("S3_BUCKET"); v != "" {
			c.S3Config.Bucket = v
		}
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
