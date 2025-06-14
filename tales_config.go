package tales

import (
	"context"
	"fmt"
	"time"

	"github.com/kelindar/tales/internal/s3"
)

// Option configures the Service. All options are optional and may be provided
// to New when constructing a logger.
type Option func(*config)

// WithChunkInterval sets the interval at which in-memory chunks are flushed.
func WithChunkInterval(d time.Duration) Option {
	return func(c *config) { c.ChunkInterval = d }
}

// WithBufferSize sets the maximum number of entries kept in memory.
func WithBufferSize(size int) Option {
	return func(c *config) { c.BufferSize = size }
}

// WithPrefix sets the S3 key prefix to use when storing objects.
func WithPrefix(prefix string) Option {
	return func(c *config) { c.S3Config.Prefix = prefix }
}

// WithConcurrency sets the (unused) concurrency parameter on the S3 config.
func WithConcurrency(n int) Option {
	return func(c *config) { c.S3Config.Concurrency = n }
}

// WithRetries sets the (unused) retries parameter on the S3 config.
func WithRetries(n int) Option {
	return func(c *config) { c.S3Config.Retries = n }
}

// WithClient allows overriding the S3 client creation function.
func WithClient(fn func(context.Context, s3.Config) (s3.Client, error)) Option {
	return func(c *config) { c.NewClient = fn }
}

// config holds all configuration for the logger. It is kept private and
// manipulated through Option helpers.
type config struct {
	S3Config      s3.Config
	ChunkInterval time.Duration
	BufferSize    int
	NewClient     func(context.Context, s3.Config) (s3.Client, error)
}

// setDefaults applies default values to the configuration.
func (c *config) setDefaults() {
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
func (c *config) validate() error {
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
