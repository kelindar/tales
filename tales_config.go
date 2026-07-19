// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package tales

import (
	"fmt"
	"hash/fnv"
	"time"

	"github.com/kelindar/s3/aws"
	"github.com/kelindar/tales/internal/machine"
	"github.com/kelindar/tales/internal/s3"
)

// Option configures the Service. All options are optional and may be provided
// to New when constructing a logger.
type Option func(*config)

// WithInterval sets the interval at which in-memory chunks are flushed.
func WithInterval(d time.Duration) Option {
	return func(c *config) { c.ChunkInterval = d }
}

// WithBuffer sets the maximum number of entries kept in memory.
func WithBuffer(size int) Option {
	return func(c *config) { c.BufferSize = size }
}

// WithWriterID hashes a stable name into the service's 16-character writer ID.
func WithWriterID(id string) Option {
	return func(c *config) {
		hash := fnv.New64a()
		_, _ = hash.Write([]byte(id))
		c.WriterID = fmt.Sprintf("%016x", hash.Sum64())
	}
}

// WithPrefix sets the S3 key prefix to use when storing objects.
func WithPrefix(prefix string) Option {
	return func(c *config) { c.S3.Prefix = prefix }
}

// WithClient allows overriding the S3 client creation function.
func WithClient(fn func(s3.Config) (s3.Client, error)) Option {
	return func(c *config) { c.NewClient = fn }
}

// WithKey sets the signing key to use for the S3 client.
func WithKey(key *aws.SigningKey) Option {
	return func(c *config) { c.S3.Key = key }
}

// WithBackblaze sets the S3 service to Backblaze B2.
func WithBackblaze() Option {
	return func(c *config) { c.S3.Service = "b2" }
}

// config holds all configuration for the logger. It is kept private and
// manipulated through Option helpers.
type config struct {
	S3            s3.Config
	ChunkInterval time.Duration
	NewClient     func(s3.Config) (s3.Client, error)
	BufferSize    int
	WriterID      string
	now           func() time.Time
	composeParts  int
}

// setDefaults applies default values to the configuration.
func (c *config) setDefaults() {
	if c.ChunkInterval == 0 {
		c.ChunkInterval = 5 * time.Minute
	}
	if c.BufferSize == 0 {
		c.BufferSize = 1 << 20
	}
	if c.S3.Service == "" {
		c.S3.Service = "s3"
	}
	if c.WriterID == "" {
		c.WriterID = fmt.Sprintf("%016x", machine.ID())
	}
	if c.now == nil {
		c.now = time.Now
	}
	if c.composeParts == 0 {
		c.composeParts = 10_000
	}
}

// validate checks if the configuration is valid.
func (c *config) validate() error {
	switch {
	case c.S3.Bucket == "":
		return fmt.Errorf("S3 bucket is required")
	case c.S3.Region == "":
		return fmt.Errorf("S3 region is required")
	case c.ChunkInterval < time.Minute:
		return fmt.Errorf("chunk interval must be at least 1 minute")
	case c.BufferSize < 1:
		return fmt.Errorf("buffer size must be at least 1")
	case len(c.WriterID) != 16:
		return fmt.Errorf("invalid writer ID")
	}
	return nil
}
