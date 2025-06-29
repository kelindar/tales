// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"

	s3lib "github.com/kelindar/s3"
	"github.com/kelindar/s3/aws"
)

// Config represents the S3-specific configuration.
type Config struct {
	Bucket  string          // S3 bucket name (required)
	Region  string          // AWS region (required for mock)
	Prefix  string          // S3 key prefix (optional)
	Service string          // S3 service (optional)
	Key     *aws.SigningKey // Optional signing key
}

// Client interface abstracts S3 operations for easier testing and mocking
// This matches the previous AWS SDK based implementation.
type Client interface {
	Upload(ctx context.Context, key string, data []byte) error
	UploadReader(ctx context.Context, key string, reader io.ReaderAt, size int64) error
	Download(ctx context.Context, key string) ([]byte, error)
	DownloadRange(ctx context.Context, key string, start, end int64) ([]byte, error)
	ObjectExists(ctx context.Context, key string) (bool, error)
	ObjectSize(ctx context.Context, key string) (int64, error)
}

// ErrS3Operation represents an S3 operation error.
type ErrS3Operation struct {
	Operation string
	Err       error
}

func (e ErrS3Operation) Error() string {
	return fmt.Sprintf("S3 operation failed (%s): %v", e.Operation, e.Err)
}

func (e ErrS3Operation) Unwrap() error { return e.Err }

// S3Client wraps the kelindar/s3 bucket with convenience methods.
type S3Client struct {
	bucket     *s3lib.Bucket
	key        *aws.SigningKey
	bucketName string
	prefix     string
}

// NewClient creates a new client using ambient credentials.
func NewClient(cfg Config) (client Client, err error) {
	var key *aws.SigningKey
	switch {
	case cfg.Key != nil:
		key = cfg.Key
	case cfg.Bucket != "":
		key, err = aws.AmbientKey(cfg.Service, cfg.Region, s3lib.DeriveForBucket(cfg.Bucket))
	default:
		err = fmt.Errorf("region or key is required")
	}

	if err != nil {
		return nil, ErrS3Operation{Operation: "credentials", Err: err}
	}

	bucket := s3lib.NewBucket(key, cfg.Bucket)
	bucket.Lazy = true
	return &S3Client{bucket: bucket, key: key, bucketName: cfg.Bucket, prefix: cfg.Prefix}, nil
}

// buildKey constructs an S3 key with the configured prefix.
func (c *S3Client) buildKey(key string) string {
	if c.prefix == "" {
		return key
	}
	return strings.TrimSuffix(c.prefix, "/") + "/" + key
}

// Upload overwrites an S3 object with the given data.
func (c *S3Client) Upload(ctx context.Context, key string, data []byte) error {
	fullKey := c.buildKey(key)
	if _, err := c.bucket.Write(ctx, fullKey, data); err != nil {
		return ErrS3Operation{Operation: "write", Err: err}
	}
	return nil
}

// UploadReader uploads data from a ReaderAt to S3.
func (c *S3Client) UploadReader(ctx context.Context, key string, reader io.ReaderAt, size int64) error {
	fullKey := c.buildKey(key)
	if err := c.bucket.WriteFrom(ctx, fullKey, reader, size); err != nil {
		return ErrS3Operation{Operation: "write", Err: err}
	}
	return nil
}

// Download downloads an object.
func (c *S3Client) Download(ctx context.Context, key string) ([]byte, error) {
	fullKey := c.buildKey(key)
	f, err := c.bucket.Open(fullKey)
	if err != nil {
		return nil, ErrS3Operation{Operation: "download", Err: err}
	}
	defer f.Close()
	return io.ReadAll(f)
}

// DownloadRange downloads a byte range from S3.
func (c *S3Client) DownloadRange(ctx context.Context, key string, start, end int64) ([]byte, error) {
	fullKey := c.buildKey(key)
	reader, err := c.bucket.OpenRange(fullKey, "", start, end-start+1)
	if err != nil {
		return nil, ErrS3Operation{Operation: "range", Err: err}
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

// ObjectExists checks if an object exists.
func (c *S3Client) ObjectExists(ctx context.Context, key string) (bool, error) {
	fullKey := c.buildKey(key)
	_, err := c.bucket.Open(fullKey)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, ErrS3Operation{Operation: "head", Err: err}
	}
	return true, nil
}

// ObjectSize returns the size of an object.
func (c *S3Client) ObjectSize(ctx context.Context, key string) (int64, error) {
	fullKey := c.buildKey(key)
	f, err := c.bucket.Open(fullKey)
	if err != nil {
		return 0, ErrS3Operation{Operation: "head", Err: err}
	}
	defer f.Close()
	if s3f, ok := f.(*s3lib.File); ok {
		return s3f.Reader.Size, nil
	}
	return 0, ErrS3Operation{Operation: "head", Err: fmt.Errorf("not a file")}
}

// IsNoSuchKey checks if the error is a missing object error.
func IsNoSuchKey(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, fs.ErrNotExist) {
		return true
	}
	return strings.Contains(err.Error(), "NoSuchKey") || strings.Contains(err.Error(), "404")
}
