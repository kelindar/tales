package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Config represents the S3-specific configuration.
type Config struct {
	Bucket        string // S3 bucket name (required)
	Region        string // AWS region (required)
	Prefix        string // S3 key prefix (optional)
	MaxConcurrent int    // Max concurrent S3 requests (default: 10)
	RetryAttempts int    // Retry attempts for failed requests (default: 3)
	// AWS credentials are handled via environment variables or IAM roles
}

// Client interface abstracts S3 operations for easier testing and mocking
type Client interface {
	UploadData(ctx context.Context, key string, data []byte) error
	AppendData(ctx context.Context, key string, data []byte) error
	DownloadData(ctx context.Context, key string) ([]byte, error)
	DownloadRange(ctx context.Context, key string, start, end int64) ([]byte, error)
	DownloadTail(ctx context.Context, key string, tailSize int64) ([]byte, error)
	ObjectExists(ctx context.Context, key string) (bool, error)
	GetObjectSize(ctx context.Context, key string) (int64, error)
}

// ErrS3Operation represents an S3 operation error.
type ErrS3Operation struct {
	Operation string
	Err       error
}

func (e ErrS3Operation) Error() string {
	return fmt.Sprintf("S3 operation failed (%s): %v", e.Operation, e.Err)
}

func (e ErrS3Operation) Unwrap() error {
	return e.Err
}

// S3Client wraps the AWS S3 client with retry logic and convenience methods.
type S3Client struct {
	client        *s3.Client
	bucket        string
	prefix        string
	maxConcurrent int
	retryAttempts int
}

// NewClient creates a new S3 client with the given configuration.
func NewClient(ctx context.Context, s3Config Config) (Client, error) {
	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(s3Config.Region))
	if err != nil {
		return nil, ErrS3Operation{Operation: "load config", Err: err}
	}

	client := s3.NewFromConfig(cfg)

	return &S3Client{
		client:        client,
		bucket:        s3Config.Bucket,
		prefix:        s3Config.Prefix,
		maxConcurrent: s3Config.MaxConcurrent,
		retryAttempts: s3Config.RetryAttempts,
	}, nil
}

// buildKey constructs an S3 key with the configured prefix.
func (c *S3Client) buildKey(key string) string {
	if c.prefix == "" {
		return key
	}
	return strings.TrimSuffix(c.prefix, "/") + "/" + key
}

// UploadData uploads data to S3 with retry logic.
func (c *S3Client) UploadData(ctx context.Context, key string, data []byte) error {
	fullKey := c.buildKey(key)

	for attempt := 0; attempt <= c.retryAttempts; attempt++ {
		_, err := c.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(c.bucket),
			Key:    aws.String(fullKey),
			Body:   bytes.NewReader(data),
		})

		if err == nil {
			return nil
		}

		if attempt == c.retryAttempts {
			return ErrS3Operation{Operation: "upload", Err: err}
		}
	}

	return nil
}

// AppendData appends data to an existing S3 object by downloading, concatenating, and re-uploading.
func (c *S3Client) AppendData(ctx context.Context, key string, data []byte) error {
	// Try to download existing data
	existing, err := c.DownloadData(ctx, key)
	if err != nil {
		// If object doesn't exist, create it with the new data
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return c.UploadData(ctx, key, data)
		}
		return err
	}

	// Concatenate existing and new data
	combined := append(existing, data...)

	// Upload the combined data
	return c.UploadData(ctx, key, combined)
}

// DownloadData downloads data from S3 with retry logic.
func (c *S3Client) DownloadData(ctx context.Context, key string) ([]byte, error) {
	fullKey := c.buildKey(key)

	for attempt := 0; attempt <= c.retryAttempts; attempt++ {
		resp, err := c.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(c.bucket),
			Key:    aws.String(fullKey),
		})

		if err == nil {
			defer resp.Body.Close()
			data, readErr := io.ReadAll(resp.Body)
			if readErr != nil {
				return nil, ErrS3Operation{Operation: "read response", Err: readErr}
			}
			return data, nil
		}

		if attempt == c.retryAttempts {
			return nil, ErrS3Operation{Operation: "download", Err: err}
		}
	}

	return nil, nil
}

// DownloadRange downloads a byte range from S3.
func (c *S3Client) DownloadRange(ctx context.Context, key string, start, end int64) ([]byte, error) {
	fullKey := c.buildKey(key)
	rangeHeader := fmt.Sprintf("bytes=%d-%d", start, end)

	for attempt := 0; attempt <= c.retryAttempts; attempt++ {
		resp, err := c.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(c.bucket),
			Key:    aws.String(fullKey),
			Range:  aws.String(rangeHeader),
		})

		if err == nil {
			defer resp.Body.Close()
			data, readErr := io.ReadAll(resp.Body)
			if readErr != nil {
				return nil, ErrS3Operation{Operation: "read range response", Err: readErr}
			}
			return data, nil
		}

		if attempt == c.retryAttempts {
			return nil, ErrS3Operation{Operation: "download range", Err: err}
		}
	}

	return nil, nil
}

// DownloadTail downloads the tail of a file (last N bytes).
func (c *S3Client) DownloadTail(ctx context.Context, key string, tailSize int64) ([]byte, error) {
	fullKey := c.buildKey(key)
	rangeHeader := fmt.Sprintf("bytes=-%d", tailSize)

	for attempt := 0; attempt <= c.retryAttempts; attempt++ {
		resp, err := c.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(c.bucket),
			Key:    aws.String(fullKey),
			Range:  aws.String(rangeHeader),
		})

		if err == nil {
			defer resp.Body.Close()
			data, readErr := io.ReadAll(resp.Body)
			if readErr != nil {
				return nil, ErrS3Operation{Operation: "read tail response", Err: readErr}
			}
			return data, nil
		}

		if attempt == c.retryAttempts {
			return nil, ErrS3Operation{Operation: "download tail", Err: err}
		}
	}

	return nil, nil
}

// ObjectExists checks if an object exists in S3.
func (c *S3Client) ObjectExists(ctx context.Context, key string) (bool, error) {
	fullKey := c.buildKey(key)

	_, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(fullKey),
	})

	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return false, nil
		}
		return false, ErrS3Operation{Operation: "head object", Err: err}
	}

	return true, nil
}

// GetObjectSize returns the size of an object in S3.
func (c *S3Client) GetObjectSize(ctx context.Context, key string) (int64, error) {
	fullKey := c.buildKey(key)

	resp, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(fullKey),
	})

	if err != nil {
		return 0, ErrS3Operation{Operation: "head object", Err: err}
	}

	return *resp.ContentLength, nil
}
