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
	Upload(ctx context.Context, key string, data []byte) error
	Append(ctx context.Context, key string, data []byte) error
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

// Append appends data to an existing S3 object using a multipart upload.
// If the object does not exist it will be created.
func (c *S3Client) Append(ctx context.Context, key string, data []byte) error {
	fullKey := c.buildKey(key)

	exists, err := c.ObjectExists(ctx, key)
	if err != nil {
		return err
	}

	// If the object doesn't exist simply create it
	if !exists {
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
				return ErrS3Operation{Operation: "put object", Err: err}
			}
		}
		return nil
	}

	// Start multipart upload
	var uploadID string
	for attempt := 0; attempt <= c.retryAttempts; attempt++ {
		resp, err := c.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(c.bucket),
			Key:    aws.String(fullKey),
		})
		if err == nil {
			uploadID = aws.ToString(resp.UploadId)
			break
		}
		if attempt == c.retryAttempts {
			return ErrS3Operation{Operation: "create multipart upload", Err: err}
		}
	}

	completed := make([]types.CompletedPart, 0, 2)

	// Copy existing object as first part
	copySource := fmt.Sprintf("%s/%s", c.bucket, fullKey)
	for attempt := 0; attempt <= c.retryAttempts; attempt++ {
		resp, err := c.client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
			Bucket:     aws.String(c.bucket),
			Key:        aws.String(fullKey),
			PartNumber: aws.Int32(1),
			UploadId:   aws.String(uploadID),
			CopySource: aws.String(copySource),
		})
		if err == nil {
			completed = append(completed, types.CompletedPart{ETag: resp.CopyPartResult.ETag, PartNumber: aws.Int32(1)})
			break
		}
		if attempt == c.retryAttempts {
			c.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(c.bucket), Key: aws.String(fullKey), UploadId: aws.String(uploadID)})
			return ErrS3Operation{Operation: "upload part copy", Err: err}
		}
	}

	// Upload new data as second part
	for attempt := 0; attempt <= c.retryAttempts; attempt++ {
		resp, err := c.client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(c.bucket),
			Key:        aws.String(fullKey),
			PartNumber: aws.Int32(2),
			UploadId:   aws.String(uploadID),
			Body:       bytes.NewReader(data),
		})
		if err == nil {
			completed = append(completed, types.CompletedPart{ETag: resp.ETag, PartNumber: aws.Int32(2)})
			break
		}
		if attempt == c.retryAttempts {
			c.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(c.bucket), Key: aws.String(fullKey), UploadId: aws.String(uploadID)})
			return ErrS3Operation{Operation: "upload part", Err: err}
		}
	}

	// Complete the multipart upload
	_, err = c.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(c.bucket),
		Key:      aws.String(fullKey),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completed,
		},
	})
	if err != nil {
		c.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(c.bucket), Key: aws.String(fullKey), UploadId: aws.String(uploadID)})
		return ErrS3Operation{Operation: "complete multipart upload", Err: err}
	}

	return nil
}

// Download downloads data from S3 with retry logic.
func (c *S3Client) Download(ctx context.Context, key string) ([]byte, error) {
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

// ObjectExists checks if an object exists in S3.
func (c *S3Client) ObjectExists(ctx context.Context, key string) (bool, error) {
	fullKey := c.buildKey(key)

	_, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(fullKey),
	})

	if err != nil {
		// Check for NoSuchKey error (real AWS)
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return false, nil
		}

		// Check for HTTP 404 errors (mock server or other S3-compatible services)
		errStr := err.Error()
		if strings.Contains(errStr, "404") || strings.Contains(errStr, "NotFound") || strings.Contains(errStr, "NoSuchKey") {
			return false, nil
		}

		return false, ErrS3Operation{Operation: "head object", Err: err}
	}

	return true, nil
}

// ObjectSize returns the size of an object in S3.
func (c *S3Client) ObjectSize(ctx context.Context, key string) (int64, error) {
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

// Upload overwrites an S3 object with the given data.
func (c *S3Client) Upload(ctx context.Context, key string, data []byte) error {
	fullKey := c.buildKey(key)

	for attempt := 0; attempt <= c.retryAttempts; attempt++ {
		_, err := c.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(c.bucket),
			Key:    aws.String(fullKey),
			Body:   bytes.NewReader(data),
		})
		if err == nil {
			return nil // Success
		}
		if attempt == c.retryAttempts {
			return ErrS3Operation{Operation: "overwrite object (put)", Err: err}
		}
		// Consider adding a small delay here for retries if appropriate for the application
	}
	return nil // Should be unreachable if retryAttempts >= 0
}

// IsNoSuchKey checks if the error is a NoSuchKey error.
func IsNoSuchKey(err error) bool {
	if err == nil {
		return false
	}

	var nsk *types.NoSuchKey
	switch {
	case errors.As(err, &nsk):
		return true
	case strings.Contains(err.Error(), "NoSuchKey"):
		return true
	case strings.Contains(err.Error(), "404"):
		return true
	default:
		return false
	}
}
