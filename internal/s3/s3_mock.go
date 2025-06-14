package s3

import (
	"context"

	s3lib "github.com/kelindar/s3"
	"github.com/kelindar/s3/aws"
	s3mock "github.com/kelindar/s3/mock"
)

// CreateConfigForMock creates a Config pointing to the mock server.
func CreateConfigForMock(_ *s3mock.Server, bucket, prefix string) Config {
	return Config{
		Bucket: bucket,
		Region: "us-east-1",
		Prefix: prefix,
	}
}

// NewMockClient creates a new S3 client configured to use the mock server.
func NewMockClient(ctx context.Context, server *s3mock.Server, cfg Config) (Client, error) {
	key := aws.DeriveKey("", "test", "test", cfg.Region, "s3")
	key.BaseURI = server.URL()
	bucket := s3lib.NewBucket(ctx, key, cfg.Bucket)
	bucket.Lazy = true
	return &S3Client{bucket: bucket, key: key, bucketName: cfg.Bucket, prefix: cfg.Prefix}, nil
}
