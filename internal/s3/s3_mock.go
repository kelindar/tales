// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package s3

import (
	s3lib "github.com/kelindar/s3"
	"github.com/kelindar/s3/aws"
	s3mock "github.com/kelindar/s3/mock"
)

// NewMockConfig creates a Config pointing to the mock server.
func NewMockConfig(_ *s3mock.Server, bucket, prefix string) Config {
	return Config{
		Bucket: bucket,
		Region: "us-east-1",
		Prefix: prefix,
	}
}

// NewMockClient creates a new S3 client configured to use the mock server.
func NewMockClient(server *s3mock.Server, cfg Config) (Client, error) {
	key := aws.DeriveKey("", "test", "test", cfg.Region, "s3")
	key.BaseURI = server.URL()
	bucket := s3lib.NewBucket(key, cfg.Bucket)
	bucket.Lazy = true
	return &S3Client{bucket: bucket, key: key, bucketName: cfg.Bucket, prefix: cfg.Prefix}, nil
}
