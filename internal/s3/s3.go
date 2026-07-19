// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"slices"
	"strings"

	s3lib "github.com/kelindar/s3"
	"github.com/kelindar/s3/aws"
)

type Config struct {
	Bucket  string
	Region  string
	Prefix  string
	Service string
	Key     *aws.SigningKey
}

type Object struct {
	Key  string
	ETag string
	Size int64
	Dir  bool
}

type Client interface {
	Upload(context.Context, string, []byte) (string, error)
	UploadReader(context.Context, string, io.ReaderAt, int64) (string, error)
	Download(context.Context, string) ([]byte, error)
	DownloadRange(context.Context, string, string, int64, int64) ([]byte, error)
	List(context.Context, string) iter.Seq2[Object, error]
	Stat(context.Context, string) (Object, error)
	Delete(context.Context, string) error
	Compose(context.Context, string, []s3lib.CopyPart) (string, error)
}

type ErrS3Operation struct {
	Operation string
	Err       error
}

func (e ErrS3Operation) Error() string {
	return fmt.Sprintf("S3 operation failed (%s): %v", e.Operation, e.Err)
}

func (e ErrS3Operation) Unwrap() error { return e.Err }

type S3Client struct {
	bucket     *s3lib.Bucket
	key        *aws.SigningKey
	bucketName string
	prefix     string
}

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
	return &S3Client{bucket: bucket, key: key, bucketName: cfg.Bucket, prefix: strings.Trim(cfg.Prefix, "/")}, nil
}

func (c *S3Client) buildKey(key string) string {
	if c.prefix == "" {
		return key
	}
	return c.prefix + "/" + strings.TrimPrefix(key, "/")
}

func (c *S3Client) trimKey(key string) string {
	if c.prefix == "" {
		return key
	}
	return strings.TrimPrefix(key, c.prefix+"/")
}

func (c *S3Client) Upload(ctx context.Context, key string, data []byte) (string, error) {
	etag, err := c.bucket.Write(ctx, c.buildKey(key), data)
	if err != nil {
		return "", ErrS3Operation{Operation: "write", Err: err}
	}
	return etag, nil
}

func (c *S3Client) UploadReader(ctx context.Context, key string, reader io.ReaderAt, size int64) (string, error) {
	if err := c.bucket.WriteFrom(ctx, c.buildKey(key), reader, size); err != nil {
		return "", ErrS3Operation{Operation: "write", Err: err}
	}
	object, err := c.Stat(ctx, key)
	if err != nil {
		return "", err
	}
	return object.ETag, nil
}

func (c *S3Client) Download(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := c.bucket.Open(c.buildKey(key))
	if err != nil {
		return nil, ErrS3Operation{Operation: "download", Err: err}
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return nil, ErrS3Operation{Operation: "download", Err: err}
	}
	return data, nil
}

func (c *S3Client) DownloadRange(ctx context.Context, key, etag string, offset, size int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reader, err := c.bucket.OpenRange(c.buildKey(key), etag, offset, size)
	if err != nil {
		return nil, ErrS3Operation{Operation: "range", Err: err}
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err == nil {
		err = ctx.Err()
	}
	if err == nil && int64(len(data)) != size {
		err = io.ErrUnexpectedEOF
	}
	if err != nil {
		return nil, ErrS3Operation{Operation: "range", Err: err}
	}
	return data, nil
}

func (c *S3Client) List(ctx context.Context, prefix string) iter.Seq2[Object, error] {
	return func(yield func(Object, error) bool) {
		for entry, err := range c.bucket.List(ctx, c.buildKey(prefix)) {
			if err != nil {
				yield(Object{}, ErrS3Operation{Operation: "list", Err: err})
				return
			}
			switch value := entry.(type) {
			case *s3lib.File:
				if !yield(Object{Key: c.trimKey(value.Path()), ETag: value.Reader.ETag, Size: value.Reader.Size}, nil) {
					return
				}
			case *s3lib.Prefix:
				if !yield(Object{Key: strings.TrimSuffix(c.trimKey(value.Path), "/"), Dir: true}, nil) {
					return
				}
			}
		}
	}
}

func (c *S3Client) Stat(ctx context.Context, key string) (Object, error) {
	if err := ctx.Err(); err != nil {
		return Object{}, err
	}
	reader, err := s3lib.Stat(c.key, c.bucketName, c.buildKey(key))
	if err != nil {
		return Object{}, ErrS3Operation{Operation: "head", Err: err}
	}
	if err := ctx.Err(); err != nil {
		return Object{}, err
	}
	return Object{Key: key, ETag: reader.ETag, Size: reader.Size}, nil
}

func (c *S3Client) Delete(ctx context.Context, key string) error {
	if err := c.bucket.Delete(ctx, c.buildKey(key)); err != nil {
		return ErrS3Operation{Operation: "delete", Err: err}
	}
	return nil
}

func (c *S3Client) Compose(ctx context.Context, key string, parts []s3lib.CopyPart) (string, error) {
	parts = slices.Clone(parts)
	for i := range parts {
		parts[i].SourceKey = c.buildKey(parts[i].SourceKey)
	}
	etag, err := c.bucket.Compose(ctx, c.buildKey(key), parts)
	if err != nil {
		return "", ErrS3Operation{Operation: "compose", Err: err}
	}
	return etag, nil
}

func IsNoSuchKey(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || strings.Contains(fmt.Sprint(err), "NoSuchKey") || strings.Contains(fmt.Sprint(err), "404")
}

func IsComposeUnsupported(err error) bool {
	message := strings.ToLower(fmt.Sprint(err))
	return strings.Contains(message, "notimplemented") || strings.Contains(message, "not implemented") || strings.Contains(message, "not supported") || strings.Contains(message, "501")
}
