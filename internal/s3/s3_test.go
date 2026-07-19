// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package s3

import (
	"context"
	"io/fs"
	"testing"

	s3lib "github.com/kelindar/s3"
	s3mock "github.com/kelindar/s3/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3ClientBasics(t *testing.T) {
	server := s3mock.New("test-bucket", "us-east-1")
	defer server.Close()

	cfg := NewMockConfig(server, "test-bucket", "test")
	client, err := NewMockClient(server, cfg)
	require.NoError(t, err)

	ctx := context.Background()
	key := "file.txt"
	data := []byte("hello world")

	// Upload and download
	etag, err := client.Upload(ctx, key, data)
	require.NoError(t, err)
	require.NotEmpty(t, etag)

	out, err := client.Download(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, data, out)

	// Range
	part, err := client.DownloadRange(ctx, key, etag, 0, int64(len(data)))
	require.NoError(t, err)
	assert.Equal(t, data, part)
	requests := server.GetRequestsWithMethod("GET")
	require.Equal(t, etag, requests[len(requests)-1].Headers["If-Match"])
	_, err = client.DownloadRange(ctx, key, "stale-etag", 0, int64(len(data)))
	require.ErrorIs(t, err, s3lib.ErrETagChanged)

	object, err := client.Stat(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, int64(len(data)), object.Size)
	assert.Equal(t, etag, object.ETag)
	assert.Len(t, server.GetRequestsWithMethod("HEAD"), 1)

	var listed []Object
	for object, err := range client.List(ctx, ".") {
		require.NoError(t, err)
		listed = append(listed, object)
	}
	require.Len(t, listed, 1)
	require.NoError(t, client.Delete(ctx, key))
	_, err = client.Stat(ctx, key)
	require.True(t, IsNoSuchKey(err))
}

func TestNoSuchKey(t *testing.T) {
	assert.True(t, IsNoSuchKey(fs.ErrNotExist))
	assert.False(t, IsNoSuchKey(nil))
}
