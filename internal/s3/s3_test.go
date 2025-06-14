package s3

import (
	"context"
	"io/fs"
	"testing"

	s3mock "github.com/kelindar/s3/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3ClientBasics(t *testing.T) {
	server := s3mock.New("test-bucket", "us-east-1")
	defer server.Close()

	cfg := CreateConfigForMock(server, "test-bucket", "test")
	client, err := NewMockClient(context.Background(), server, cfg)
	require.NoError(t, err)

	ctx := context.Background()
	key := "file.txt"
	data := []byte("hello world")

	// Upload and download
	err = client.Upload(ctx, key, data)
	require.NoError(t, err)

	out, err := client.Download(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, data, out)

	// Range
	part, err := client.DownloadRange(ctx, key, 0, int64(len(data))-1)
	require.NoError(t, err)
	assert.Equal(t, data, part)

	// Exists
	exists, err := client.ObjectExists(ctx, key)
	require.NoError(t, err)
	assert.True(t, exists)

	// Size
	size, err := client.ObjectSize(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, int64(len(data)), size)
}

func TestIsNoSuchKey(t *testing.T) {
	assert.True(t, IsNoSuchKey(fs.ErrNotExist))
	assert.False(t, IsNoSuchKey(nil))
}
