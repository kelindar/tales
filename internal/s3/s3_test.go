package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"testing"

	s3lib "github.com/kelindar/s3"
	s3mock "github.com/kelindar/s3/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3ClientWithMock(t *testing.T) {
	// Start mock S3 server
	mockServer := s3mock.New("test-bucket", "us-east-1")
	defer mockServer.Close()

	// Create S3 config for mock
	config := CreateConfigForMock(mockServer, "test-bucket", "test-prefix")

	// Create S3 client that connects to mock server
	client, err := NewMockClient(context.Background(), mockServer, config)
	require.NoError(t, err)

	t.Run("UploadAndDownload", func(t *testing.T) {
		ctx := context.Background()
		key := "test-file.txt"
		data := []byte("Hello, S3!")

		// Upload data
		err := client.Append(ctx, key, data)
		assert.NoError(t, err)

		// Verify it exists in mock server
		storedData, exists := mockServer.ObjectContent("test-prefix/" + key)
		assert.True(t, exists)
		assert.Equal(t, data, storedData)

		// Download data back
		downloadedData, err := client.Download(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, data, downloadedData)
	})

	t.Run("AppendData", func(t *testing.T) {
		ctx := context.Background()
		key := "append-test.txt"

		// First upload
		data1 := []byte("First part")
		err := client.Append(ctx, key, data1)
		assert.NoError(t, err)

		// Append more data
		data2 := []byte(" Second part")
		err = client.Append(ctx, key, data2)
		assert.NoError(t, err)

		// Download and verify combined data
		combined, err := client.Download(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, "First part Second part", string(combined))
	})

	t.Run("AppendToNonExistentFile", func(t *testing.T) {
		ctx := context.Background()
		key := "new-file.txt"
		data := []byte("New file content")

		// Append to non-existent file should create it
		err := client.Append(ctx, key, data)
		assert.NoError(t, err)

		// Verify file was created
		downloaded, err := client.Download(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, data, downloaded)
	})

	t.Run("AppendLargeObject", func(t *testing.T) {
		ctx := context.Background()
		key := "large-file.bin"
		large := bytes.Repeat([]byte("A"), s3lib.MinPartSize+1024)

		// Upload a large object first
		err := client.Append(ctx, key, large)
		require.NoError(t, err)

		// Append a small chunk
		small := bytes.Repeat([]byte("B"), 1024)
		err = client.Append(ctx, key, small)
		require.NoError(t, err)

		// Verify size and content
		combined, err := client.Download(ctx, key)
		require.NoError(t, err)
		require.Equal(t, len(large)+len(small), len(combined))
		assert.Equal(t, large, combined[:len(large)])
		assert.Equal(t, small, combined[len(large):])
	})

	t.Run("DownloadRange", func(t *testing.T) {
		ctx := context.Background()
		key := "range-test.txt"
		data := []byte("0123456789ABCDEF")

		// Upload test data
		err := client.Append(ctx, key, data)
		assert.NoError(t, err)

		// Download range (bytes 5-9)
		rangeData, err := client.DownloadRange(ctx, key, 5, 9)
		assert.NoError(t, err)
		assert.Equal(t, "56789", string(rangeData))

		// Download range from start
		rangeData, err = client.DownloadRange(ctx, key, 0, 4)
		assert.NoError(t, err)
		assert.Equal(t, "01234", string(rangeData))

		// Download range to end
		rangeData, err = client.DownloadRange(ctx, key, 12, 15)
		assert.NoError(t, err)
		assert.Equal(t, "CDEF", string(rangeData))
	})

	t.Run("ObjectExists", func(t *testing.T) {
		ctx := context.Background()
		existingKey := "existing-file.txt"
		nonExistentKey := "non-existent-file.txt"

		// Upload a file
		err := client.Append(ctx, existingKey, []byte("test"))
		assert.NoError(t, err)

		// Check existing file
		exists, err := client.ObjectExists(ctx, existingKey)
		assert.NoError(t, err)
		assert.True(t, exists)

		// Check non-existent file
		exists, err = client.ObjectExists(ctx, nonExistentKey)
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("GetObjectSize", func(t *testing.T) {
		ctx := context.Background()
		key := "size-test.txt"
		data := []byte("This file has exactly 32 chars!")

		// Upload test data
		err := client.Append(ctx, key, data)
		assert.NoError(t, err)

		// Get object size
		size, err := client.ObjectSize(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(data)), size)
		assert.Greater(t, size, int64(0)) // Should be positive
	})

	t.Run("DownloadNonExistentFile", func(t *testing.T) {
		ctx := context.Background()
		key := "does-not-exist.txt"

		// Try to download non-existent file
		_, err := client.Download(ctx, key)
		assert.Error(t, err)

		// Should be an S3 operation error
		var s3Err ErrS3Operation
		assert.ErrorAs(t, err, &s3Err)
		assert.Equal(t, "download", s3Err.Operation)
	})

	t.Run("GetSizeOfNonExistentFile", func(t *testing.T) {
		ctx := context.Background()
		key := "does-not-exist.txt"

		// Try to get size of non-existent file
		_, err := client.ObjectSize(ctx, key)
		assert.Error(t, err)

		// Should be an S3 operation error
		var s3Err ErrS3Operation
		assert.ErrorAs(t, err, &s3Err)
		assert.Equal(t, "head", s3Err.Operation)
	})
}

func TestS3ClientKeyPrefixing(t *testing.T) {
	// Start mock S3 server
	mockServer := s3mock.New("test-bucket", "us-east-1")
	defer mockServer.Close()

	t.Run("WithPrefix", func(t *testing.T) {
		// Create config with prefix
		config := CreateConfigForMock(mockServer, "test-bucket", "my/prefix")
		client, err := NewMockClient(context.Background(), mockServer, config)
		require.NoError(t, err)

		ctx := context.Background()
		key := "test-file.txt"
		data := []byte("test data")

		// Upload data
		err = client.Append(ctx, key, data)
		assert.NoError(t, err)

		// Verify it's stored with prefix
		storedData, exists := mockServer.ObjectContent("my/prefix/" + key)
		assert.True(t, exists)
		assert.Equal(t, data, storedData)

		// Download should work without specifying prefix
		downloaded, err := client.Download(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, data, downloaded)
	})

	t.Run("WithoutPrefix", func(t *testing.T) {
		// Create config without prefix
		config := CreateConfigForMock(mockServer, "test-bucket", "")
		client, err := NewMockClient(context.Background(), mockServer, config)
		require.NoError(t, err)

		ctx := context.Background()
		key := "no-prefix-file.txt"
		data := []byte("no prefix data")

		// Upload data
		err = client.Append(ctx, key, data)
		assert.NoError(t, err)

		// Verify it's stored without prefix
		storedData, exists := mockServer.ObjectContent(key)
		assert.True(t, exists)
		assert.Equal(t, data, storedData)
	})
}

func TestMockServerDirectly(t *testing.T) {
	mockServer := s3mock.New("test-bucket", "us-east-1")
	defer mockServer.Close()

	t.Run("ListObjects", func(t *testing.T) {
		// Initially empty
		objects := mockServer.ListObjects("")
		assert.Empty(t, objects)

		// Create config and client
		config := CreateConfigForMock(mockServer, "test-bucket", "test")
		client, err := NewMockClient(context.Background(), mockServer, config)
		require.NoError(t, err)

		// Upload some files
		ctx := context.Background()
		files := map[string][]byte{
			"file1.txt": []byte("content1"),
			"file2.txt": []byte("content2"),
			"file3.txt": []byte("content3"),
		}

		for key, data := range files {
			err := client.Append(ctx, key, data)
			assert.NoError(t, err)
		}

		// List objects
		objects = mockServer.ListObjects("")
		assert.Len(t, objects, 3)

		// Should be sorted
		expectedKeys := []string{
			"test/file1.txt",
			"test/file2.txt",
			"test/file3.txt",
		}
		assert.Equal(t, expectedKeys, objects)
	})

	t.Run("GetObjectDirectly", func(t *testing.T) {
		// Upload via client
		config := CreateConfigForMock(mockServer, "test-bucket", "direct")
		client, err := NewMockClient(context.Background(), mockServer, config)
		require.NoError(t, err)

		ctx := context.Background()
		key := "direct-test.txt"
		data := []byte("direct access test")

		err = client.Append(ctx, key, data)
		assert.NoError(t, err)

		// Access directly via mock server
		storedData, exists := mockServer.ObjectContent("direct/" + key)
		assert.True(t, exists)
		assert.Equal(t, data, storedData)

		// Try non-existent key
		_, exists = mockServer.ObjectContent("non-existent-key")
		assert.False(t, exists)
	})
}

func TestIsNoSuchKey(t *testing.T) {
	t.Run("with fs.ErrNotExist", func(t *testing.T) {
		assert.True(t, IsNoSuchKey(fs.ErrNotExist))
	})

	t.Run("with wrapped fs.ErrNotExist", func(t *testing.T) {
		err := fmt.Errorf("wrapped: %w", fs.ErrNotExist)
		assert.True(t, IsNoSuchKey(err))
	})

	t.Run("with 'NoSuchKey' substring", func(t *testing.T) {
		assert.True(t, IsNoSuchKey(errors.New("some error with NoSuchKey in it")))
	})

	t.Run("with '404' substring", func(t *testing.T) {
		assert.True(t, IsNoSuchKey(errors.New("http status 404")))
	})

	t.Run("with unrelated error", func(t *testing.T) {
		assert.False(t, IsNoSuchKey(errors.New("some other error")))
	})

	t.Run("with nil error", func(t *testing.T) {
		assert.False(t, IsNoSuchKey(nil))
	})
}
