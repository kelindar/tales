// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"testing"
	"testing/iotest"

	s3lib "github.com/kelindar/s3"
	s3mock "github.com/kelindar/s3/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadRange(t *testing.T) {
	failure := errors.New("read failed")
	for _, test := range []struct {
		name   string
		reader io.Reader
		size   int64
		want   string
		err    error
	}{
		{"exact", strings.NewReader("abc"), 3, "abc", nil},
		{"empty", strings.NewReader(""), 0, "", nil},
		{"short", strings.NewReader("ab"), 3, "", io.ErrUnexpectedEOF},
		{"missing", strings.NewReader(""), 3, "", io.ErrUnexpectedEOF},
		{"excess", strings.NewReader("abcd"), 3, "", io.ErrUnexpectedEOF},
		{"unexpected", strings.NewReader("a"), 0, "", io.ErrUnexpectedEOF},
		{"failure", iotest.ErrReader(failure), 3, "", failure},
		{"trailing failure", io.MultiReader(strings.NewReader("abc"), iotest.ErrReader(failure)), 3, "", failure},
		{"fragmented", iotest.OneByteReader(strings.NewReader("abc")), 3, "abc", nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := readRange(test.reader, test.size)
			assert.ErrorIs(t, err, test.err)
			assert.Equal(t, test.want, string(got))
		})
	}
	_, err := readRange(strings.NewReader(""), -1)
	assert.Error(t, err)
}

func BenchmarkRangeRead(b *testing.B) {
	data := bytes.Repeat([]byte("x"), 7_200_000)
	for _, exact := range []bool{false, true} {
		b.Run(fmt.Sprint(exact), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				reader := bytes.NewReader(data)
				var got []byte
				var err error
				if exact {
					got, err = readRange(reader, int64(len(data)))
				} else {
					got, err = io.ReadAll(reader)
				}
				if err != nil || len(got) != len(data) {
					b.Fatal("invalid range", err)
				}
			}
		})
	}
}

func TestClient(t *testing.T) {
	tests := map[string]func(*testing.T){
		"basics":      testClientBasics,
		"list prefix": testListPrefix,
		"new client":  testNewClient,
	}
	for name, fn := range tests {
		t.Run(name, fn)
	}
}

func testClientBasics(t *testing.T) {
	server := s3mock.New("test-bucket", "us-east-1")
	defer server.Close()

	cfg := NewMockConfig(server, "test-bucket", "test")
	client, err := NewMockClient(server, cfg)
	require.NoError(t, err)

	ctx := context.Background()
	key := "file.txt"
	data := []byte("hello world")

	etag, err := client.Upload(ctx, key, data)
	require.NoError(t, err)
	require.NotEmpty(t, etag)

	out, err := client.Download(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, data, out)

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

func testListPrefix(t *testing.T) {
	server := s3mock.New("test-bucket", "us-east-1")
	defer server.Close()
	cfg := NewMockConfig(server, "test-bucket", "root")
	client, err := NewMockClient(server, cfg)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = client.Upload(ctx, "a/one.txt", []byte("1"))
	require.NoError(t, err)
	_, err = client.Upload(ctx, "b/two.txt", []byte("2"))
	require.NoError(t, err)

	var keys []string
	for object, err := range client.List(ctx, "") {
		require.NoError(t, err)
		keys = append(keys, object.Key)
	}
	assert.NotEmpty(t, keys)
}

func testNewClient(t *testing.T) {
	_, err := NewClient(Config{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "credentials")
}

func TestErrors(t *testing.T) {
	t.Run("no such key", func(t *testing.T) {
		assert.True(t, IsNoSuchKey(fs.ErrNotExist))
		assert.True(t, IsNoSuchKey(fmt.Errorf("NoSuchKey")))
		assert.True(t, IsNoSuchKey(fmt.Errorf("status 404")))
		assert.False(t, IsNoSuchKey(nil))
	})
	t.Run("compose unsupported", func(t *testing.T) {
		assert.True(t, IsComposeUnsupported(fmt.Errorf("NotImplemented")))
		assert.True(t, IsComposeUnsupported(fmt.Errorf("not supported")))
		assert.True(t, IsComposeUnsupported(fmt.Errorf("HTTP 501")))
		assert.False(t, IsComposeUnsupported(fmt.Errorf("timeout")))
	})
	t.Run("operation wrap", func(t *testing.T) {
		err := ErrS3Operation{Operation: "write", Err: fs.ErrNotExist}
		assert.Contains(t, err.Error(), "write")
		assert.ErrorIs(t, err, fs.ErrNotExist)
	})
}
