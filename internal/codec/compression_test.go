// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package codec

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCodecCreation(t *testing.T) {
	t.Run("NewCodec", func(t *testing.T) {
		codec, err := NewCodec()
		require.NoError(t, err)
		assert.NotNil(t, codec)
		assert.NotNil(t, codec.encoder)
		assert.NotNil(t, codec.decoder)

		// Clean up
		codec.Close()
		assert.Nil(t, codec.encoder)
		assert.Nil(t, codec.decoder)
	})

	t.Run("MultipleCodecs", func(t *testing.T) {
		// Test that we can create multiple independent codecs
		codec1, err := NewCodec()
		require.NoError(t, err)
		defer codec1.Close()

		codec2, err := NewCodec()
		require.NoError(t, err)
		defer codec2.Close()

		// They should be independent instances
		assert.NotSame(t, codec1.encoder, codec2.encoder)
		assert.NotSame(t, codec1.decoder, codec2.decoder)
	})
}

func TestCodecCompression(t *testing.T) {
	codec, err := NewCodec()
	require.NoError(t, err)
	defer codec.Close()

	t.Run("BasicCompression", func(t *testing.T) {
		original := []byte("Hello, world! This is a test string for compression.")

		compressed, err := codec.Compress(original)
		require.NoError(t, err)
		assert.NotEmpty(t, compressed)
		assert.NotEqual(t, original, compressed)

		decompressed, err := codec.Decompress(compressed)
		require.NoError(t, err)
		assert.Equal(t, original, decompressed)
	})

	t.Run("EmptyData", func(t *testing.T) {
		original := []byte{}

		compressed, err := codec.Compress(original)
		require.NoError(t, err)

		decompressed, err := codec.Decompress(compressed)
		require.NoError(t, err)
		assert.Empty(t, decompressed)
		// Note: decompressed might be nil instead of empty slice, both are valid for empty data
		assert.Len(t, decompressed, 0)
	})

	t.Run("SingleByte", func(t *testing.T) {
		original := []byte{42}

		compressed, err := codec.Compress(original)
		require.NoError(t, err)

		decompressed, err := codec.Decompress(compressed)
		require.NoError(t, err)
		assert.Equal(t, original, decompressed)
	})

	t.Run("BinaryData", func(t *testing.T) {
		// Test with binary data (not just text)
		original := make([]byte, 256)
		for i := range original {
			original[i] = byte(i)
		}

		compressed, err := codec.Compress(original)
		require.NoError(t, err)

		decompressed, err := codec.Decompress(compressed)
		require.NoError(t, err)
		assert.Equal(t, original, decompressed)
	})
}

func TestCodecCompressionEfficiency(t *testing.T) {
	codec, err := NewCodec()
	require.NoError(t, err)
	defer codec.Close()

	t.Run("RepetitiveData", func(t *testing.T) {
		// Create highly repetitive data that should compress well
		original := bytes.Repeat([]byte("ABCD"), 1000) // 4000 bytes of "ABCDABCD..."

		compressed, err := codec.Compress(original)
		require.NoError(t, err)

		// Should achieve significant compression
		compressionRatio := float64(len(compressed)) / float64(len(original))
		assert.Less(t, compressionRatio, 0.1, "Expected compression ratio < 10%% for repetitive data")

		decompressed, err := codec.Decompress(compressed)
		require.NoError(t, err)
		assert.Equal(t, original, decompressed)
	})

	t.Run("RandomData", func(t *testing.T) {
		// Create pseudo-random data that won't compress as well
		original := make([]byte, 1000)
		for i := range original {
			original[i] = byte((i*7 + 13) % 256) // Pseudo-random pattern
		}

		compressed, err := codec.Compress(original)
		require.NoError(t, err)

		decompressed, err := codec.Decompress(compressed)
		require.NoError(t, err)
		assert.Equal(t, original, decompressed)

		// Even random data should not expand too much with ZSTD
		expansionRatio := float64(len(compressed)) / float64(len(original))
		assert.Less(t, expansionRatio, 1.2, "Expected expansion ratio < 120%% even for random data")
	})
}

func TestCodecLargeData(t *testing.T) {
	codec, err := NewCodec()
	require.NoError(t, err)
	defer codec.Close()

	t.Run("LargeTextData", func(t *testing.T) {
		// Create large text data
		text := "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
		original := []byte(strings.Repeat(text, 1000)) // ~57KB

		compressed, err := codec.Compress(original)
		require.NoError(t, err)
		assert.Less(t, len(compressed), len(original))

		decompressed, err := codec.Decompress(compressed)
		require.NoError(t, err)
		assert.Equal(t, original, decompressed)
	})

	t.Run("VeryLargeData", func(t *testing.T) {
		// Test with 1MB of data
		original := make([]byte, 1024*1024)
		pattern := []byte("The quick brown fox jumps over the lazy dog. ")
		for i := 0; i < len(original); i++ {
			original[i] = pattern[i%len(pattern)]
		}

		compressed, err := codec.Compress(original)
		require.NoError(t, err)

		// Should achieve good compression on repetitive text
		compressionRatio := float64(len(compressed)) / float64(len(original))
		assert.Less(t, compressionRatio, 0.05, "Expected compression ratio < 5%% for large repetitive text")

		decompressed, err := codec.Decompress(compressed)
		require.NoError(t, err)
		assert.Equal(t, original, decompressed)
	})
}

func TestCodecErrorHandling(t *testing.T) {
	codec, err := NewCodec()
	require.NoError(t, err)
	defer codec.Close()

	t.Run("InvalidCompressedData", func(t *testing.T) {
		// Try to decompress invalid data
		invalidData := []byte("this is not compressed data")

		_, err := codec.Decompress(invalidData)
		assert.Error(t, err)
		assert.IsType(t, &CompressionError{}, err)
	})

	t.Run("CorruptedCompressedData", func(t *testing.T) {
		// Create valid compressed data, then corrupt it
		original := []byte("Hello, world!")
		compressed, err := codec.Compress(original)
		require.NoError(t, err)

		// Corrupt the compressed data
		if len(compressed) > 5 {
			compressed[5] ^= 0xFF // Flip bits
		}

		_, err = codec.Decompress(compressed)
		assert.Error(t, err)
		assert.IsType(t, &CompressionError{}, err)
	})
}

func TestCodecConcurrency(t *testing.T) {
	codec, err := NewCodec()
	require.NoError(t, err)
	defer codec.Close()

	t.Run("ConcurrentOperations", func(t *testing.T) {
		// Test that the same codec can be used concurrently
		const numGoroutines = 10
		const dataSize = 1000

		done := make(chan bool, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer func() { done <- true }()

				// Create unique data for this goroutine
				original := make([]byte, dataSize)
				for j := range original {
					original[j] = byte((id*dataSize + j) % 256)
				}

				// Compress and decompress
				compressed, err := codec.Compress(original)
				assert.NoError(t, err)

				decompressed, err := codec.Decompress(compressed)
				assert.NoError(t, err)
				assert.Equal(t, original, decompressed)
			}(i)
		}

		// Wait for all goroutines to complete
		for i := 0; i < numGoroutines; i++ {
			<-done
		}
	})
}

func TestCompressionError(t *testing.T) {
	t.Run("ErrorFormatting", func(t *testing.T) {
		originalErr := errors.New("underlying error")
		compressionErr := &CompressionError{
			Operation: "test operation",
			Err:       originalErr,
		}

		assert.Contains(t, compressionErr.Error(), "compression operation failed")
		assert.Contains(t, compressionErr.Error(), "test operation")
		assert.Contains(t, compressionErr.Error(), "underlying error")

		assert.Equal(t, originalErr, compressionErr.Unwrap())
	})
}
