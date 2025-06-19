// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package tales

import (
	"context"
	"fmt"
	"testing"
	"time"

	s3mock "github.com/kelindar/s3/mock"
	"github.com/kelindar/tales/internal/codec"
	"github.com/kelindar/tales/internal/s3"
	"github.com/kelindar/tales/internal/seq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/*
cpu: 13th Gen Intel(R) Core(TM) i7-13700K
BenchmarkLog-24    	 4557070	       228.2 ns/op	   4403361 tps	     869 B/op	       2 allocs/op
*/
func BenchmarkLog(b *testing.B) {
	logger, err := newService()
	require.NoError(b, err)
	defer logger.Close()

	count := 0
	start := time.Now()
	for time.Now().Sub(start) < time.Second {
		const interval = 50 * time.Millisecond
		for i := time.Now(); time.Now().Sub(i) < interval; {
			logger.Log("hello world", 1)
			count++
		}

		logger.flush()
	}

	b.N = count
	b.ReportMetric(float64(count)/time.Now().Sub(start).Seconds(), "tps")
}

/*
cpu: 13th Gen Intel(R) Core(TM) i7-13700K
BenchmarkQuery-24    	      90	  11189897 ns/op	        89.39 qps	 5660155 B/op	   32406 allocs/op
*/
func BenchmarkQuery(b *testing.B) {
	logger, err := newService()
	require.NoError(b, err)
	defer logger.Close()

	for chunk := 0; chunk < 100; chunk++ {
		for i := 0; i < 1000; i++ {
			logger.Log("hello world", uint32(i%100))
		}
		logger.flush()
	}

	b.ResetTimer()
	count := 0
	start := time.Now()
	for time.Now().Sub(start) < time.Second {
		const interval = 50 * time.Millisecond
		for i := time.Now(); time.Now().Sub(i) < interval; {
			for range logger.Query(time.Now().Add(-1*time.Hour), time.Now().Add(1*time.Hour), 1) {
				// Consume the iterator
			}
			count++
		}
	}

	b.N = count
	b.ReportMetric(float64(count)/time.Now().Sub(start).Seconds(), "qps")
}

func TestIntegration(t *testing.T) {
	logger, err := newService()
	require.NoError(t, err)
	defer logger.Close()

	// Log some messages
	logger.Log("hello world 1", 1)
	logger.Log("hello world 2", 2)
	logger.flush()
	logger.Log("hello world 3", 1, 3)
	logger.flush()
	logger.Log("hello world 4", 1, 2)

	from := time.Now().Add(-1 * time.Hour)
	to := time.Now().Add(1 * time.Hour)

	// Query for actor 1
	var results1 []string
	for timestamp, msg := range logger.Query(from, to, 1) {
		_ = timestamp // ignore timestamp for this test
		results1 = append(results1, msg)
	}

	assert.Equal(t, []string{
		"hello world 1",
		"hello world 3",
		"hello world 4",
	}, results1)

	// Query for actor 2
	var results2 []string
	for _, msg := range logger.Query(from, to, 2) {
		results2 = append(results2, msg)
	}
	assert.Equal(t, []string{
		"hello world 2",
		"hello world 4",
	}, results2)

	// Query for actor 3
	var results3 []string
	for _, msg := range logger.Query(from, to, 3) {
		results3 = append(results3, msg)
	}
	assert.Equal(t, []string{
		"hello world 3",
	}, results3)

	// Query for actor 4 (no logs)
	var results4 []string
	for _, msg := range logger.Query(from, to, 4) {
		results4 = append(results4, msg)
	}
	assert.Empty(t, results4)

	// Query for multiple actors (intersection)
	var resultsMultiple []string
	for _, msg := range logger.Query(from, to, 1, 2) {
		resultsMultiple = append(resultsMultiple, msg)
	}
	assert.Equal(t, []string{
		"hello world 4", // Only entry that has both actors 1 and 2
	}, resultsMultiple)
}

func TestBuildKeys(t *testing.T) {
	// Test metadata key
	metadataKey := keyOfMetadata("2023-01-02")
	assert.Equal(t, "2023-01-02/metadata.json", metadataKey)

	// Test chunk key
	chunk := uint64(5)
	assert.Equal(t, "2023-01-02/5.log", keyOfChunk("2023-01-02", chunk))
}

func TestCloseFlushes(t *testing.T) {
	logger, err := newService()
	require.NoError(t, err)

	// Log an event but do not flush manually
	logger.Log("pending event", 1)

	// Close should flush the buffered event
	err = logger.Close()
	require.NoError(t, err)

	ctx := context.Background()
	date := seq.FormatDate(time.Now())

	// Metadata should exist
	exists, err := logger.s3Client.ObjectExists(ctx, keyOfMetadata(date))
	require.NoError(t, err)
	assert.True(t, exists)

	// Metadata should describe at least one chunk and the chunk file must exist
	buf, err := logger.s3Client.Download(ctx, keyOfMetadata(date))
	require.NoError(t, err)
	meta, err := codec.DecodeMetadata(buf)
	require.NoError(t, err)
	if assert.Greater(t, meta.Length, uint32(0)) {
		chunkKey := keyOfChunk(date, 0)
		exists, err = logger.s3Client.ObjectExists(ctx, chunkKey)
		require.NoError(t, err)
		assert.True(t, exists)
	}
}

func TestParallelDownloadsConfiguration(t *testing.T) {
	// Test with custom parallel downloads setting
	logger, err := New(
		"test-bucket", 
		"us-east-1",
		WithParallelDownloads(8),
		WithClient(func(config s3.Config) (s3.Client, error) {
			mockS3 := s3mock.New("test-bucket", "us-east-1")
			return s3.NewMockClient(mockS3, config)
		}),
	)
	require.NoError(t, err)
	defer logger.Close()
	
	assert.Equal(t, 8, logger.config.ParallelDownloads)
	
	// Test with default value
	logger2, err := New(
		"test-bucket", 
		"us-east-1",
		WithClient(func(config s3.Config) (s3.Client, error) {
			mockS3 := s3mock.New("test-bucket", "us-east-1")
			return s3.NewMockClient(mockS3, config)
		}),
	)
	require.NoError(t, err)
	defer logger2.Close()
	
	assert.Equal(t, 4, logger2.config.ParallelDownloads) // default value
	
	// Test validation
	_, err = New("test-bucket", "us-east-1", 
		WithParallelDownloads(0),
		WithClient(func(config s3.Config) (s3.Client, error) {
			mockS3 := s3mock.New("test-bucket", "us-east-1")
			return s3.NewMockClient(mockS3, config)
		}),
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parallel downloads must be at least 1")
	
	_, err = New("test-bucket", "us-east-1", 
		WithParallelDownloads(25),
		WithClient(func(config s3.Config) (s3.Client, error) {
			mockS3 := s3mock.New("test-bucket", "us-east-1")
			return s3.NewMockClient(mockS3, config)
		}),
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parallel downloads must not exceed 20")
}

func TestParallelDownloadsBehavior(t *testing.T) {
	// Create a service with parallel downloads
	logger, err := New(
		"test-bucket", 
		"us-east-1",
		WithParallelDownloads(3),
		WithClient(func(config s3.Config) (s3.Client, error) {
			mockS3 := s3mock.New("test-bucket", "us-east-1")
			return s3.NewMockClient(mockS3, config)
		}),
	)
	require.NoError(t, err)
	defer logger.Close()

	// Add multiple log entries that will create multiple chunks
	for i := 0; i < 100; i++ {
		err = logger.Log(fmt.Sprintf("test message %d", i), uint32(i%10))
		require.NoError(t, err)
		if i%20 == 19 { // Flush every 20 entries to create multiple chunks
			logger.flush()
		}
	}
	logger.flush() // Final flush

	// Query to trigger parallel downloads
	from := time.Now().Add(-1 * time.Hour)
	to := time.Now().Add(1 * time.Hour)
	
	count := 0
	for _, msg := range logger.Query(from, to, 1) {
		_ = msg
		count++
	}
	
	// Should find entries for actor 1 (messages 1, 11, 21, 31, 41, 51, 61, 71, 81, 91)
	assert.Greater(t, count, 0, "Should find some log entries")
}

func newService() (*Service, error) {
	// Create a mock S3 server
	mockS3 := s3mock.New("test-bucket", "us-east-1")

	return New(
		"test-bucket",
		"us-east-1",
		WithPrefix("test-prefix"),
		WithInterval(1*time.Minute),
		WithBuffer(1024*1024),
		WithClient(func(config s3.Config) (s3.Client, error) {
			return s3.NewMockClient(mockS3, config)
		}),
	)
}
