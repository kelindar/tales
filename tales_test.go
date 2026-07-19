// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package tales

import (
	"context"
	"fmt"
	"log"
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

func TestQuery(t *testing.T) {
	tests := map[string]func(*testing.T){
		"integration":          testQueryIntegration,
		"multi day":            testQueryMultiDay,
		"downloads chunk once": testQueryDownloadsOnce,
	}
	for name, fn := range tests {
		t.Run(name, fn)
	}
}

func testQueryIntegration(t *testing.T) {
	logger, err := newService()
	require.NoError(t, err)
	defer logger.Close()

	logger.Log("hello world 1", 1)
	logger.Log("hello world 2", 2)
	logger.flush()
	logger.Log("hello world 3", 1, 3)
	logger.flush()
	logger.Log("hello world 4", 1, 2)

	from := time.Now().Add(-1 * time.Hour)
	to := time.Now().Add(1 * time.Hour)

	var results1 []string
	for timestamp, msg := range logger.Query(from, to, 1) {
		_ = timestamp
		results1 = append(results1, msg)
	}

	assert.Equal(t, []string{
		"hello world 1",
		"hello world 3",
		"hello world 4",
	}, results1)

	var results2 []string
	for _, msg := range logger.Query(from, to, 2) {
		results2 = append(results2, msg)
	}
	assert.Equal(t, []string{
		"hello world 2",
		"hello world 4",
	}, results2)

	var results3 []string
	for _, msg := range logger.Query(from, to, 3) {
		results3 = append(results3, msg)
	}
	assert.Equal(t, []string{
		"hello world 3",
	}, results3)

	var results4 []string
	for _, msg := range logger.Query(from, to, 4) {
		results4 = append(results4, msg)
	}
	assert.Empty(t, results4)

	var resultsMultiple []string
	for _, msg := range logger.Query(from, to, 1, 2) {
		resultsMultiple = append(resultsMultiple, msg)
	}
	assert.Equal(t, []string{
		"hello world 4",
	}, resultsMultiple)
}

func testQueryMultiDay(t *testing.T) {
	logger, err := newService()
	require.NoError(t, err)
	defer logger.Close()

	logger.Log("message from day 1", 1)
	logger.Log("message from day 1 actor 2", 2)
	logger.flush()
	logger.Log("another message from day 1", 1, 3)
	logger.flush()

	futureNow := time.Now().Add(2 * 24 * time.Hour)
	from := futureNow.Add(-3 * 24 * time.Hour)
	to := futureNow.Add(1 * 24 * time.Hour)

	var results1 []string
	for _, msg := range logger.Query(from, to, 1) {
		results1 = append(results1, msg)
	}

	assert.Equal(t, []string{
		"message from day 1",
		"another message from day 1",
	}, results1)

	var resultsMultiple []string
	for _, msg := range logger.Query(from, to, 1, 3) {
		resultsMultiple = append(resultsMultiple, msg)
	}
	assert.Equal(t, []string{
		"another message from day 1",
	}, resultsMultiple)

	futureFrom := time.Now().Add(5 * 24 * time.Hour)
	futureTo := time.Now().Add(7 * 24 * time.Hour)
	var futureResults []string
	for _, msg := range logger.Query(futureFrom, futureTo, 1) {
		futureResults = append(futureResults, msg)
	}
	assert.Empty(t, futureResults, "Should not find any results in future time range")
}

func testQueryDownloadsOnce(t *testing.T) {
	logger, err := newService()
	require.NoError(t, err)
	defer logger.Close()

	require.NoError(t, logger.Log("hello world", 1))
	logger.flush()

	client := &countingClient{Client: logger.s3Client}
	logger.s3Client = client
	for range logger.Query(time.Now().Add(-time.Hour), time.Now().Add(time.Hour), 1) {
	}
	assert.Equal(t, 1, client.ranges)
}

func TestKeys(t *testing.T) {
	assert.Equal(t, "2023-01-02/metadata.json", keyOfMetadata("2023-01-02"))
	assert.Equal(t, "2023-01-02/5.log", keyOfChunk("2023-01-02", 5))
}

func TestClose(t *testing.T) {
	logger, err := newService()
	require.NoError(t, err)

	logger.Log("pending event", 1)

	err = logger.Close()
	require.NoError(t, err)

	ctx := context.Background()
	date := seq.FormatDate(time.Now())

	exists, err := logger.s3Client.ObjectExists(ctx, keyOfMetadata(date))
	require.NoError(t, err)
	assert.True(t, exists)

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

// Example demonstrates basic usage of the tales library
func Example() {
	mockServer := s3mock.New("example-bucket", "us-east-1")
	defer mockServer.Close()

	logger, err := New(
		"example-bucket",
		"us-east-1",
		WithPrefix("events"),
		WithInterval(5*time.Minute),
		WithBuffer(1000),
		WithClient(func(cfg s3.Config) (s3.Client, error) {
			return s3.NewMockClient(mockServer, cfg)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer logger.Close()

	logger.Log("Player joined the game", 12345)
	logger.Log("Player moved to position (100, 200)", 12345)
	logger.Log("Player attacked monster", 12345, 67890)
	logger.Log("Monster died", 67890)
	logger.Log("Player gained 100 XP", 12345)

	from := time.Now().Add(-1 * time.Hour)
	to := time.Now().Add(1 * time.Hour)

	fmt.Println("Events for player 12345:")
	var count int
	for _, text := range logger.Query(from, to, 12345) {
		fmt.Printf("- %s\n", text)
		count++
	}
	fmt.Printf("Total events: %d\n", count)

	fmt.Println("\nEvents involving both player 12345 and monster 67890:")
	count = 0
	for _, text := range logger.Query(from, to, 12345, 67890) {
		fmt.Printf("- %s\n", text)
		count++
	}
	fmt.Printf("Total intersection events: %d\n", count)

	// Output:
	// Events for player 12345:
	// - Player joined the game
	// - Player moved to position (100, 200)
	// - Player attacked monster
	// - Player gained 100 XP
	// Total events: 4
	//
	// Events involving both player 12345 and monster 67890:
	// - Player attacked monster
	// Total intersection events: 1
}

func newService() (*Service, error) {
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

type countingClient struct {
	s3.Client
	ranges int
}

func (c *countingClient) DownloadRange(ctx context.Context, key string, start, end int64) ([]byte, error) {
	c.ranges++
	return c.Client.DownloadRange(ctx, key, start, end)
}
