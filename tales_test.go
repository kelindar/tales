package threads

import (
	"context"
	"testing"
	"time"

	s3mock "github.com/kelindar/s3/mock"
	"github.com/kelindar/threads/internal/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/*
BenchmarkTales-12    	       1	9549168167 ns/op	     26822 log/s	182058528 B/op	 1027347 allocs/op
*/
func BenchmarkTales(b *testing.B) {
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
		logger.Query(1, time.Now().Add(-1*time.Hour), time.Now().Add(1*time.Hour))
	}

	b.N = count
	b.ReportMetric(float64(count)/time.Now().Sub(start).Seconds(), "log/s")
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
	for timestamp, msg := range logger.Query(1, from, to) {
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
	for _, msg := range logger.Query(2, from, to) {
		results2 = append(results2, msg)
	}
	assert.Equal(t, []string{
		"hello world 2",
		"hello world 4",
	}, results2)

	// Query for actor 3
	var results3 []string
	for _, msg := range logger.Query(3, from, to) {
		results3 = append(results3, msg)
	}
	assert.Equal(t, []string{
		"hello world 3",
	}, results3)

	// Query for actor 4 (no logs)
	var results4 []string
	for _, msg := range logger.Query(4, from, to) {
		results4 = append(results4, msg)
	}
	assert.Empty(t, results4)
}

func newService() (*Service, error) {
	// Create a mock S3 server
	mockS3 := s3mock.New("test-bucket", "us-east-1")

	return New(
		"test-bucket",
		"us-east-1",
		WithPrefix("test-prefix"),
		WithChunkInterval(1*time.Minute),
		WithBufferSize(1024*1024),
		WithClient(func(ctx context.Context, config s3.Config) (s3.Client, error) {
			return s3.NewMockClient(ctx, mockS3, config)
		}),
	)
}
