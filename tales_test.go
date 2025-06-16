package tales

import (
	"testing"
	"time"

	s3mock "github.com/kelindar/s3/mock"
	"github.com/kelindar/tales/internal/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/*
cpu: 13th Gen Intel(R) Core(TM) i7-13700K
BenchmarkLog-24    	 4461338	       228.8 ns/op	   4395903 tps	     831 B/op	       3 allocs/op
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
BenchmarkQuery-24    	      60	  28469333 ns/op	        57.43 qps	360792949 B/op	   58913 allocs/op
BenchmarkQuery-24    	      90	  20760861 ns/op	        85.35 qps	245520819 B/op	   51301 allocs/op
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
