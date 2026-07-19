package main

import (
	"time"

	"github.com/kelindar/bench"
	s3mock "github.com/kelindar/s3/mock"
	"github.com/kelindar/tales"
	"github.com/kelindar/tales/internal/s3"
)

const entries = 100_000

func main() {
	appendLogger := newLogger()
	defer appendLogger.Close()

	queryLogger := newLogger()
	defer queryLogger.Close()
	for range entries {
		must(queryLogger.Log("hello world", 1))
	}

	from := time.Now().Add(-time.Hour)
	for range queryLogger.Query(from, time.Now().Add(time.Hour), 1) { // Drain pending writes.
	}
	to := time.Now().Add(time.Hour)
	count := 0
	for range queryLogger.Query(from, to, 1) { // Warm the cache and verify the workload.
		count++
	}
	if count != entries {
		panic("query seed incomplete")
	}

	bench.Run(func(b *bench.B) {
		b.Run("append", func(int) {
			must(appendLogger.Log("hello world", 1))
		})

		b.Run("query", func(int) {
			for range queryLogger.Query(from, to, 1) {
			}
		})
	})
}

func newLogger() *tales.Service {
	server := s3mock.New("bench", "us-east-1")
	logger, err := tales.New("bench", "us-east-1",
		tales.WithBuffer(1_000),
		tales.WithClient(func(config s3.Config) (s3.Client, error) {
			return s3.NewMockClient(server, config)
		}),
	)
	must(err)
	return logger
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
