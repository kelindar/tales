package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/kelindar/bench"
	"github.com/kelindar/roaring"
	s3mock "github.com/kelindar/s3/mock"
	"github.com/kelindar/tales"
	"github.com/kelindar/tales/internal/codec"
	"github.com/kelindar/tales/internal/s3"
)

const (
	entries        = 100_000
	compactionDays = 4_096
)

func main() {
	appendLogger := newLogger()
	defer appendLogger.Close()

	queryLogger := newLogger()
	defer queryLogger.Close()
	for range entries {
		must(queryLogger.Log("hello world", 1))
	}
	must(queryLogger.Sync(context.Background()))

	from := time.Now().Add(-time.Hour)
	for _, err := range queryLogger.Query(context.Background(), from, time.Now().Add(time.Hour), 1) {
		must(err)
	}
	to := time.Now().Add(time.Hour)
	count := 0
	for _, err := range queryLogger.Query(context.Background(), from, to, 1) {
		must(err)
		count++
	}
	if count != entries {
		panic("query seed incomplete")
	}
	compactLogger, firstCompactDay, closeCompaction := newCompactionLogger(compactionDays)
	defer closeCompaction()
	var compacted atomic.Uint32

	bench.Run(func(b *bench.B) {
		b.Run("append", func(int) {
			must(appendLogger.Log("hello world", 1))
		})

		b.Run("query", func(int) {
			for _, err := range queryLogger.Query(context.Background(), from, to, 1) {
				must(err)
			}
		})

		b.Run("compact", func(int) {
			index := int(compacted.Add(1)) - 1
			if index >= compactionDays {
				panic("compaction benchmark exhausted seeded days")
			}
			must(compactLogger.Compact(context.Background(), firstCompactDay.AddDate(0, 0, -index)))
		})
	})
}

func newLogger() *tales.Service {
	server := s3mock.New("bench", "us-east-1")
	logger, err := tales.New("bench", "us-east-1",
		tales.WithClient(func(config s3.Config) (s3.Client, error) {
			return s3.NewMockClient(server, config)
		}),
	)
	must(err)
	return logger
}

func newCompactionLogger(days int) (*tales.Service, time.Time, func()) {
	server := s3mock.New("bench", "us-east-1")
	config := s3.Config{Bucket: "bench", Region: "us-east-1", Prefix: "compact"}
	client, err := s3.NewMockClient(server, config)
	must(err)

	compressor, err := codec.NewCodec()
	must(err)
	entry, err := codec.NewLogEntry(0, "hello world", []uint32{1})
	must(err)
	compressed, err := compressor.Compress(entry)
	must(err)
	compressor.Close()
	bitmap := roaring.New()
	bitmap.Set(0)
	index := bitmap.ToBytes()
	object := append(append([]byte(nil), index...), compressed...)
	firstDay := utcDay(time.Now()).AddDate(0, 0, -2)
	const writer = "0000000000000000"

	for offset := range days {
		day := firstDay.AddDate(0, 0, -offset).Format("2006-01-02")
		chunkKey := fmt.Sprintf("%s/writers/%s/%020d.log", day, writer, 0)
		etag, err := client.Upload(context.Background(), chunkKey, object)
		must(err)
		manifest := &codec.Manifest{Day: day, Writer: writer, Chunks: []codec.ChunkEntry{{
			Sequence: 0, Entries: 1, Time: [2]uint32{0, 0}, BitmapSize: int64(len(index)),
			Data: codec.Range{Offset: int64(len(index)), Size: int64(len(compressed))},
			Size: int64(len(object)), ETag: etag,
			Actors: map[uint32]codec.Range{1: {Offset: 0, Size: int64(len(index))}},
		}}}
		encoded, err := codec.Encode(manifest)
		must(err)
		_, err = client.Upload(context.Background(), fmt.Sprintf("%s/writers/%s/manifest.json", day, writer), encoded)
		must(err)
	}

	logger, err := tales.New("bench", "us-east-1",
		tales.WithPrefix("compact"),
		tales.WithWriterID("compactor"),
		tales.WithClient(func(config s3.Config) (s3.Client, error) {
			return s3.NewMockClient(server, config)
		}),
	)
	must(err)
	return logger, firstDay, func() {
		must(logger.Close())
		server.Close()
	}
}

func utcDay(value time.Time) time.Time {
	year, month, day := value.UTC().Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
