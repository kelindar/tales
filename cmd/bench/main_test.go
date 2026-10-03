package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/kelindar/s3/mock"
	"github.com/kelindar/tales"
	"github.com/kelindar/tales/internal/codec"
	"github.com/kelindar/tales/internal/s3"
	"github.com/stretchr/testify/require"
)

const (
	syncPrefix = "sync-bench"
	syncWriter = "sync-benchmark"
	syncBuffer = 1024
	syncText   = "hello world"
)

// Run with: go test -run '^$' -bench '^BenchmarkSync' -benchmem -benchtime=100x.
func BenchmarkSync(b *testing.B) {
	for _, seedHistory := range []int{0, 1000} {
		for _, batch := range []int{1, 100} {
			b.Run(fmt.Sprintf("history=%d/batch=%d", seedHistory+1, batch), func(b *testing.B) {
				b.StopTimer()
				server := mock.New("bench", "us-east-1")
				server.SetRequestLogging(false)
				defer server.Close()

				fixture := newSyncFixture(b, server, seedHistory)
				b.ReportAllocs()
				b.ReportMetric(float64(batch), "events/sync")
				for range b.N {
					fixture.reset(b)
					logger := newSyncLogger(b, server)
					require.NoError(b, logger.Log(syncText, 1))
					require.NoError(b, logger.Sync(context.Background()))

					b.StartTimer()
					err := syncBatch(logger, batch)
					b.StopTimer()
					require.NoError(b, err)
					require.NoError(b, logger.Close())
				}
			})
		}
	}
}

type syncFixture struct {
	server       *mock.Server
	history      int
	day          string
	writer       string
	chunk        codec.ChunkEntry
	chunkData    []byte
	manifestKey  string
	manifestData []byte
}

func newSyncFixture(b *testing.B, server *mock.Server, history int) *syncFixture {
	fixture := &syncFixture{server: server, history: history}
	logger := newSyncLogger(b, server)
	require.NoError(b, logger.Log(syncText, 1))
	require.NoError(b, logger.Sync(context.Background()))
	require.NoError(b, logger.Close())

	var manifestKey string
	for _, key := range server.ListObjects(syncPrefix + "/") {
		if strings.HasSuffix(key, "/manifest.json") {
			manifestKey = key
			break
		}
	}
	require.NotEmpty(b, manifestKey)
	manifestObject, ok := server.GetObject(manifestKey)
	require.True(b, ok)
	manifest, err := codec.Decode[codec.Manifest](manifestObject.Content)
	require.NoError(b, err)
	require.Len(b, manifest.Chunks, 1)

	fixture.writer = manifest.Writer
	fixture.chunk = manifest.Chunks[0]
	fixture.day = manifest.Day
	chunkKey := syncChunkKey(fixture.day, fixture.writer, 0)
	chunkObject, ok := server.GetObject(chunkKey)
	require.True(b, ok)
	fixture.chunkData = bytes.Clone(chunkObject.Content)
	fixture.manifestKey = manifestKey
	if history == 0 {
		server.Clear()
		return fixture
	}

	fixture.seedHistory(b)
	return fixture
}

func (f *syncFixture) reset(b *testing.B) {
	require.Equal(b, f.day, utcDay(time.Now()).Format("2006-01-02"), "benchmark crossed a UTC day; rerun with a fresh fixture")
	if f.history == 0 {
		f.server.DeleteObject(f.manifestKey)
		return
	}
	f.server.PutObject(f.manifestKey, f.manifestData)
}

func (f *syncFixture) seedHistory(b *testing.B) {
	manifest := &codec.Manifest{Day: f.day, Writer: f.writer, Chunks: make([]codec.ChunkEntry, f.history)}
	for sequence := range f.history {
		chunk := f.chunk
		chunk.Sequence = codec.Sequence(sequence)
		chunk.ETag = f.server.PutObject(syncChunkKey(f.day, f.writer, uint64(sequence)), f.chunkData)
		manifest.Chunks[sequence] = chunk
	}
	require.NoError(b, codec.ValidateManifest(manifest, f.day, f.writer))
	data, err := codec.Encode(manifest)
	require.NoError(b, err)
	f.manifestData = data
	f.server.PutObject(f.manifestKey, data)
}

func newSyncLogger(b *testing.B, server *mock.Server) *tales.Service {
	logger, err := tales.New("bench", "us-east-1",
		tales.WithPrefix(syncPrefix),
		tales.WithWriterID(syncWriter),
		tales.WithBuffer(syncBuffer),
		tales.WithClient(func(config s3.Config) (s3.Client, error) {
			return s3.NewMockClient(server, config)
		}),
	)
	require.NoError(b, err)
	return logger
}

func syncBatch(logger *tales.Service, batch int) error {
	for range batch {
		if err := logger.Log(syncText, 1); err != nil {
			return err
		}
	}
	return logger.Sync(context.Background())
}

func syncChunkKey(day, writer string, sequence uint64) string {
	return fmt.Sprintf("%s/%s/writers/%s/%020d.log", syncPrefix, day, writer, sequence)
}

func TestMust(t *testing.T) {
	require.NotPanics(t, func() { must(nil) })
	require.PanicsWithValue(t, errString("boom"), func() { must(errString("boom")) })
}

type errString string

func (e errString) Error() string { return string(e) }
