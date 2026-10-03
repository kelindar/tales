package tales

import (
	"context"
	"fmt"
	"testing"
	"time"

	s3mock "github.com/kelindar/s3/mock"
	"github.com/kelindar/tales/internal/codec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChunkKey(t *testing.T) {
	require.Equal(t, "2026-07-19/writers/0123456789abcdef/manifest.json", keyOfManifest("2026-07-19", "0123456789abcdef"))
	require.Equal(t, "2026-07-19/writers/0123456789abcdef/00000000000000000005.log", keyOfChunk("2026-07-19", "0123456789abcdef", 5))
}

func TestPendingCutoff(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	first := testService(t, server, "cutoff", "writer", func(c *config) { c.now = func() time.Time { return now } })
	require.NoError(t, first.Log("committed", 1))
	require.NoError(t, first.Sync(context.Background()))
	require.NoError(t, first.Close())

	second := testService(t, server, "cutoff", "writer", func(c *config) { c.now = func() time.Time { return now } })
	client := &failingClient{Client: second.s3Client, failManifest: true}
	second.s3Client = client
	defer second.Close()
	require.NoError(t, second.Log("pending", 1))
	events := collectEvents(t, second.Scan(context.Background(), now, now, 1))
	require.Equal(t, []string{"committed", "pending"}, eventTexts(events))
	require.Error(t, second.Sync(context.Background()))
	events = collectEvents(t, second.Scan(context.Background(), now, now, 1))
	require.Equal(t, []string{"committed", "pending"}, eventTexts(events))
	client.failManifest = false
}

func TestSyncCancellation(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	service := testService(t, server, "cancel-sync", "writer", func(c *config) { c.now = func() time.Time { return now } })
	defer service.Close()
	require.NoError(t, service.Log("one", 1))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, service.Sync(ctx), context.Canceled)
	require.NoError(t, service.Sync(context.Background()))
	require.Equal(t, []string{"one"}, eventTexts(collectEvents(t, service.Scan(context.Background(), now, now, 1))))
}

func TestManifestEncoding(t *testing.T) {
	for _, chunks := range []int{0, 1, 1000} {
		t.Run(fmt.Sprint(chunks), func(t *testing.T) {
			manifest := seedManifest(chunks)
			if chunks == 0 {
				manifest.Chunks = nil
			}
			next := seedManifest(chunks + 1)
			next.Chunks[chunks].ETag = "\"etag\\\n\x00<>&\u2028雪\""
			next.Chunks[chunks].Actors = map[uint32]codec.Range{1: {Size: 4}, 10: {Offset: 8, Size: 4}, 2: {Offset: 4, Size: 4}}
			next.Chunks[chunks].BitmapSize = 12
			next.Chunks[chunks].Data.Offset = 12
			next.Chunks[chunks].Size = 13
			require.NoError(t, codec.ValidateManifest(next, next.Day, next.Writer))
			state := &writerState{}
			data, err := state.encodeManifest(manifest, next)
			require.NoError(t, err)
			want, err := codec.Encode(next)
			require.NoError(t, err)
			assert.Equal(t, want, data)
			retained := append([]byte(nil), data...)

			// A retry must produce the same bytes without changing an earlier upload.
			again, err := state.encodeManifest(manifest, next)
			require.NoError(t, err)
			assert.Equal(t, want, again)
			assert.Equal(t, retained, data)

			next.Chunks[chunks].ETag = "different"
			changed, err := state.encodeManifest(manifest, next)
			require.NoError(t, err)
			want, err = codec.Encode(next)
			require.NoError(t, err)
			assert.Equal(t, want, changed)
			assert.Equal(t, retained, data, "later uploads must not mutate retained publication bytes")
		})
	}
}

func TestManifestAllocation(t *testing.T) {
	manifest, next := seedManifest(1000), seedManifest(1001)
	state := &writerState{}
	_, err := state.encodeManifest(manifest, next)
	require.NoError(t, err)
	allocations := testing.AllocsPerRun(20, func() {
		_, err = state.encodeManifest(manifest, next)
	})
	require.NoError(t, err)
	assert.LessOrEqual(t, allocations, float64(32), "committed chunks must not be marshalled again")
}

func BenchmarkManifest(b *testing.B) {
	for _, chunks := range []int{0, 1000} {
		b.Run(fmt.Sprint(chunks), func(b *testing.B) {
			manifest, next := seedManifest(chunks), seedManifest(chunks+1)
			state := &writerState{}
			_, err := state.encodeManifest(manifest, next)
			require.NoError(b, err)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, err = state.encodeManifest(manifest, next)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func seedManifest(chunks int) *codec.Manifest {
	manifest := &codec.Manifest{Day: "2026-07-19", Writer: "0123456789abcdef", Chunks: make([]codec.ChunkEntry, chunks)}
	for i := range manifest.Chunks {
		manifest.Chunks[i] = codec.ChunkEntry{
			Version: 1, Blocks: []codec.Block{{Entries: 1, Size: 1}}, Sequence: codec.Sequence(i),
			Entries: 1, BitmapSize: 4, Data: codec.Range{Offset: 4, Size: 1}, Size: 5, ETag: "etag",
			Actors: map[uint32]codec.Range{1: {Size: 4}},
		}
	}
	return manifest
}
