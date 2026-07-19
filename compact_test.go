package tales

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/kelindar/roaring"
	s3lib "github.com/kelindar/s3"
	s3mock "github.com/kelindar/s3/mock"
	"github.com/kelindar/tales/internal/codec"
	"github.com/stretchr/testify/require"
)

func TestCompactKeys(t *testing.T) {
	require.Equal(t, "2026-07-19/compact/index.bin", keyOfCompactIndex("2026-07-19"))
	require.Equal(t, "2026-07-19/compact/data.log", keyOfCompactData("2026-07-19"))
	require.Equal(t, "2026-07-19/compact/metadata.json", keyOfCompactMeta("2026-07-19"))
}

func TestCompactEdges(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	service := testService(t, server, "compact-edges", "compactor", func(c *config) { c.now = func() time.Time { return now } })
	defer service.Close()
	day := now.AddDate(0, 0, -2)

	require.Error(t, service.Compact(nil, day))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, service.Compact(ctx, day), context.Canceled)
	require.Error(t, service.Compact(context.Background(), now))
	require.Error(t, service.Compact(context.Background(), now.AddDate(0, 0, -1)))
	require.NoError(t, service.Compact(context.Background(), day))
	require.False(t, server.ObjectExists("compact-edges/"+keyOfCompactMeta(dayKey(day))))
}

func TestShiftBitmap(t *testing.T) {
	src := roaring.New()
	src.Set(1)
	dst := roaring.New()
	require.NoError(t, shiftBitmap(dst, src, 10))
	require.True(t, dst.Contains(11))

	overflow := roaring.New()
	overflow.Set(1)
	require.Error(t, shiftBitmap(roaring.New(), overflow, math.MaxUint32))
}

func TestBuildCompactSources(t *testing.T) {
	chunks := []sourceChunk{{
		writer: "aaaa000000000000",
		chunk: codec.ChunkEntry{
			Sequence: 0, Entries: 2, Time: [2]uint32{0, 1}, ETag: "etag",
			Data: codec.Range{Offset: 4, Size: s3lib.MinPartSize},
		},
		key:  "chunk-0.log",
		base: 0,
	}, {
		writer: "aaaa000000000000",
		chunk: codec.ChunkEntry{
			Sequence: 1, Entries: 1, Time: [2]uint32{1, 2}, ETag: "etag",
			Data: codec.Range{Offset: 4, Size: 10},
		},
		key:  "chunk-1.log",
		base: 2,
	}}
	sources, parts, copied := buildCompactSources(chunks, 10)
	require.Len(t, sources, 2)
	require.Len(t, parts, 1)
	require.Equal(t, []int{0}, copied)
	require.True(t, sources[0].Copied == false)
	require.Equal(t, "chunk-0.log", sources[0].Source)
}
