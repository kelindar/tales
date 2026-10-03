package tales

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kelindar/roaring"
	s3lib "github.com/kelindar/s3"
	s3mock "github.com/kelindar/s3/mock"
	"github.com/kelindar/tales/internal/codec"
	internals3 "github.com/kelindar/tales/internal/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompactionPublication(t *testing.T) {
	for _, writerID := range []string{"first", "late"} {
		t.Run(writerID, func(t *testing.T) {
			server := s3mock.New("events", "us-east-1")
			defer server.Close()
			day := time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC)
			now := day.Add(time.Hour)
			clock := func(c *config) { c.now = func() time.Time { return now } }
			writer := testService(t, server, "publication", "first", clock)
			defer func() { require.NoError(t, writer.Close()) }()
			require.NoError(t, writer.Log("first", 1))
			require.NoError(t, writer.Sync(context.Background()))
			late := writer
			if writerID != "first" {
				late = testService(t, server, "publication", writerID, clock)
				defer func() { require.NoError(t, late.Close()) }()
			}
			require.NoError(t, late.Log("late", 1, 2))
			now = day.Add(72 * time.Hour)
			compactor := testService(t, server, "publication", "compactor", clock)
			defer func() { require.NoError(t, compactor.Close()) }()
			late.s3Client = &compactionClient{Client: late.s3Client, before: func() error {
				return compactor.Compact(context.Background(), day)
			}}
			require.NoError(t, late.Sync(context.Background()))

			reader := testService(t, server, "publication", "reader", clock)
			defer func() { require.NoError(t, reader.Close()) }()
			from, to := day, day.Add(24*time.Hour-time.Millisecond)
			want := []string{"first", "late"}
			if late.config.WriterID < writer.config.WriterID {
				want = []string{"late", "first"}
			}
			assert.Equal(t, want, eventTexts(collectEvents(t, reader.Scan(context.Background(), from, to, 1))))
			assert.Equal(t, []string{"late"}, eventTexts(collectEvents(t, reader.Scan(context.Background(), from, to, 2))))
			for _, descending := range []bool{false, true} {
				start, end := from, to
				expected := want
				if descending {
					start, end, expected = to, from, []string{want[1], want[0]}
				}
				var texts []string
				cursor := Zero
				for page := 0; page < 3; page++ {
					events, next, err := reader.Page(context.Background(), start, end, cursor, 1, 1)
					require.NoError(t, err)
					texts = append(texts, eventTexts(events)...)
					cursor = next
					if next == Zero {
						break
					}
				}
				assert.Equal(t, expected, texts)
				assert.Equal(t, Zero, cursor)
			}
		})
	}
}

type compactionClient struct {
	internals3.Client
	before       func() error
	failMetadata bool
}

func (c *compactionClient) Upload(ctx context.Context, key string, data []byte) (string, error) {
	if c.before != nil && strings.HasSuffix(key, "/manifest.json") {
		before := c.before
		c.before = nil
		if err := before(); err != nil {
			return "", err
		}
	}
	etag, err := c.Client.Upload(ctx, key, data)
	if err == nil && c.failMetadata && strings.HasSuffix(key, "/compact/metadata.json") {
		c.failMetadata = false
		return "", fmt.Errorf("ambiguous compact response")
	}
	return etag, err
}

func TestCompactRetry(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	day := time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC)
	now := day.Add(time.Hour)
	clock := func(c *config) { c.now = func() time.Time { return now } }
	writer := testService(t, server, "compact-retry", "writer", clock)
	defer func() { require.NoError(t, writer.Close()) }()
	require.NoError(t, writer.Log("first", 1))
	require.NoError(t, writer.Sync(context.Background()))
	now = now.Add(time.Millisecond)
	require.NoError(t, writer.Log("late", 1))
	now = day.Add(72 * time.Hour)
	compactor := testService(t, server, "compact-retry", "compactor", clock)
	defer func() { require.NoError(t, compactor.Close()) }()
	compactor.s3Client = &compactionClient{Client: compactor.s3Client, failMetadata: true}
	var publishErr error
	writer.s3Client = &compactionClient{Client: writer.s3Client, before: func() error {
		publishErr = compactor.Compact(context.Background(), day)
		return nil
	}}
	require.NoError(t, writer.Sync(context.Background()))
	require.ErrorContains(t, publishErr, "ambiguous compact response")

	reader := testService(t, server, "compact-retry", "reader", clock)
	defer func() { require.NoError(t, reader.Close()) }()
	from, to := day, day.Add(24*time.Hour-time.Millisecond)
	want := []string{"first", "late"}
	assert.Equal(t, want, eventTexts(collectEvents(t, reader.Scan(context.Background(), from, to, 1))))
	require.NoError(t, compactor.Compact(context.Background(), day))
	assert.Equal(t, want, eventTexts(collectEvents(t, reader.Scan(context.Background(), from, to, 1))))
}

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
