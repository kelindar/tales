package tales

import (
	"context"
	"testing"
	"time"

	s3mock "github.com/kelindar/s3/mock"
	"github.com/kelindar/tales/internal/codec"
	"github.com/stretchr/testify/require"
)

func TestQueryMillis(t *testing.T) {
	day := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	from, to := queryMillis(day, day.Add(-time.Hour), day.Add(25*time.Hour))
	require.Equal(t, uint32(0), from)
	require.Equal(t, uint32(86_399_999), to)
}

func TestCompactFallback(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	day := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	writer := testService(t, server, "fallback", "writer", func(c *config) { c.now = func() time.Time { return day } })
	require.NoError(t, writer.Log("one", 1))
	require.NoError(t, writer.Sync(context.Background()))
	require.NoError(t, writer.Close())

	now := day.Add(72 * time.Hour)
	compactor := testService(t, server, "fallback", "compactor", func(c *config) { c.now = func() time.Time { return now } })
	require.NoError(t, compactor.Compact(context.Background(), day))
	meta, ok, err := compactor.compactMetadata(context.Background(), dayKey(day))
	require.NoError(t, err)
	require.True(t, ok)
	meta.Index.ETag, err = compactor.s3Client.Upload(context.Background(), meta.Index.Key, []byte("invalid"))
	require.NoError(t, err)
	meta.Index.Size = int64(len("invalid"))
	meta.Actors[1] = codec.Range{Offset: 0, Size: int64(len("invalid"))}
	encoded, err := codec.Encode(meta)
	require.NoError(t, err)
	_, err = compactor.s3Client.Upload(context.Background(), keyOfCompactMeta(dayKey(day)), encoded)
	require.NoError(t, err)
	require.NoError(t, compactor.Close())

	reader := testService(t, server, "fallback", "reader", func(c *config) { c.now = func() time.Time { return now } })
	defer reader.Close()
	events := collectEvents(t, reader.Query(context.Background(), day.Add(-time.Hour), day.Add(time.Hour), 1))
	require.Equal(t, []string{"one"}, eventTexts(events))
}

func TestQueryTime(t *testing.T) {
	t.Run("inclusive UTC day boundary", func(t *testing.T) {
		server := s3mock.New("events", "us-east-1")
		defer server.Close()
		now := time.Date(2026, 7, 18, 23, 59, 59, 999_000_000, time.UTC)
		service := testService(t, server, "days", "writer", func(c *config) { c.now = func() time.Time { return now } })
		defer service.Close()
		first := now
		require.NoError(t, service.Log("last", 1))
		now = now.Add(time.Millisecond)
		second := now
		require.NoError(t, service.Log("first", 1))
		require.NoError(t, service.Sync(context.Background()))
		events := collectEvents(t, service.Query(context.Background(), first, second, 1))
		require.Equal(t, []string{"last", "first"}, eventTexts(events))
		require.Equal(t, []time.Time{first, second}, []time.Time{events[0].Time(), events[1].Time()})
	})

	t.Run("clock rollback", func(t *testing.T) {
		server := s3mock.New("events", "us-east-1")
		defer server.Close()
		day := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
		now := day.Add(10 * time.Hour)
		service := testService(t, server, "rollback", "writer", func(c *config) { c.now = func() time.Time { return now } })
		defer service.Close()
		require.NoError(t, service.Log("later", 1))
		now = day.Add(9 * time.Hour)
		require.NoError(t, service.Log("earlier", 1))
		require.NoError(t, service.Sync(context.Background()))
		events := collectEvents(t, service.Query(context.Background(), day, day.Add(24*time.Hour-time.Millisecond), 1))
		require.Equal(t, []string{"earlier", "later"}, eventTexts(events))
	})
}

func TestDiscoveryRefresh(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	writerA := testService(t, server, "refresh", "writer-a", func(c *config) { c.now = func() time.Time { return now } })
	defer writerA.Close()
	require.NoError(t, writerA.Log("a", 1))
	require.NoError(t, writerA.Sync(context.Background()))

	reader := testService(t, server, "refresh", "reader", func(c *config) { c.now = func() time.Time { return now } })
	defer reader.Close()
	from, to := now.Add(-time.Hour), now.Add(time.Hour)
	require.Equal(t, []string{"a"}, eventTexts(collectEvents(t, reader.Query(context.Background(), from, to, 1))))

	writerB := testService(t, server, "refresh", "writer-b", func(c *config) { c.now = func() time.Time { return now } })
	defer writerB.Close()
	require.NoError(t, writerB.Log("b", 1))
	require.NoError(t, writerB.Sync(context.Background()))
	require.Equal(t, []string{"a"}, eventTexts(collectEvents(t, reader.Query(context.Background(), from, to, 1))))

	now = now.Add(reader.config.ChunkInterval)
	to = now.Add(time.Hour)
	require.ElementsMatch(t, []string{"a", "b"}, eventTexts(collectEvents(t, reader.Query(context.Background(), from, to, 1))))
}

func TestQueryEdges(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	service := testService(t, server, "query-edges", "writer", func(c *config) { c.now = func() time.Time { return now } })
	defer service.Close()
	require.Empty(t, collectEvents(t, service.Query(context.Background(), now, now, 1)))
	require.NoError(t, service.Log("one", 1, 2))
	require.Equal(t, []string{"one"}, eventTexts(collectEvents(t, service.Query(context.Background(), now, now, 1, 1))))
}
