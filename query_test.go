package tales

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/kelindar/roaring"
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
	events := collectEvents(t, reader.Scan(context.Background(), day.Add(-time.Hour), day.Add(time.Hour), 1))
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
		events := collectEvents(t, service.Scan(context.Background(), first, second, 1))
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
		events := collectEvents(t, service.Scan(context.Background(), day, day.Add(24*time.Hour-time.Millisecond), 1))
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
	require.Equal(t, []string{"a"}, eventTexts(collectEvents(t, reader.Scan(context.Background(), from, to, 1))))

	writerB := testService(t, server, "refresh", "writer-b", func(c *config) { c.now = func() time.Time { return now } })
	defer writerB.Close()
	require.NoError(t, writerB.Log("b", 1))
	require.NoError(t, writerB.Sync(context.Background()))
	require.Equal(t, []string{"a"}, eventTexts(collectEvents(t, reader.Scan(context.Background(), from, to, 1))))

	now = now.Add(reader.config.ChunkInterval)
	to = now.Add(time.Hour)
	require.ElementsMatch(t, []string{"a", "b"}, eventTexts(collectEvents(t, reader.Scan(context.Background(), from, to, 1))))
}

func TestQueryEdges(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	service := testService(t, server, "query-edges", "writer", func(c *config) { c.now = func() time.Time { return now } })
	defer service.Close()
	require.Empty(t, collectEvents(t, service.Scan(context.Background(), now, now, 1)))
	require.NoError(t, service.Log("one", 1, 2))
	require.Equal(t, []string{"one"}, eventTexts(collectEvents(t, service.Scan(context.Background(), now, now, 1, 1))))
}

func TestQueryHelpers(t *testing.T) {
	entry, err := codec.NewLogEntry(10, "hi", []uint32{1, 2})
	require.NoError(t, err)
	require.True(t, containsActors(entry, []uint32{1}))
	require.True(t, containsActors(entry, []uint32{1, 2}))
	require.False(t, containsActors(entry, []uint32{1, 3}))
	require.False(t, containsActors(entry, nil))

	require.Equal(t, uint64(0), compactEntries(&codec.CompactMetadata{}))
	require.Equal(t, uint64(5), compactEntries(&codec.CompactMetadata{
		Sources: []codec.CompactSource{{Base: 2, Entries: 3}},
	}))

	_, err = decodeBitmap([]byte{1, 2, 3}, 1)
	require.Error(t, err)

	bm := roaring.New()
	bm.Set(0)
	bm.Set(5)
	var buf bytes.Buffer
	_, err = bm.WriteTo(&buf)
	require.NoError(t, err)
	_, err = decodeBitmap(buf.Bytes(), 1)
	require.Error(t, err)
	ok, err := decodeBitmap(buf.Bytes(), 10)
	require.NoError(t, err)
	require.True(t, ok.Contains(5))

	day := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	selected := roaring.New()
	selected.Set(0)
	refs, err := collectRaw(entry, 1, day, day, day.Add(time.Hour), []uint32{1}, "0000000000000000", 0, selected)
	require.NoError(t, err)
	require.Len(t, refs, 1)

	refs, err = collectRaw(entry, 1, day, day.Add(time.Hour), day.Add(2*time.Hour), []uint32{1}, "0000000000000000", 0, nil)
	require.NoError(t, err)
	require.Empty(t, refs)
}

func TestPage(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	service := testService(t, server, "pages", "writer", func(c *config) { c.now = func() time.Time { return now } })
	defer service.Close()
	from := now.Add(-time.Hour)
	for _, text := range []string{"one", "two", "three", "four", "five"} {
		now = now.Add(time.Millisecond)
		require.NoError(t, service.Log(text, 1))
	}
	to := now.Add(time.Hour)

	t.Run("empty", func(t *testing.T) {
		events, next, err := service.Page(context.Background(), from.Add(-time.Hour), from.Add(-time.Minute), Zero, 2, 1)
		require.NoError(t, err)
		require.Empty(t, events)
		require.Empty(t, next)
	})
	t.Run("fewer than limit", func(t *testing.T) {
		events, next, err := service.Page(context.Background(), to, from, Zero, 10, 1)
		require.NoError(t, err)
		require.Equal(t, []string{"five", "four", "three", "two", "one"}, eventTexts(events))
		require.Empty(t, next)
	})
	t.Run("exactly limit", func(t *testing.T) {
		events, next, err := service.Page(context.Background(), to, from, Zero, 5, 1)
		require.NoError(t, err)
		require.Len(t, events, 5)
		require.Empty(t, next)
	})
	t.Run("more than limit", func(t *testing.T) {
		events, next, err := service.Page(context.Background(), to, from, Zero, 2, 1)
		require.NoError(t, err)
		require.Equal(t, []string{"five", "four"}, eventTexts(events))
		require.NotEmpty(t, next)
	})
	t.Run("all pages once", func(t *testing.T) {
		var got []Event
		var cursor Cursor
		for {
			events, next, err := service.Page(context.Background(), to, from, cursor, 2, 1)
			require.NoError(t, err)
			got = append(got, events...)
			if next == Zero {
				break
			}
			cursor = next
		}
		require.Equal(t, []string{"five", "four", "three", "two", "one"}, eventTexts(got))
	})
	t.Run("ascending", func(t *testing.T) {
		events, _, err := service.Page(context.Background(), from, to, Zero, 5, 1)
		require.NoError(t, err)
		require.Equal(t, []string{"one", "two", "three", "four", "five"}, eventTexts(events))
	})
}

func TestPageOrdering(t *testing.T) {
	t.Run("days and one writer ties", func(t *testing.T) {
		server := s3mock.New("events", "us-east-1")
		defer server.Close()
		now := time.Date(2026, 7, 18, 23, 59, 59, 999_000_000, time.UTC)
		service := testService(t, server, "page-order", "writer",
			WithBuffer(1),
			func(c *config) { c.now = func() time.Time { return now } },
		)
		defer service.Close()
		require.NoError(t, service.Log("old-day", 1))
		now = now.Add(time.Millisecond)
		require.NoError(t, service.Log("same-1", 1))
		require.NoError(t, service.Log("same-2", 1))
		from, to := now.Add(-time.Hour), now.Add(time.Hour)
		ascending := collectEvents(t, service.Scan(context.Background(), from, to, 1))

		var descending []Event
		var cursor Cursor
		for {
			events, next, err := service.Page(context.Background(), to, from, cursor, 1, 1)
			require.NoError(t, err)
			descending = append(descending, events...)
			if next == Zero {
				break
			}
			cursor = next
		}
		require.Equal(t, []string{"same-2", "same-1", "old-day"}, eventTexts(descending))
		require.Equal(t, eventTexts(descending), eventTexts(collectEvents(t, service.Scan(context.Background(), to, from, 1))))
		for i := range ascending {
			require.Equal(t, ascending[len(ascending)-1-i].Text(), descending[i].Text())
		}
	})

	t.Run("multiple writer ties", func(t *testing.T) {
		server := s3mock.New("events", "us-east-1")
		defer server.Close()
		now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
		for _, writer := range []string{"writer-a", "writer-b"} {
			service := testService(t, server, "page-writers", writer, func(c *config) { c.now = func() time.Time { return now } })
			require.NoError(t, service.Log(writer, 1))
			require.NoError(t, service.Sync(context.Background()))
			require.NoError(t, service.Close())
		}
		reader := testService(t, server, "page-writers", "reader", func(c *config) { c.now = func() time.Time { return now } })
		defer reader.Close()
		from, to := now.Add(-time.Hour), now.Add(time.Hour)
		ascending := collectEvents(t, reader.Scan(context.Background(), from, to, 1))
		first, next, err := reader.Page(context.Background(), to, from, Zero, 1, 1)
		require.NoError(t, err)
		require.NotEmpty(t, next)
		second, done, err := reader.Page(context.Background(), to, from, next, 1, 1)
		require.NoError(t, err)
		require.Empty(t, done)
		require.Equal(t, []string{ascending[1].Text(), ascending[0].Text()}, eventTexts(append(first, second...)))
	})
}

func TestPageActors(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	service := testService(t, server, "page-actors", "writer", func(c *config) { c.now = func() time.Time { return now } })
	defer service.Close()
	for _, event := range []struct {
		text   string
		actors []uint32
	}{
		{"one", []uint32{1}},
		{"both", []uint32{1, 2}},
		{"other", []uint32{2}},
	} {
		now = now.Add(time.Millisecond)
		require.NoError(t, service.Log(event.text, event.actors...))
	}
	from, to := now.Add(-time.Hour), now.Add(time.Hour)
	one, _, err := service.Page(context.Background(), to, from, Zero, 10, 1)
	require.NoError(t, err)
	require.Equal(t, []string{"both", "one"}, eventTexts(one))
	both, _, err := service.Page(context.Background(), to, from, Zero, 10, 1, 2)
	require.NoError(t, err)
	require.Equal(t, []string{"both"}, eventTexts(both))
	unrelated, _, err := service.Page(context.Background(), to, from, Zero, 10, 3)
	require.NoError(t, err)
	require.Empty(t, unrelated)
}

func TestPageCursor(t *testing.T) {
	t.Run("survives compaction", func(t *testing.T) {
		server := s3mock.New("events", "us-east-1")
		defer server.Close()
		old := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
		writer := testService(t, server, "page-compact", "writer",
			WithBuffer(1),
			func(c *config) { c.now = func() time.Time { return old } },
		)
		for _, text := range []string{"one", "two", "three", "four"} {
			require.NoError(t, writer.Log(text, 1))
		}
		require.NoError(t, writer.Sync(context.Background()))
		require.NoError(t, writer.Close())

		now := old.Add(72 * time.Hour)
		service := testService(t, server, "page-compact", "compactor", func(c *config) { c.now = func() time.Time { return now } })
		defer service.Close()
		from, to := old.Add(-time.Hour), old.Add(time.Hour)
		first, next, err := service.Page(context.Background(), to, from, Zero, 2, 1)
		require.NoError(t, err)
		require.NotEmpty(t, next)
		require.NoError(t, service.Compact(context.Background(), old))
		second, done, err := service.Page(context.Background(), to, from, next, 2, 1)
		require.NoError(t, err)
		require.Empty(t, done)
		require.Equal(t, []string{"four", "three", "two", "one"}, eventTexts(append(first, second...)))

		repeated, _, err := service.Page(context.Background(), to, from, next, 2, 1)
		require.NoError(t, err)
		require.Equal(t, eventTexts(second), eventTexts(repeated))
		require.NotContains(t, eventTexts(second), first[len(first)-1].Text())
	})

	t.Run("includes unsynced local events", func(t *testing.T) {
		server := s3mock.New("events", "us-east-1")
		defer server.Close()
		now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
		service := testService(t, server, "page-local", "writer", func(c *config) { c.now = func() time.Time { return now } })
		defer service.Close()
		require.NoError(t, service.Log("one", 1))
		require.NoError(t, service.Log("two", 1))
		first, next, err := service.Page(context.Background(), now.Add(time.Hour), now.Add(-time.Hour), Zero, 1, 1)
		require.NoError(t, err)
		require.Equal(t, []string{"two"}, eventTexts(first))
		require.NoError(t, service.Log("new", 1))
		second, done, err := service.Page(context.Background(), now.Add(time.Hour), now.Add(-time.Hour), next, 1, 1)
		require.NoError(t, err)
		require.Equal(t, []string{"one"}, eventTexts(second))
		require.Empty(t, done)
	})
}

func TestPageValidation(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	service := testService(t, server, "page-validation", "writer", func(c *config) { c.now = func() time.Time { return now } })
	from, to := now.Add(-time.Hour), now.Add(time.Hour)

	_, _, err := service.Page(nil, from, to, Zero, 1, 1)
	require.Error(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = service.Page(ctx, from, to, Zero, 1, 1)
	require.ErrorIs(t, err, context.Canceled)
	for name, call := range map[string]func() error{
		"actors": func() error {
			_, _, err := service.Page(context.Background(), from, to, Zero, 1)
			return err
		},
		"zero limit": func() error {
			_, _, err := service.Page(context.Background(), from, to, Zero, 0, 1)
			return err
		},
		"negative limit": func() error {
			_, _, err := service.Page(context.Background(), from, to, Zero, -1, 1)
			return err
		},
		"large limit": func() error {
			_, _, err := service.Page(context.Background(), from, to, Zero, 1001, 1)
			return err
		},
		"cursor": func() error {
			_, _, err := service.Page(context.Background(), from, to, Cursor("invalid"), 1, 1)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) { require.Error(t, call()) })
	}

	require.NoError(t, service.Log("one", 1))
	require.NoError(t, service.Log("two", 1))
	_, next, err := service.Page(context.Background(), to, from, Zero, 1, 1)
	require.NoError(t, err)
	_, _, err = service.Page(context.Background(), now.Add(-time.Millisecond), from.Add(-time.Hour), next, 1, 1)
	require.Error(t, err)
	require.NoError(t, service.Close())
	_, _, err = service.Page(context.Background(), from, to, Zero, 1, 1)
	require.Error(t, err)

}

func ExampleService_Page() {
	var log *Service
	ctx := context.Background()
	start, now := time.Now().Add(-time.Hour), time.Now()
	actors := []uint32{42}
	var cursor Cursor

	for {
		events, next, err := log.Page(ctx, now, start, cursor, 50, actors...)
		if err != nil {
			return
		}
		for _, event := range events { // Newest first: now toward start.
			_ = event.Bytes()
		}
		if next == Zero {
			break
		}
		cursor = next // Reuse with the same bounds and actors.
	}
}
