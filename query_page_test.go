// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package tales

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"testing"
	"time"

	s3mock "github.com/kelindar/s3/mock"
	"github.com/kelindar/tales/internal/codec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const pageCorpusSize = 100_000

type pageTestEvent struct {
	text   string
	millis int
	actors []uint32
}

func writePageEvents(tb testing.TB, service *Service, now *time.Time, day time.Time, events []pageTestEvent) {
	tb.Helper()
	for _, event := range events {
		*now = day.Add(time.Duration(event.millis) * time.Millisecond)
		if err := service.Log(event.text, event.actors...); err != nil {
			tb.Fatal(err)
		}
	}
}

func pageAllEvents(t *testing.T, service *Service, from, to time.Time, limit int, actors ...uint32) []Event {
	t.Helper()
	var events []Event
	var cursor Cursor
	for page := 0; ; page++ {
		if page == 100 {
			t.Fatal("page iteration did not finish")
		}
		current, next, err := service.Page(context.Background(), from, to, cursor, limit, actors...)
		require.NoError(t, err)
		events = append(events, current...)
		if next == Zero {
			return events
		}
		cursor = next
	}
}

func pageCorpus(tb testing.TB, prefix string) (*s3mock.Server, *Service, time.Time) {
	tb.Helper()
	server := s3mock.New("events", "us-east-1")
	server.SetRequestLogging(false)
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	service := testService(tb, server, prefix, "writer",
		WithBuffer(pageCorpusSize+1),
		func(c *config) { c.now = func() time.Time { return now } },
	)
	for range pageCorpusSize {
		if err := service.Log("hello world", 1); err != nil {
			tb.Fatal(err)
		}
	}
	if err := service.Sync(context.Background()); err != nil {
		tb.Fatal(err)
	}
	return server, service, now
}

func TestPageMemory(t *testing.T) {
	server, service, now := pageCorpus(t, "page-memory")
	defer server.Close()
	defer service.Close()
	from, to := now.Add(-time.Hour), now.Add(time.Hour)

	runtime.GC()
	events, _, err := service.Page(context.Background(), from, to, Zero, 50, 1)
	require.NoError(t, err)
	require.Len(t, events, 50)

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	events, _, err = service.Page(context.Background(), from, to, Zero, 50, 1)
	runtime.ReadMemStats(&after)
	require.NoError(t, err)
	require.Len(t, events, 50)
	assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(8<<20))
}

func TestPageParity(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	server.SetRequestLogging(false)
	defer server.Close()
	day := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	times := []int{9, 1, 7, 1, 4, 0, 6, 3, 1, 10, 5, 2, 1, 8, 0, 4, 1, 6, 3, 9, 2, 7, 1, 5}
	actors := [][]uint32{{1, 2}, {1}, {2}, {1, 2}}

	aNow := day
	writerA := testService(t, server, "page-parity", "writer-a", WithBuffer(2), func(c *config) {
		c.now = func() time.Time { return aNow }
	})
	defer func() { require.NoError(t, writerA.Close()) }()
	aEvents := make([]pageTestEvent, len(times))
	for i, millis := range times {
		aEvents[i] = pageTestEvent{text: fmt.Sprintf("a-%02d", i), millis: millis, actors: actors[i%len(actors)]}
	}
	writePageEvents(t, writerA, &aNow, day, aEvents)
	require.NoError(t, writerA.Sync(context.Background()))
	client := &failingClient{Client: writerA.s3Client, failManifest: true}
	writerA.s3Client = client
	writePageEvents(t, writerA, &aNow, day, []pageTestEvent{
		{text: "a-pending-tie", millis: 1, actors: []uint32{1}},
		{text: "a-pending-rollback", millis: 0, actors: []uint32{1, 2}},
	})
	require.Error(t, writerA.Sync(context.Background()))
	client.failManifest = false
	writePageEvents(t, writerA, &aNow, day, []pageTestEvent{
		{text: "a-local-tie", millis: 1, actors: []uint32{1, 2}},
		{text: "a-local-rollback", millis: 0, actors: []uint32{1}},
	})

	bNow := day
	writerB := testService(t, server, "page-parity", "writer-b", WithBuffer(2), func(c *config) {
		c.now = func() time.Time { return bNow }
	})
	defer func() { require.NoError(t, writerB.Close()) }()
	bEvents := make([]pageTestEvent, len(times))
	for i, millis := range times {
		bEvents[i] = pageTestEvent{text: fmt.Sprintf("b-%02d", i), millis: millis, actors: actors[(i+1)%len(actors)]}
	}
	writePageEvents(t, writerB, &bNow, day, bEvents)
	require.NoError(t, writerB.Sync(context.Background()))

	from, to := day.Add(-time.Hour), day.Add(time.Hour)
	for _, selection := range []struct {
		name   string
		actors []uint32
	}{
		{name: "actor", actors: []uint32{1}},
		{name: "actor-and", actors: []uint32{1, 2}},
	} {
		for _, direction := range []struct {
			name     string
			from, to time.Time
		}{
			{name: "ascending", from: from, to: to},
			{name: "descending", from: to, to: from},
		} {
			t.Run(selection.name+"/"+direction.name, func(t *testing.T) {
				want := collectEvents(t, writerA.Scan(context.Background(), direction.from, direction.to, selection.actors...))
				for _, limit := range []int{1, 3, 50} {
					got := pageAllEvents(t, writerA, direction.from, direction.to, limit, selection.actors...)
					assert.Equalf(t, want, got, "page limit %d", limit)
				}
			})
		}
	}
}

func TestPageSelection(t *testing.T) {
	refs := make([]eventRef, 97)
	for i := range refs {
		refs[i] = eventRef{millis: int64(i * 31 % 17), writer: uint64(i * 7 % 3), position: uint64(i)}
	}
	cursorRef := refs[len(refs)/2]
	cursors := []*cursorPosition{
		nil,
		{millis: cursorRef.millis, writer: cursorRef.writer, position: cursorRef.position},
		{millis: 8, writer: 1, position: 20},
	}

	for _, step := range []int{1, 13, 47} {
		input := make([]eventRef, len(refs))
		for i := range input {
			input[i] = refs[(i*step+19)%len(refs)]
		}
		for _, ascending := range []bool{true, false} {
			for _, cursor := range cursors {
				for _, limit := range []int{1, 3, 50} {
					selection := eventSelection{limit: limit, ascending: ascending, position: cursor}
					selection.reserve(len(input))
					for _, ref := range input {
						selection.add(ref)
						assert.LessOrEqual(t, len(selection.refs), limit)
					}

					want := make([]eventRef, 0, min(limit, len(refs)))
					for _, ref := range refs {
						if !skipBeforeCursor(ref, cursor, ascending) {
							want = append(want, ref)
						}
					}
					sortEventRefs(want, ascending)
					if len(want) > limit {
						want = want[:limit]
					}
					got := slices.Clone(selection.refs)
					sortEventRefs(got, ascending)
					assert.Equal(t, want, got)
				}
			}
		}
	}
}

func TestPageFallback(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	day := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	writer := testService(t, server, "page-fallback", "writer", WithBuffer(1), func(c *config) {
		c.now = func() time.Time { return day }
	})
	require.NoError(t, writer.Log("one", 1))
	require.NoError(t, writer.Log("two", 1))
	require.NoError(t, writer.Close())

	reader := testService(t, server, "page-fallback", "compactor", func(c *config) {
		c.now = func() time.Time { return day.Add(72 * time.Hour) }
	})
	defer reader.Close()
	require.NoError(t, reader.Compact(context.Background(), day))
	meta, ok, err := reader.compactMetadata(context.Background(), dayKey(day))
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, meta.Sources, 2)
	// The first source yields a candidate before the second source fails.
	meta.Sources[1].Payload.Key = "missing"
	meta.Sources[1].Copied = true
	require.NoError(t, codec.ValidateCompact(meta, dayKey(day)))

	for _, ascending := range []bool{true, false} {
		reader.compactMeta[dayKey(day)] = meta
		from, to := day.Add(-time.Hour), day.Add(time.Hour)
		want := []string{"one", "two"}
		if !ascending {
			from, to = to, from
			slices.Reverse(want)
		}
		events, next, err := reader.Page(context.Background(), from, to, Zero, 2, 1)
		require.NoError(t, err)
		assert.Equal(t, want, eventTexts(events))
		assert.Equal(t, Zero, next)
	}
}

func TestBlockSelection(t *testing.T) {
	const dayMillis int64 = 1_000
	for _, ascending := range []bool{true, false} {
		for _, writer := range []uint64{1, 2} {
			for _, base := range []uint64{0, 42} {
				block := codec.Block{First: 7, Entries: 4, Time: &[2]uint32{0, 20}}
				refs := make([]eventRef, 4)
				for i, millis := range []int64{10, 0, 20, 0} {
					refs[i] = eventRef{millis: dayMillis + millis, writer: writer, position: base + 7 + uint64(i)}
				}
				cutoffs := append(slices.Clone(refs),
					eventRef{millis: dayMillis, writer: 0},
					eventRef{millis: dayMillis + 20, writer: 3},
				)
				var skipped int
				for _, worst := range cutoffs {
					for _, cursor := range append([]eventRef{{millis: dayMillis - 1}}, cutoffs...) {
						for _, full := range []bool{true, false} {
							selection := eventSelection{limit: 1, ascending: ascending, position: &cursorPosition{
								millis: cursor.millis, writer: cursor.writer, position: cursor.position,
							}}
							if full {
								selection.refs = []eventRef{worst}
							}
							if !selection.skipBlock(block, dayMillis, writer, base) {
								continue
							}
							skipped++
							for _, ref := range refs {
								order := compareEventRefs(ref, worst)
								better := ascending && order < 0 || !ascending && order > 0
								assert.False(t, !skipBeforeCursor(ref, selection.position, ascending) && (!full || better), "pruned a possible page match")
							}
						}
					}
				}
				assert.Positive(t, skipped)
				block.Time = nil
				selection := eventSelection{limit: 1, refs: []eventRef{cutoffs[0]}, ascending: ascending}
				assert.False(t, selection.skipBlock(block, dayMillis, writer, base), "legacy blocks have no pruning bounds")
			}
		}
	}
}

func BenchmarkPage100k(b *testing.B) {
	server, service, now := pageCorpus(b, "bench-page")
	defer server.Close()
	defer service.Close()
	from, to := now.Add(-time.Hour), now.Add(time.Hour)

	for _, direction := range []struct {
		name     string
		from, to time.Time
	}{
		{name: "ascending", from: from, to: to},
		{name: "descending", from: to, to: from},
	} {
		_, _, err := service.Page(context.Background(), direction.from, direction.to, Zero, 50, 1)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(direction.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			var events []Event
			for range b.N {
				var err error
				events, _, err = service.Page(context.Background(), direction.from, direction.to, Zero, 50, 1)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if len(events) != 50 {
				b.Fatalf("got %d events, want 50", len(events))
			}
		})
	}
}

const (
	pruneEventsPerWriter = 2048
	pruneBlockEntries    = 256
	pruneFrameBytes      = 1024
)

func TestPagePrune(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	server.SetRequestLogging(false)
	defer server.Close()

	day := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	now := [2]time.Time{day, day}
	writers := make([]*Service, len(now))
	for i := range writers {
		writers[i] = testService(t, server, "page-prune", fmt.Sprintf("writer-%c", 'a'+i), WithBuffer(pruneEventsPerWriter), func(c *config) {
			c.now = func() time.Time { return now[i] }
		})
		defer writers[i].Close()
	}

	originalBlocks := make(map[string][]codec.Block, len(writers))
	for i, writer := range writers {
		rank := 0
		if writer.config.WriterID > writers[1-i].config.WriterID {
			rank = 1
		}
		base := uint32(rank) * 40_940_000
		events := prunePageEvents(byte('a'+rank), base)
		writePageEvents(t, writer, &now[i], day, events)
		require.NoError(t, writer.Sync(context.Background()))

		manifest, err := writer.downloadManifest(context.Background(), dayKey(day), writer.config.WriterID)
		require.NoError(t, err)
		require.Len(t, manifest.Chunks, 1)
		blocks := manifest.Chunks[0].Blocks
		require.Len(t, blocks, pruneEventsPerWriter/pruneBlockEntries)
		originalBlocks[writer.config.WriterID] = blocks
		for _, block := range blocks {
			require.NotNil(t, block.Time)
			assert.Equal(t, uint32(pruneBlockEntries), block.Entries)
		}
		assert.Equal(t, &[2]uint32{base, base + 5_080_000}, blocks[0].Time)
		assert.Equal(t, &[2]uint32{base + 9_990_000, base + 15_340_000}, blocks[2].Time, "rollback at a block boundary")
	}

	from, to := day, day.Add(24*time.Hour-time.Millisecond)
	compactor := testService(t, server, "page-prune", "compactor", func(c *config) {
		c.now = func() time.Time { return day.Add(72 * time.Hour) }
	})
	defer compactor.Close()
	actors := []uint32{1, 2}
	directions := []struct {
		name     string
		from, to time.Time
	}{
		{name: "ascending", from: from, to: to},
		{name: "descending", from: to, to: from},
	}

	var meta *codec.CompactMetadata
	for _, stage := range []string{"writer", "compact", "legacy"} {
		// Prepare each stage even when -run filters out an earlier subtest.
		switch stage {
		case "compact":
			require.NoError(t, compactor.Compact(context.Background(), day))
			var ok bool
			var err error
			meta, ok, err = compactor.compactMetadata(context.Background(), dayKey(day))
			require.NoError(t, err)
			require.True(t, ok)
			require.Len(t, meta.Sources, len(writers))
			for _, source := range meta.Sources {
				assert.Equal(t, originalBlocks[source.Writer], source.Blocks, "compaction preserves block bounds")
			}
		case "legacy":
			for _, source := range meta.Sources {
				for i := range source.Blocks {
					source.Blocks[i].Time = nil
				}
			}
			data, err := codec.Encode(meta)
			require.NoError(t, err)
			_, err = compactor.s3Client.Upload(context.Background(), keyOfCompactMeta(dayKey(day)), data)
			require.NoError(t, err)
		}

		t.Run(stage, func(t *testing.T) {
			reader := testService(t, server, "page-prune", "reader", func(c *config) {
				c.now = func() time.Time { return day.Add(72 * time.Hour) }
			})
			defer reader.Close()
			meter := &measuredClient{Client: reader.s3Client}
			reader.s3Client = meter
			for _, direction := range directions {
				t.Run(direction.name, func(t *testing.T) {
					paged := pageAllEvents(t, reader, direction.from, direction.to, 7, actors...)
					meter.bytes, meter.requests = 0, 0
					want := collectEvents(t, reader.Scan(context.Background(), direction.from, direction.to, actors...))
					scanBytes, scanRanges := meter.bytes, meter.requests
					assert.Equal(t, want, paged)
					if stage == "legacy" {
						return
					}

					for _, offset := range []int{0, len(want) * 3 / 4} {
						cursor := pagePruneCursorAfter(t, reader, direction.from, direction.to, actors, offset, 17)
						meter.bytes, meter.requests = 0, 0
						events, next, err := reader.Page(context.Background(), direction.from, direction.to, cursor, 5, actors...)
						require.NoError(t, err)
						assert.NotEqual(t, Zero, next)
						assert.Equalf(t, want[offset:offset+5], events, "cursor offset %d", offset)
						assert.Less(t, meter.bytes, scanBytes, "a small page should read fewer payload bytes than Scan")
						assert.Less(t, meter.requests, scanRanges, "a small page should read fewer ranges than Scan")
					}
				})
			}
		})
	}
}

func BenchmarkPageReads(b *testing.B) {
	server := s3mock.New("events", "us-east-1")
	server.SetRequestLogging(false)
	defer server.Close()

	day := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	const eventsPerWriter = 5_000
	blockCount := 0
	for i := range 2 {
		now := day
		writer := testService(b, server, "bench-page-reads", fmt.Sprintf("writer-%c", 'a'+i), WithBuffer(eventsPerWriter), func(c *config) {
			c.now = func() time.Time { return now }
		})
		defer writer.Close()
		tag := byte('a' + i)
		events := make([]pageTestEvent, eventsPerWriter)
		for ordinal := range events {
			millis := ordinal * 17_000
			actors := []uint32{1}
			if ordinal%31 == 0 {
				actors = []uint32{1, 2}
			}
			events[ordinal] = pageTestEvent{
				text:   pagePruneText(tag, ordinal, len(actors), 600),
				millis: millis,
				actors: actors,
			}
		}
		writePageEvents(b, writer, &now, day, events)
		require.NoError(b, writer.Sync(context.Background()))
		manifest, err := writer.downloadManifest(context.Background(), dayKey(day), writer.config.WriterID)
		require.NoError(b, err)
		require.Len(b, manifest.Chunks, 1)
		blockCount += len(manifest.Chunks[0].Blocks)
	}
	require.GreaterOrEqual(b, blockCount, 24)

	reader := testService(b, server, "bench-page-reads", "reader", func(c *config) {
		c.now = func() time.Time { return day.Add(72 * time.Hour) }
	})
	defer reader.Close()
	meter := &measuredClient{Client: reader.s3Client}
	reader.s3Client = meter
	from, to := day, day.Add(24*time.Hour-time.Millisecond)
	queries := []struct {
		name   string
		actors []uint32
	}{
		{name: "dense", actors: []uint32{1}},
		{name: "sparse-and", actors: []uint32{1, 2}},
	}
	directions := []struct {
		name     string
		from, to time.Time
	}{
		{name: "ascending", from: from, to: to},
		{name: "descending", from: to, to: from},
	}

	for _, query := range queries {
		for _, direction := range directions {
			scanEvents := collectEvents(b, reader.Scan(context.Background(), direction.from, direction.to, query.actors...))
			cursor := pagePruneCursorAfter(b, reader, direction.from, direction.to, query.actors, len(scanEvents)*3/4, 1_000)
			for _, position := range []struct {
				name   string
				cursor Cursor
			}{
				{name: "first", cursor: Zero},
				{name: "deep", cursor: cursor},
			} {
				_, _, err := reader.Page(context.Background(), direction.from, direction.to, position.cursor, 50, query.actors...)
				require.NoError(b, err)
				b.Run(fmt.Sprintf("%s/%s/%s", query.name, direction.name, position.name), func(b *testing.B) {
					var bytesRead, ranges int64
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						meter.bytes, meter.requests = 0, 0
						_, _, err := reader.Page(context.Background(), direction.from, direction.to, position.cursor, 50, query.actors...)
						if err != nil {
							b.Fatal(err)
						}
						bytesRead += meter.bytes
						ranges += int64(meter.requests)
					}
					b.StopTimer()
					if b.N > 0 {
						b.ReportMetric(float64(bytesRead)/float64(b.N), "read-B/op")
						b.ReportMetric(float64(ranges)/float64(b.N), "ranges/op")
					}
				})
			}
		}
	}
}

func prunePageEvents(tag byte, base uint32) []pageTestEvent {
	events := make([]pageTestEvent, pruneEventsPerWriter)
	for ordinal := range events {
		offset := uint32(ordinal * 20_000)
		switch {
		case ordinal == 255 || ordinal == 256:
			offset = 5_080_000
		case ordinal > 0 && ordinal%512 == 0:
			offset -= 250_000
		}
		actors := []uint32{1}
		if ordinal/pruneBlockEntries%2 == 0 && ordinal%31 == 0 {
			actors = []uint32{1, 2}
		}
		events[ordinal] = pageTestEvent{
			text:   pagePruneText(tag, ordinal, len(actors), pruneFrameBytes),
			millis: int(base + offset),
			actors: actors,
		}
	}
	return events
}

func pagePruneText(tag byte, ordinal, actors, frameBytes int) string {
	textBytes := frameBytes - 8 - actors*4
	prefix := fmt.Sprintf("%c%04d|level=info service=tales action=append actor=1 payload=", tag, ordinal)
	return pagePruneFill(prefix, textBytes, uint64(ordinal+1)^uint64(tag)<<48)
}

func pagePruneFill(prefix string, size int, state uint64) string {
	text := make([]byte, size)
	copy(text, prefix)
	for i := len(prefix); i < len(text); i++ {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		text[i] = "abcdefghijklmnopqrstuvwxyz0123456789"[state%36]
	}
	return string(text)
}

func pagePruneCursorAfter(tb testing.TB, service *Service, from, to time.Time, actors []uint32, count, pageSize int) Cursor {
	tb.Helper()
	if count == 0 {
		return Zero
	}
	var cursor Cursor
	for skipped := 0; skipped < count; {
		limit := min(pageSize, count-skipped)
		events, next, err := service.Page(context.Background(), from, to, cursor, limit, actors...)
		require.NoError(tb, err)
		require.Len(tb, events, limit)
		skipped += len(events)
		cursor = next
	}
	require.NotEqual(tb, Zero, cursor, "cursor target must have later events")
	return cursor
}
