package tales

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/kelindar/roaring"
	s3mock "github.com/kelindar/s3/mock"
	"github.com/kelindar/tales/internal/buffer"
	"github.com/kelindar/tales/internal/codec"
	internals3 "github.com/kelindar/tales/internal/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type measuredClient struct {
	internals3.Client
	bytes    int64
	requests int
}

type payloadClient struct {
	internals3.Client
	data     []byte
	err      error
	short    bool
	requests int
}

func (c *payloadClient) DownloadRange(ctx context.Context, _ string, _ string, offset, size int64) ([]byte, error) {
	c.requests++
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.err != nil {
		return nil, c.err
	}
	data := c.data[offset : offset+size]
	if c.short {
		data = data[:len(data)-1]
	}
	return data, nil
}

func TestPayloadReads(t *testing.T) {
	c, err := codec.NewCodec()
	require.NoError(t, err)
	defer c.Close()
	day := time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC)
	var raw, data []byte
	var blocks []codec.Block
	for i, millis := range []uint32{30, 10, 20, 10} {
		entry, err := codec.NewLogEntry(millis, string(rune('a'+i)), []uint32{1})
		require.NoError(t, err)
		raw = append(raw, entry...)
		frame, err := c.Compress(entry)
		require.NoError(t, err)
		blocks = append(blocks, codec.Block{First: uint32(i), Entries: 1, Offset: int64(len(data)), Size: int64(len(frame))})
		data = append(data, frame...)
	}
	whole, err := c.Compress(raw)
	require.NoError(t, err)
	for mask := 0; mask < 16; mask++ {
		selected := roaring.New()
		for i := uint32(0); i < 4; i++ {
			if mask&(1<<i) != 0 {
				selected.Set(i)
			}
		}
		for _, window := range [][2]uint32{{0, 40}, {10, 20}, {11, 19}} {
			from, to := day.Add(time.Duration(window[0])*time.Millisecond), day.Add(time.Duration(window[1])*time.Millisecond)
			want, err := collectRaw(raw, 4, day, from, to, []uint32{1}, "0000000000000001", 100, selected)
			require.NoError(t, err)
			for _, legacy := range []bool{false, true} {
				encoded, directory := data, blocks
				if legacy {
					encoded, directory = whole, nil
				}
				client := &payloadClient{data: append([]byte{9, 9, 9}, encoded...)}
				service := &Service{codec: c, s3Client: client}
				got, err := service.queryPayload(context.Background(), codec.ObjectRange{Offset: 3, Size: int64(len(encoded))}, directory, 4, day, from, to, []uint32{1}, "0000000000000001", 100, selected)
				require.NoError(t, err)
				// Check both directions, ties, rollback, payload offsets and writer-local positions.
				for _, ascending := range []bool{true, false} {
					sortEventRefs(got, ascending)
					sortEventRefs(want, ascending)
					require.Len(t, got, len(want))
					for i := range want {
						assert.Equal(t, want[i].event.Text(), got[i].event.Text())
						assert.Equal(t, want[i].position, got[i].position)
						assert.Equal(t, want[i].millis, got[i].millis)
					}
				}
				if mask == 0 {
					assert.Zero(t, client.requests)
				}
				if mask == 15 {
					assert.Equal(t, 1, client.requests)
				}
			}
		}
	}

	t.Run("errors", func(t *testing.T) {
		selected := roaring.New()
		selected.Set(0)
		failure := errors.New("range failure")
		for _, scenario := range []string{"range", "short", "corrupt", "malformed", "count", "extra", "canceled"} {
			t.Run(scenario, func(t *testing.T) {
				client := &payloadClient{data: slices.Clone(data)}
				directory := slices.Clone(blocks)
				ctx := context.Background()
				switch scenario {
				case "range":
					client.err = failure
				case "short":
					client.short = true
				case "corrupt":
					client.data[0] ^= 0xff
				case "malformed", "count", "extra":
					input := raw
					if scenario == "malformed" {
						input = []byte{1, 2}
					}
					client.data, err = c.Compress(input)
					require.NoError(t, err)
					count := uint32(5)
					if scenario == "extra" {
						count = 1
					}
					directory = []codec.Block{{Entries: count, Size: int64(len(client.data))}}
				case "canceled":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				service := &Service{codec: c, s3Client: client}
				got, err := service.queryPayload(ctx, codec.ObjectRange{Size: int64(len(client.data))}, directory, 4, day, day, day.Add(time.Second), []uint32{1}, "0000000000000001", 0, selected)
				assert.Error(t, err)
				assert.Empty(t, got)
				switch scenario {
				case "range":
					assert.ErrorIs(t, err, failure)
				case "short":
					assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
				case "canceled":
					assert.ErrorIs(t, err, context.Canceled)
				}
			})
		}
	})
}

func (c *measuredClient) DownloadRange(ctx context.Context, key, etag string, offset, size int64) ([]byte, error) {
	c.bytes += size
	c.requests++
	return c.Client.DownloadRange(ctx, key, etag, offset, size)
}

func TestSelectiveBlocks(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	day := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	writer := testService(t, server, "blocks", "writer", func(c *config) { c.now = func() time.Time { return day } })
	payload := make([]byte, 60_000)
	var expected []string
	var all []string
	for i := 0; i < 120; i++ {
		_, err := rand.Read(payload)
		require.NoError(t, err)
		actors := []uint32{1}
		if i == 0 || i == 119 {
			actors = append(actors, 2)
			expected = append(expected, string(payload))
		}
		require.NoError(t, writer.Log(string(payload), actors...))
		all = append(all, string(payload))
	}
	require.NoError(t, writer.Close())
	clock := day.Add(72 * time.Hour)
	reader := testService(t, server, "blocks", "reader", func(c *config) { c.now = func() time.Time { return clock } })
	defer reader.Close()
	meter := &measuredClient{Client: reader.s3Client}
	reader.s3Client = meter
	for _, stage := range []string{"writer", "compact", "cleaned"} {
		t.Run(stage, func(t *testing.T) {
			if stage != "writer" {
				if stage == "cleaned" {
					clock = clock.Add(25 * time.Hour)
				}
				require.NoError(t, reader.Compact(context.Background(), day))
			}
			meter.bytes, meter.requests = 0, 0
			sparse := collectEvents(t, reader.Scan(context.Background(), day, day, 1, 2))
			assert.Equal(t, expected, eventTexts(sparse))
			sparseBytes := meter.bytes
			assert.Equal(t, 4, meter.requests, "two actor bitmaps and two separated blocks")
			meter.bytes, meter.requests = 0, 0
			dense := collectEvents(t, reader.Scan(context.Background(), day, day, 1))
			assert.Len(t, dense, 120)
			assert.Equal(t, all, eventTexts(dense))
			assert.Equal(t, 2, meter.requests, "one bitmap and one coalesced payload range")
			assert.Less(t, sparseBytes, meter.bytes/10)
			t.Logf("sparse=%d bytes, dense=%d bytes", sparseBytes, meter.bytes)
			first, cursor, err := reader.Page(context.Background(), day, day, Zero, 1, 2)
			require.NoError(t, err)
			require.NotEmpty(t, cursor)
			second, done, err := reader.Page(context.Background(), day, day, cursor, 1, 2)
			require.NoError(t, err)
			assert.Empty(t, done)
			assert.Equal(t, expected, eventTexts(append(first, second...)))

			// Reopen to exercise persisted metadata, not the compactor's cached directory.
			fresh := testService(t, server, "blocks", "fresh", func(c *config) { c.now = func() time.Time { return clock } })
			defer fresh.Close()
			assert.Equal(t, expected, eventTexts(collectEvents(t, fresh.Scan(context.Background(), day, day, 2))))
		})
	}
	assert.False(t, server.ObjectExists("blocks/"+keyOfChunk(dayKey(day), writer.config.WriterID, 0)))
}

func BenchmarkBlockReads(b *testing.B) {
	benchmarkPayload(b, 120, 60_000, true)
}

func BenchmarkSmallBlocks(b *testing.B) {
	benchmarkPayload(b, 12000, 600, false)
}

func benchmarkPayload(b *testing.B, count, size int, random bool) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	service := testService(b, server, "bench-blocks", "reader")
	defer service.Close()
	day := dayOf(time.Now())
	buf := buffer.New(count, service.codec)
	value := make([]byte, size)
	const record = "level=info service=tales action=append actor=123 status=ok "
	for i := range value {
		value[i] = record[i%len(record)]
	}
	for i := 0; i < count; i++ {
		if random {
			_, err := rand.Read(value)
			require.NoError(b, err)
		}
		entry, err := codec.NewLogEntry(uint32(i), string(value), []uint32{1})
		require.NoError(b, err)
		require.NoError(b, buf.Add(day, entry))
	}
	batch, err := buf.Take()
	require.NoError(b, err)
	whole, err := service.codec.Compress(batch.Raw)
	require.NoError(b, err)
	for _, density := range []string{"sparse", "dense", "fragmented"} {
		selected := roaring.New()
		for i := uint32(0); i < batch.Entries; i++ {
			if density == "dense" || density == "sparse" && (i == 0 || i == batch.Entries-1) {
				selected.Set(i)
			}
		}
		if density == "fragmented" {
			for i := 0; i < len(batch.Blocks); i += 2 {
				selected.Set(batch.Blocks[i].First)
			}
		}
		for _, format := range []string{"whole", "blocks"} {
			b.Run(density+"/"+format, func(b *testing.B) {
				data := whole
				var blocks []codec.Block
				if format == "blocks" {
					data, blocks = batch.Data, batch.Blocks
				}
				etag, err := service.s3Client.Upload(context.Background(), "payload", data)
				require.NoError(b, err)
				meter := &measuredClient{Client: service.s3Client}
				service.s3Client = meter
				defer func() { service.s3Client = meter.Client }()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					refs, err := service.queryPayload(context.Background(), codec.ObjectRange{Key: "payload", ETag: etag, Size: int64(len(data))}, blocks, batch.Entries, day, day, day.Add(time.Hour), []uint32{1}, service.config.WriterID, 0, selected)
					if err != nil {
						b.Fatal(err)
					}
					if len(refs) != int(selected.Count()) {
						b.Fatal("wrong result count")
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(meter.bytes)/float64(b.N), "read-B/op")
				b.ReportMetric(float64(meter.requests)/float64(b.N), "ranges/op")
				b.ReportMetric(float64(len(data)), "stored-B")
			})
		}
	}
}
