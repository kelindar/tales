package tales

import (
	"context"
	"fmt"
	"iter"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/kelindar/tales/internal/codec"
	"github.com/kelindar/tales/internal/seq"
)

// queryWarm queries the in-memory buffer for entries.
func (l *Service) queryWarm(actors []uint32, from, to time.Time, yield func(time.Time, string) bool) {
	ret := make(chan iter.Seq[codec.LogEntry], 1)
	l.commands <- command{query: &queryCmd{actors: actors, from: from, to: to, ret: ret}}

	day := seq.DayOf(from)
	for entry := range <-ret {
		if !yield(entry.Time(day), entry.Text()) {
			return
		}
	}
}

// queryCold implements the S3 historical query logic.
func (l *Service) queryCold(ctx context.Context, actors []uint32, from, to time.Time, yield func(time.Time, string) bool) {
	t0 := seq.DayOf(from)
	t1 := seq.DayOf(to).Add(24 * time.Hour)
	for ; t0.Before(t1); t0 = t0.Add(24 * time.Hour) {
		if !l.queryDay(ctx, actors, t0, from, to, yield) {
			return // yield returned false, stop iteration
		}
	}
}

// queryDay queries S3 data for a specific day.
func (l *Service) queryDay(ctx context.Context, actors []uint32, day time.Time, from, to time.Time, yield func(time.Time, string) bool) bool {
	if len(actors) == 0 {
		return true
	}

	// Retrieve metadata from cache or S3
	meta, err := l.downloadMetadata(ctx, day)
	if err != nil {
		return true
	}

	// Pre-compute time range in minutes once per day
	fromMin := uint32(from.Sub(day).Minutes())
	toMin := uint32(to.Sub(day).Minutes())

	// Build a quick membership map for actors
	actorSet := make(map[uint32]struct{}, len(actors))
	for _, a := range actors {
		actorSet[a] = struct{}{}
	}

	// For each chunk, load all relevant bitmaps and compute intersection
	for _, chunk := range meta.Chunks {
		chunkKey := keyOfChunk(seq.FormatDate(day), chunk.Offset)

		// Process each actor directly, building intersection as we go
		var index *roaring.Bitmap
		for i, a := range actors {
			idx, ok := chunk.Actors[a]
			if !ok || idx.Time < fromMin || idx.Time > toMin {
				index = nil
				break
			}

			bitmap, err := l.loadBitmap(ctx, chunkKey, idx)
			if err != nil {
				index = nil
				break
			}

			switch {
			case i == 0:
				index = bitmap.Clone()
			case index != nil:
				index.And(bitmap)
			}
		}

		// Query log section with intersection bitmap
		if index != nil && !index.IsEmpty() {
			if !l.queryChunk(ctx, chunkKey, chunk, index, day, from, to, yield) {
				return false
			}
		}
	}

	return true
}

// loadBitmap downloads and decodes a single bitmap for a given index entry.
func (l *Service) loadBitmap(ctx context.Context, key string, entry codec.IndexEntry) (*roaring.Bitmap, error) {
	i0 := int64(entry.Offset)
	i1 := i0 + int64(entry.Size) - 1

	compressed, err := l.s3Client.DownloadRange(ctx, key, i0, i1)
	if err != nil {
		return nil, fmt.Errorf("failed to download bitmap chunk: %w", err)
	}

	// Decompress and deserialize bitmap
	buffer, err := l.codec.Decompress(compressed)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress bitmap: %w", err)
	}

	output := roaring.New()
	if _, err := output.FromBuffer(buffer); err != nil {
		return nil, fmt.Errorf("failed to deserialize bitmap: %w", err)
	}

	return output, nil
}

// queryChunk queries a specific log chunk for sequence IDs.
func (l *Service) queryChunk(ctx context.Context, chunkKey string, chunk codec.ChunkEntry, sids *roaring.Bitmap, day, from, to time.Time, yield func(time.Time, string) bool) bool {
	entries, err := l.rangeChunks(ctx, chunkKey, chunk)
	if err != nil {
		return true // Skip chunks that fail to process
	}

	// Filter and yield matching entries
	for entry := range entries {
		id := entry.ID()
		if !sids.Contains(id) {
			continue
		}

		ts := seq.TimeOf(id, day)
		if !ts.Before(from) && !ts.After(to) && !yield(ts, entry.Text()) {
			return false // Stop iteration
		}
	}

	return true
}

// rangeChunks downloads the log section from a chunk file, decompresses it, and returns an iterator over log entries.
func (l *Service) rangeChunks(ctx context.Context, chunkKey string, chunk codec.ChunkEntry) (iter.Seq[codec.LogEntry], error) {
	// Calculate log section offset and download only that section
	logOffset := int64(chunk.LogOffset())
	logSize := chunk.LogSize
	if logSize == 0 {
		return func(yield func(codec.LogEntry) bool) {}, nil // Empty iterator
	}

	logEnd := logOffset + int64(logSize) - 1
	compressed, err := l.s3Client.DownloadRange(ctx, chunkKey, logOffset, logEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to download log section: %w", err)
	}

	// Decompress chunk and parse log entries
	buffer, err := l.codec.Decompress(compressed)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress log section: %w", err)
	}

	return func(yield func(codec.LogEntry) bool) {
		for len(buffer) > 4 {
			entry := codec.LogEntry(buffer)
			size := entry.Size()
			if size == 0 || uint32(len(buffer)) < size {
				return // Invalid size or not enough data, stop iteration
			}

			if !yield(entry[:size]) {
				return // Stop iteration if yield returns false
			}
			buffer = buffer[size:]
		}
	}, nil
}
