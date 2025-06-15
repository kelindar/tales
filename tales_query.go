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

// queryMemory queries the in-memory buffer for entries.
func (l *Service) queryMemory(actor uint32, from, to time.Time, yield func(time.Time, string) bool) {
	ret := make(chan iter.Seq[codec.LogEntry], 1)
	l.commands <- command{query: &queryCmd{actor: actor, from: from, to: to, ret: ret}}

	day := seq.DayOf(from)
	for entry := range <-ret {
		if !yield(entry.Time(day), entry.Text()) {
			return
		}
	}
}

// queryHistory implements the S3 historical query logic.
func (l *Service) queryHistory(ctx context.Context, actor uint32, from, to time.Time, yield func(time.Time, string) bool) {
	// Query each day in the time range
	current := seq.DayOf(from)
	end := seq.DayOf(to).Add(24 * time.Hour)

	for current.Before(end) {
		if !l.queryDay(ctx, actor, current, from, to, yield) {
			return // yield returned false, stop iteration
		}
		current = current.Add(24 * time.Hour)
	}
}

// queryDay queries S3 data for a specific day.
func (l *Service) queryDay(ctx context.Context, actor uint32, day time.Time, from, to time.Time, yield func(time.Time, string) bool) bool {
	date := seq.FormatDate(day)

	// Build S3 key for metadata
	tidx := buildMetadataKey(date)

	// Download metadata file
	metaBytes, err := l.s3Client.Download(ctx, tidx)
	if err != nil || len(metaBytes) == 0 {
		return true
	}

	meta, err := codec.DecodeMetadata(metaBytes)
	if err != nil || meta == nil {
		return true
	}

	// For each chunk, load its sections from the merged file
	for _, chunk := range meta.Chunks {
		chunkKey := buildChunkKey(date, chunk.Offset())

		// Load index section
		entries, err := l.loadIndex(ctx, chunkKey, chunk)
		if err != nil {
			continue
		}

		entries = l.filterEntries(entries, actor, day, from, to)
		if len(entries) == 0 {
			continue
		}

		// Load and merge bitmaps
		bm, err := l.mergeBitmaps(ctx, chunkKey, chunk, entries)
		if err != nil || bm.IsEmpty() {
			continue
		}

		// Query log section
		if !l.queryChunk(ctx, chunkKey, chunk, bm, day, from, to, yield) {
			return false
		}
	}

	return true
}

// loadIndex downloads and parses the index section from a chunk file.
func (l *Service) loadIndex(ctx context.Context, key string, chunk codec.ChunkEntry) ([]codec.IndexEntry, error) {
	indexSize := chunk.IndexSize()
	if indexSize == 0 {
		return nil, nil
	}

	// Download only the index section (from offset 0 to indexSize-1)
	data, err := l.s3Client.DownloadRange(ctx, key, 0, int64(indexSize-1))
	if err != nil {
		return nil, err
	}

	if len(data)%codec.IndexEntrySize != 0 {
		return nil, fmt.Errorf("invalid index section size")
	}

	count := len(data) / codec.IndexEntrySize
	entries := make([]codec.IndexEntry, count)
	for i := 0; i < count; i++ {
		start := i * codec.IndexEntrySize
		entries[i] = codec.IndexEntry(data[start : start+codec.IndexEntrySize])
	}

	return entries, nil
}

// filterEntries filters index entries by actor and time range.
func (l *Service) filterEntries(entries []codec.IndexEntry, actor uint32, day, from, to time.Time) []codec.IndexEntry {
	out := make([]codec.IndexEntry, 0, len(entries))
	fromMin := uint32(from.Sub(day).Minutes())
	toMin := uint32(to.Sub(day).Minutes())

	for _, e := range entries {
		if e.Actor() == actor && e.Time() >= fromMin && e.Time() <= toMin {
			out = append(out, e)
		}
	}

	return out
}

// loadBitmap downloads and decodes a single bitmap for a given index entry.
func (l *Service) loadBitmap(ctx context.Context, key string, chunk codec.ChunkEntry, entry codec.IndexEntry) (*roaring.Bitmap, error) {
	// Calculate absolute offset within the merged file (bitmap section starts after index section)
	bitmapSectionStart := int64(chunk.BitmapOffset())
	absoluteStart := bitmapSectionStart + int64(entry.Offset())
	absoluteEnd := absoluteStart + int64(entry.CompressedSize()) - 1

	compressed, err := l.s3Client.DownloadRange(ctx, key, absoluteStart, absoluteEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to download bitmap chunk: %w", err)
	}

	// Decompress and deserialize bitmap
	decompressed, err := l.codec.Decompress(compressed)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress bitmap: %w", err)
	}

	bm := roaring.New()
	if _, err := bm.FromBuffer(decompressed); err != nil {
		return nil, fmt.Errorf("failed to deserialize bitmap: %w", err)
	}

	return bm, nil
}

// mergeBitmaps downloads bitmap chunks and merges them.
func (l *Service) mergeBitmaps(ctx context.Context, key string, chunk codec.ChunkEntry, entries []codec.IndexEntry) (*roaring.Bitmap, error) {
	bm := roaring.New()
	for _, e := range entries {
		bitmap, err := l.loadBitmap(ctx, key, chunk, e)
		if err != nil {
			continue // Skip entries that fail to process
		}
		bm.Or(bitmap)
	}
	return bm, nil
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
	logSize := chunk.LogSize()
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
