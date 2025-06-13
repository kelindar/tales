package threads

import (
	"context"
	"fmt"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/kelindar/threads/internal/codec"
	"github.com/kelindar/threads/internal/seq"
)

// queryMemory queries the in-memory buffer for entries.
func (l *Service) queryMemory(actor uint32, from, to time.Time, yield func(time.Time, string) bool) {
	ret := make(chan []codec.LogEntry, 1)
	l.commands <- queryCmd{actor: actor, from: from, to: to, ret: ret}

	day := seq.DayOf(from)
	for _, entry := range <-ret {
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

	// Build S3 keys
	tlog := fmt.Sprintf("%s/threads.log", date)
	tidx := fmt.Sprintf("%s/threads.idx", date)
	alog := fmt.Sprintf("%s/actors.log", date)
	aidx := fmt.Sprintf("%s/actors.idx", date)

	// 1. Download and parse index file
	entries, err := l.loadIndexFile(ctx, aidx)
	if err != nil {
		return true // If index file doesn't exist, skip this day
	}

	// 2. Filter index entries by actor and time range
	entries = l.filterEntries(entries, actor, day, from, to)
	if len(entries) == 0 {
		return true
	}

	// 3. Download and merge bitmap chunks
	bm, err := l.mergeBitmaps(ctx, alog, entries)
	if err != nil {
		return true // Skip on error
	}

	if bm.IsEmpty() {
		return true
	}

	// 4. Download metadata file to get chunk information
	metaBytes, err := l.s3Client.Download(ctx, tidx)
	if err != nil {
		return true // If meta file doesn't exist, there's no data for this day.
	}

	meta, err := codec.DecodeMetadata(metaBytes)
	if err != nil {
		return true // Corrupted metadata.
	}

	// 5. Group sequence IDs by log chunks and query
	return l.queryChunks(ctx, tlog, meta, bm, day, from, to, yield)
}

// loadIndexFile downloads and parses the index file.
func (l *Service) loadIndexFile(ctx context.Context, key string) ([]codec.IndexEntry, error) {
	data, err := l.s3Client.Download(ctx, key)
	if err != nil {
		return nil, err
	}

	if len(data)%codec.IndexEntrySize != 0 {
		return nil, fmt.Errorf("invalid index file size")
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
func (l *Service) loadBitmap(ctx context.Context, key string, entry codec.IndexEntry) (*roaring.Bitmap, error) {
	end := int64(entry.Offset() + uint64(entry.CompressedSize()) - 1)
	compressed, err := l.s3Client.DownloadRange(ctx, key, int64(entry.Offset()), end)
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
func (l *Service) mergeBitmaps(ctx context.Context, key string, entries []codec.IndexEntry) (*roaring.Bitmap, error) {
	bm := roaring.New()
	for _, e := range entries {
		bitmap, err := l.loadBitmap(ctx, key, e)
		if err != nil {
			continue // Skip entries that fail to process
		}
		bm.Or(bitmap)
	}

	return bm, nil
}

// queryChunks queries log chunks for specific sequence IDs.
func (l *Service) queryChunks(ctx context.Context, logKey string, meta *codec.Metadata, bm *roaring.Bitmap, day, from, to time.Time, yield func(time.Time, string) bool) bool {
	for _, chunk := range meta.Chunks {
		if !l.queryChunk(ctx, logKey, chunk, bm, day, from, to, yield) {
			return false // Yield returned false, stop.
		}
	}

	return true
}

// loadLogChunk downloads, decompresses, and parses a log chunk.
func (l *Service) loadLogChunk(ctx context.Context, logKey string, chunk codec.ChunkEntry) ([]codec.LogEntry, error) {
	end := int64(chunk.Offset() + uint64(chunk.CompressedSize()) - 1)
	compressed, err := l.s3Client.DownloadRange(ctx, logKey, int64(chunk.Offset()), end)
	if err != nil {
		return nil, fmt.Errorf("failed to download log chunk: %w", err)
	}

	// Decompress chunk and parse log entries
	decompressed, err := l.codec.Decompress(compressed)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress log chunk: %w", err)
	}

	return l.decodeLogEntries(decompressed)
}

// queryChunk queries a specific log chunk for sequence IDs.
func (l *Service) queryChunk(ctx context.Context, logKey string, chunk codec.ChunkEntry, sids *roaring.Bitmap, day, from, to time.Time, yield func(time.Time, string) bool) bool {
	entries, err := l.loadLogChunk(ctx, logKey, chunk)
	if err != nil {
		return true // Skip chunks that fail to process
	}

	// Filter and yield matching entries
	for _, entry := range entries {
		id := entry.ID()
		if !sids.Contains(id) {
			continue
		}

		ts := seq.TimeOf(id, day)
		if ts.After(from) && ts.Before(to) {
			if !yield(ts, entry.Text()) {
				return false // Stop iteration
			}
		}
	}

	return true
}

// decodeLogEntries parses log entries from raw decompressed data.
func (l *Service) decodeLogEntries(data []byte) ([]codec.LogEntry, error) {
	var entries []codec.LogEntry
	buf := data

	for len(buf) > 4 {
		entry := codec.LogEntry(buf)
		size := entry.Size()
		if size == 0 || uint32(len(buf)) < size {
			break // Invalid size or not enough data
		}

		entries = append(entries, entry[:size])
		buf = buf[size:]
	}

	return entries, nil
}
