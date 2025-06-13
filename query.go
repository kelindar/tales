package threads

import (
	"context"
	"fmt"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/kelindar/threads/internal/codec"
)

// queryS3Historical implements the S3 historical query logic.
func (l *Logger) queryS3Historical(ctx context.Context, actor uint32, from, to time.Time, yield func(time.Time, string) bool) {
	// Query each day in the time range
	current := dayOf(from)
	end := dayOf(to).Add(24 * time.Hour)

	for current.Before(end) {
		if !l.queryS3Day(ctx, actor, current, from, to, yield) {
			return // yield returned false, stop iteration
		}
		current = current.Add(24 * time.Hour)
	}
}

// queryS3Day queries S3 data for a specific day.
func (l *Logger) queryS3Day(ctx context.Context, actor uint32, dayStart time.Time, from, to time.Time, yield func(time.Time, string) bool) bool {
	dateString := formatDate(dayStart)

	// Build S3 keys
	logKey := fmt.Sprintf("%s/threads.log", dateString)
	metaKey := fmt.Sprintf("%s/threads.meta", dateString)
	bitmapKey := fmt.Sprintf("%s/threads.rbm", dateString)
	indexKey := fmt.Sprintf("%s/threads.idx", dateString)

	// 1. Download and parse index file
	indexEntries, err := l.downloadIndexEntries(ctx, indexKey)
	if err != nil {
		// If index file doesn't exist, skip this day
		return true
	}

	// 2. Filter index entries by actor and time range
	relevantEntries := l.filterIndexEntries(indexEntries, actor, dayStart, from, to)
	if len(relevantEntries) == 0 {
		return true
	}

	// 3. Download and merge bitmap chunks
	mergedBitmap, err := l.downloadAndMergeBitmaps(ctx, bitmapKey, relevantEntries)
	if err != nil {
		return true // Skip on error
	}

	if mergedBitmap.IsEmpty() {
		return true
	}

	// 4. Download metadata file to get chunk information
	metaBytes, err := l.s3Client.DownloadData(ctx, metaKey)
	if err != nil {
		return true // If meta file doesn't exist, there's no data for this day.
	}

	tailMetadata, err := codec.DecodeMetadata(metaBytes)
	if err != nil {
		return true // Corrupted metadata.
	}

	// 5. Group sequence IDs by log chunks and query
	return l.queryLogChunks(ctx, logKey, tailMetadata, mergedBitmap, dayStart, from, to, yield)
}

// downloadIndexEntries downloads and parses the index file.
func (l *Logger) downloadIndexEntries(ctx context.Context, key string) ([]codec.IndexEntry, error) {
	data, err := l.s3Client.DownloadData(ctx, key)
	if err != nil {
		return nil, err
	}

	if len(data)%codec.IndexEntrySize != 0 {
		return nil, fmt.Errorf("invalid index file size")
	}

	entryCount := len(data) / codec.IndexEntrySize
	entries := make([]codec.IndexEntry, entryCount)

	for i := 0; i < entryCount; i++ {
		start := i * codec.IndexEntrySize
		end := start + codec.IndexEntrySize
		entries[i] = codec.IndexEntry(data[start:end])
	}

	return entries, nil
}

// filterIndexEntries filters index entries by actor and time range.
func (l *Logger) filterIndexEntries(entries []codec.IndexEntry, actor uint32, dayStart, from, to time.Time) []codec.IndexEntry {
	var filtered []codec.IndexEntry

	fromMinutes := uint32(from.Sub(dayStart).Minutes())
	toMinutes := uint32(to.Sub(dayStart).Minutes())

	for _, entry := range entries {
		if entry.Actor() == actor && entry.Time() >= fromMinutes && entry.Time() <= toMinutes {
			filtered = append(filtered, entry)
		}
	}

	return filtered
}

// downloadAndMergeBitmaps downloads bitmap chunks and merges them.
func (l *Logger) downloadAndMergeBitmaps(ctx context.Context, key string, entries []codec.IndexEntry) (*roaring.Bitmap, error) {
	mergedBitmap := roaring.New()

	for _, entry := range entries {
		// Download bitmap chunk using byte range
		compressedData, err := l.s3Client.DownloadRange(ctx, key, int64(entry.Offset()), int64(entry.Offset()+uint64(entry.CompressedSize())-1))
		if err != nil {
			continue // Skip failed downloads
		}

		// Decompress bitmap
		bitmapData, err := l.codec.Decompress(compressedData)
		if err != nil {
			continue // Skip decompression errors
		}

		// Deserialize bitmap
		bitmap := roaring.New()
		if _, err := bitmap.FromBuffer(bitmapData); err != nil {
			continue // Skip deserialization errors
		}

		// Merge with result
		mergedBitmap.Or(bitmap)
	}

	return mergedBitmap, nil
}

// queryLogChunks queries log chunks for specific sequence IDs.
func (l *Logger) queryLogChunks(ctx context.Context, logKey string, tailMetadata *codec.Metadata, bitmap *roaring.Bitmap, dayStart, from, to time.Time, yield func(time.Time, string) bool) bool {
	// Iterate over all chunks and query each one.
	// This is inefficient as we might download chunks that don't contain our sequence IDs,
	// but the current file format doesn't allow us to map sequence IDs to chunks beforehand.
	for _, chunk := range tailMetadata.Chunks {
		if !l.queryLogChunk(ctx, logKey, chunk, bitmap, dayStart, from, to, yield) {
			return false // Yield returned false, stop.
		}
	}

	return true
}

// queryLogChunk queries a specific log chunk for sequence IDs.
func (l *Logger) queryLogChunk(ctx context.Context, logKey string, chunk codec.ChunkEntry, sequenceIDs *roaring.Bitmap, dayStart, from, to time.Time, yield func(time.Time, string) bool) bool {
	// Download chunk using byte range
	compressedData, err := l.s3Client.DownloadRange(ctx, logKey, int64(chunk.Offset()), int64(chunk.Offset()+uint64(chunk.CompressedSize())-1))
	if err != nil {
		return true // Skip failed downloads
	}

	// Decompress chunk
	decompressedData, err := l.codec.Decompress(compressedData)
	if err != nil {
		return true // Skip decompression errors
	}

	// Parse log entries from decompressed data
	entries, err := l.parseLogEntriesFromData(decompressedData)
	if err != nil {
		return true // Skip parsing errors
	}

	// Filter and yield matching entries
	for _, entry := range entries {
		sequenceID := entry.ID()
		if sequenceIDs.Contains(sequenceID) {
			timestamp := timeOf(sequenceID, dayStart)
			if timestamp.After(from) && timestamp.Before(to) {
				if !yield(timestamp, entry.Text()) {
					return false
				}
			}
		}
	}

	return true
}

// parseLogEntriesFromData parses log entries from raw decompressed data.
func (l *Logger) parseLogEntriesFromData(data []byte) ([]codec.LogEntry, error) {
	var entries []codec.LogEntry
	buf := data

	for len(buf) > 0 {
		// Try to parse the current entry
		entry := codec.LogEntry(buf)
		sequenceID := entry.ID()
		text := entry.Text()
		actors := entry.Actors()

		// Check if parsing was successful
		if sequenceID == 0 && len(text) == 0 && len(actors) == 0 {
			break // End of valid data
		}

		// Re-encode to get the exact size
		reconstructed, err := codec.NewLogEntry(sequenceID, text, actors)
		if err != nil {
			break
		}

		entrySize := len(reconstructed)
		if len(buf) < entrySize {
			break
		}

		// Extract the entry
		entries = append(entries, codec.LogEntry(buf[:entrySize]))
		buf = buf[entrySize:]
	}

	return entries, nil
}
