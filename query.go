package threads

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
)

// queryS3Historical implements the S3 historical query logic.
func (l *Logger) queryS3Historical(actor uint32, from, to time.Time, yield func(time.Time, string) bool) {
	// Query each day in the time range
	current := getDayStart(from)
	end := getDayStart(to).Add(24 * time.Hour)

	for current.Before(end) {
		if !l.queryS3Day(actor, current, from, to, yield) {
			return // yield returned false, stop iteration
		}
		current = current.Add(24 * time.Hour)
	}
}

// queryS3Day queries S3 data for a specific day.
func (l *Logger) queryS3Day(actor uint32, dayStart time.Time, from, to time.Time, yield func(time.Time, string) bool) bool {
	dateString := getDateString(dayStart)

	// Build S3 keys
	logKey := fmt.Sprintf("%s/threads.log", dateString)
	bitmapKey := fmt.Sprintf("%s/threads.rbm", dateString)
	indexKey := fmt.Sprintf("%s/threads.idx", dateString)

	// 1. Download and parse index file
	indexEntries, err := l.downloadIndexEntries(indexKey)
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
	mergedBitmap, err := l.downloadAndMergeBitmaps(bitmapKey, relevantEntries)
	if err != nil {
		return true // Skip on error
	}

	if mergedBitmap.IsEmpty() {
		return true
	}

	// 4. Download tail metadata to get chunk information
	tailMetadata, err := l.readTailMetadata(logKey, dayStart)
	if err != nil {
		return true // Skip on error
	}

	// 5. Group sequence IDs by log chunks and query
	return l.queryLogChunks(logKey, tailMetadata, mergedBitmap, dayStart, from, to, yield)
}

// downloadIndexEntries downloads and parses the index file.
func (l *Logger) downloadIndexEntries(indexKey string) ([]IndexEntry, error) {
	data, err := l.s3Client.DownloadData(l.ctx, indexKey)
	if err != nil {
		return nil, err
	}

	if len(data)%IndexEntrySize != 0 {
		return nil, ErrFormat{Format: "index file", Err: fmt.Errorf("invalid file size")}
	}

	entryCount := len(data) / IndexEntrySize
	entries := make([]IndexEntry, entryCount)

	buf := bytes.NewReader(data)
	for i := 0; i < entryCount; i++ {
		entry := &entries[i]
		if err := binary.Read(buf, binary.LittleEndian, &entry.Timestamp); err != nil {
			return nil, ErrFormat{Format: "index entry", Err: err}
		}
		if err := binary.Read(buf, binary.LittleEndian, &entry.ActorID); err != nil {
			return nil, ErrFormat{Format: "index entry", Err: err}
		}
		if err := binary.Read(buf, binary.LittleEndian, &entry.Offset); err != nil {
			return nil, ErrFormat{Format: "index entry", Err: err}
		}
		if err := binary.Read(buf, binary.LittleEndian, &entry.Size); err != nil {
			return nil, ErrFormat{Format: "index entry", Err: err}
		}
	}

	return entries, nil
}

// filterIndexEntries filters index entries by actor and time range.
func (l *Logger) filterIndexEntries(entries []IndexEntry, actor uint32, dayStart, from, to time.Time) []IndexEntry {
	var filtered []IndexEntry

	fromSeconds := uint32(from.Sub(dayStart).Seconds())
	toSeconds := uint32(to.Sub(dayStart).Seconds())

	for _, entry := range entries {
		if entry.ActorID == actor && entry.Timestamp >= fromSeconds && entry.Timestamp <= toSeconds {
			filtered = append(filtered, entry)
		}
	}

	return filtered
}

// downloadAndMergeBitmaps downloads bitmap chunks and merges them.
func (l *Logger) downloadAndMergeBitmaps(bitmapKey string, entries []IndexEntry) (*roaring.Bitmap, error) {
	mergedBitmap := roaring.New()

	for _, entry := range entries {
		// Download bitmap chunk using byte range
		compressedData, err := l.s3Client.DownloadRange(l.ctx, bitmapKey, int64(entry.Offset), int64(entry.Offset+uint64(entry.Size)-1))
		if err != nil {
			continue // Skip failed downloads
		}

		// Decompress bitmap
		bitmapData, err := decompressBitmap(compressedData)
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
func (l *Logger) queryLogChunks(logKey string, tailMetadata *TailMetadata, bitmap *roaring.Bitmap, dayStart, from, to time.Time, yield func(time.Time, string) bool) bool {
	// Group sequence IDs by the chunks they belong to
	chunkGroups := l.groupSequenceIDsByChunk(bitmap, tailMetadata)

	// Sort chunk indices for efficient access
	var chunkIndices []int
	for chunkIndex := range chunkGroups {
		chunkIndices = append(chunkIndices, chunkIndex)
	}
	sort.Ints(chunkIndices)

	// Query each chunk
	for _, chunkIndex := range chunkIndices {
		sequenceIDs := chunkGroups[chunkIndex]
		if !l.queryLogChunk(logKey, tailMetadata.Chunks[chunkIndex], sequenceIDs, dayStart, from, to, yield) {
			return false
		}
	}

	return true
}

// groupSequenceIDsByChunk groups sequence IDs by the log chunks they belong to.
func (l *Logger) groupSequenceIDsByChunk(bitmap *roaring.Bitmap, tailMetadata *TailMetadata) map[int]*roaring.Bitmap {
	groups := make(map[int]*roaring.Bitmap)

	// For simplicity, we'll assume each chunk covers a 5-minute period
	// In a real implementation, you'd need more sophisticated chunk mapping
	chunkDuration := 5 * time.Minute
	minutesPerChunk := uint32(chunkDuration.Minutes())

	bitmap.Iterate(func(sequenceID uint32) bool {
		minutes := sequenceID >> 20
		chunkIndex := int(minutes / minutesPerChunk)

		if chunkIndex < len(tailMetadata.Chunks) {
			if groups[chunkIndex] == nil {
				groups[chunkIndex] = roaring.New()
			}
			groups[chunkIndex].Add(sequenceID)
		}
		return true
	})

	return groups
}

// queryLogChunk queries a specific log chunk for sequence IDs.
func (l *Logger) queryLogChunk(logKey string, chunk ChunkEntry, sequenceIDs *roaring.Bitmap, dayStart, from, to time.Time, yield func(time.Time, string) bool) bool {
	// Download chunk using byte range
	compressedData, err := l.s3Client.DownloadRange(l.ctx, logKey, int64(chunk.Offset), int64(chunk.Offset+uint64(chunk.CompressedSize)-1))
	if err != nil {
		return true // Skip failed downloads
	}

	// Decompress chunk
	decompressedData, err := decompressData(compressedData)
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
		if sequenceIDs.Contains(entry.SequenceID) {
			timestamp := reconstructTimestamp(entry.SequenceID, dayStart)
			if timestamp.After(from) && timestamp.Before(to) {
				if !yield(timestamp, entry.Text) {
					return false
				}
			}
		}
	}

	return true
}

// parseLogEntriesFromData parses log entries from raw decompressed data.
func (l *Logger) parseLogEntriesFromData(data []byte) ([]*LogEntry, error) {
	var entries []*LogEntry
	buf := bytes.NewReader(data)

	for buf.Len() > 0 {
		// Read sequence ID to determine if we have a valid entry
		var sequenceID uint32
		if err := binary.Read(buf, binary.LittleEndian, &sequenceID); err != nil {
			break // End of data
		}

		// Reset buffer to include the sequence ID we just read
		remaining := make([]byte, buf.Len()+4)
		binary.LittleEndian.PutUint32(remaining[:4], sequenceID)
		if _, err := buf.Read(remaining[4:]); err != nil {
			break
		}

		// Try to decode the entry
		entry, err := decodeLogEntry(remaining)
		if err != nil {
			break // Parsing error, stop
		}

		entries = append(entries, entry)

		// Calculate consumed bytes and advance buffer
		consumed := calculateLogEntrySize(entry)
		if consumed > len(remaining) {
			break
		}

		buf = bytes.NewReader(remaining[consumed:])
	}

	return entries, nil
}
