package buffer

import (
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/kelindar/threads/internal/codec"
)

// Buffer represents the in-memory buffer for the current chunk.
type Buffer struct {
	mu           sync.RWMutex
	codec        *codec.Codec               // Codec for compression
	data         []byte                     // Raw concatenated log entries
	actorBitmaps map[uint32]*roaring.Bitmap // Actor ID -> sequence IDs bitmap
	entryCount   int                        // Number of entries in buffer
	maxSize      int                        // Maximum number of entries
	chunkStart   time.Time
}

// New creates a new buffer with the specified maximum size.
func New(maxSize int, codec *codec.Codec) *Buffer {
	return &Buffer{
		codec:        codec,
		data:         make([]byte, 0, maxSize*100), // Estimate ~100 bytes per entry
		actorBitmaps: make(map[uint32]*roaring.Bitmap),
		entryCount:   0,
		maxSize:      maxSize,
		chunkStart:   time.Now(),
	}
}

// Add adds a log entry to the buffer and updates actor bitmaps.
func (b *Buffer) Add(entry codec.LogEntry) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Check if buffer is full
	if b.entryCount >= b.maxSize {
		return false
	}

	// Add entry to buffer data
	b.data = append(b.data, entry...)
	b.entryCount++

	// Get sequence ID and actors using accessors
	sequenceID := entry.ID()
	actors := entry.Actors()

	// Update actor bitmaps
	for _, actorID := range actors {
		bitmap := b.actorBitmaps[actorID]
		if bitmap == nil {
			bitmap = roaring.New()
			b.actorBitmaps[actorID] = bitmap
		}
		bitmap.Add(sequenceID)
	}

	return true
}

// parseEntries parses log entries from raw data.
func (b *Buffer) parseEntries(data []byte) []codec.LogEntry {
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

	return entries
}

// Query returns entries for a specific actor within a time range.
func (b *Buffer) Query(actorID uint32, dayStart time.Time, from, to time.Time) []codec.LogEntry {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var result []codec.LogEntry

	// Get bitmap for this actor
	bitmap := b.actorBitmaps[actorID]
	if bitmap == nil {
		return result
	}

	// Convert time range to sequence ID range (normalize to UTC first)
	fromSeq := timeToSequenceID(from.UTC(), dayStart)
	toSeq := timeToSequenceID(to.UTC(), dayStart)

	// Parse all entries and filter by actor and time range
	entries := b.parseEntries(b.data)
	for _, entry := range entries {
		sequenceID := entry.ID()
		if sequenceID >= fromSeq && sequenceID <= toSeq {
			// Get actors using accessor
			actors := entry.Actors()

			// Check if this entry contains the actor
			for _, actor := range actors {
				if actor == actorID {
					result = append(result, entry)
					break
				}
			}
		}
	}

	return result
}

// Binary represents compressed data.
type Binary struct {
	UncompressedSize uint32
	CompressedSize   uint32
	CompressedData   []byte
}

// Index represents a compressed actor bitmap.
type Index struct {
	Binary
	ActorID uint32
}

// Flush represents the data returned by Buffer.Flush.
type Flush struct {
	Data  Binary
	Index []Index
}

// Flush atomically extracts the current buffer contents and resets the buffer.
// It returns a deep-copied snapshot so the caller owns the returned slices/maps
// without needing additional synchronization.
func (b *Buffer) Flush() (Flush, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Compress raw data
	compressedData, err := b.codec.Compress(b.data)
	if err != nil {
		return Flush{}, err
	}
	dataCopy := Binary{
		UncompressedSize: uint32(len(b.data)),
		CompressedSize:   uint32(len(compressedData)),
		CompressedData:   compressedData,
	}

	// Compress bitmaps
	compressedBitmaps := make([]Index, 0, len(b.actorBitmaps))
	for actorID, bm := range b.actorBitmaps {
		// Serialize bitmap
		bitmapData, err := bm.ToBytes()
		if err != nil {
			return Flush{}, err
		}

		// Compress bitmap
		compressedBitmapData, err := b.codec.Compress(bitmapData)
		if err != nil {
			return Flush{}, err
		}

		compressedBitmaps = append(compressedBitmaps, Index{
			ActorID: actorID,
			Binary: Binary{
				UncompressedSize: uint32(len(bitmapData)),
				CompressedSize:   uint32(len(compressedBitmapData)),
				CompressedData:   compressedBitmapData,
			},
		})
	}

	// Reset buffer state
	b.reset()

	return Flush{Data: dataCopy, Index: compressedBitmaps}, nil
}

// reset resets the buffer's internal state.
func (b *Buffer) reset() {
	b.data = b.data[:0]
	b.entryCount = 0
	for _, bm := range b.actorBitmaps {
		bm.Clear()
	}
	b.chunkStart = time.Now()
}

// timeToSequenceID converts a time to a sequence ID for range queries.
func timeToSequenceID(t, dayStart time.Time) uint32 {
	minutesFromDayStart := uint32(t.Sub(dayStart).Minutes())

	// Use 0 for the counter part since we're doing range comparisons
	return minutesFromDayStart << 20
}
