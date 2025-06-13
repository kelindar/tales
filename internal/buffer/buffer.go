package buffer

import (
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/kelindar/threads/internal/codec"
)

// Buffer represents the in-memory buffer for the current chunk.
type Buffer struct {
	codec   *codec.Codec               // Codec for compression
	data    []byte                     // Raw concatenated log entries
	index   map[uint32]*roaring.Bitmap // Actor ID -> sequence IDs bitmap
	length  int                        // Number of entries in buffer
	maxSize int                        // Maximum number of entries
	start   time.Time                  // Start time of the buffer
}

// New creates a new buffer with the specified maximum size.
func New(maxSize int, codec *codec.Codec) *Buffer {
	return &Buffer{
		codec:   codec,
		data:    make([]byte, 0, maxSize*100), // Estimate ~100 bytes per entry
		index:   make(map[uint32]*roaring.Bitmap),
		length:  0,
		maxSize: maxSize,
		start:   time.Now(),
	}
}

// Size returns the number of entries in the buffer.
func (b *Buffer) Size() int {
	return b.length
}

// Add adds a log entry to the buffer and updates actor bitmaps.
func (b *Buffer) Add(entry codec.LogEntry) bool {
	// Check if buffer is full
	if b.length >= b.maxSize {
		return false
	}

	// Add entry to buffer data
	b.data = append(b.data, entry...)
	b.length++

	// Get sequence ID and actors using accessors
	sequenceID := entry.ID()
	actors := entry.Actors()

	// Update actor bitmaps
	for _, actorID := range actors {
		bitmap := b.index[actorID]
		if bitmap == nil {
			bitmap = roaring.New()
			b.index[actorID] = bitmap
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
		entry := codec.LogEntry(buf)
		size := int(entry.Size())
		if size == 0 || size > len(buf) {
			break
		}

		entries = append(entries, entry[:size])
		buf = buf[size:]
	}

	return entries
}

// Query returns entries for a specific actor within a time range.
func (b *Buffer) Query(actorID uint32, dayStart time.Time, from, to time.Time) []codec.LogEntry {
	var result []codec.LogEntry

	// Get bitmap for this actor
	bitmap := b.index[actorID]
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
	compressedBitmaps := make([]Index, 0, len(b.index))
	for actorID, bm := range b.index {
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
	b.length = 0
	for _, bm := range b.index {
		bm.Clear()
	}
	b.start = time.Now()
}

// timeToSequenceID converts a time to a sequence ID for range queries.
func timeToSequenceID(t, dayStart time.Time) uint32 {
	minutesFromDayStart := uint32(t.Sub(dayStart).Minutes())

	// Use 0 for the counter part since we're doing range comparisons
	return minutesFromDayStart << 20
}
