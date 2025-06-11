package threads

import (
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/kelindar/threads/internal/codec"
)

// Buffer represents the in-memory buffer for the current chunk.
type Buffer struct {
	mu           sync.RWMutex
	data         []byte                     // Raw concatenated log entries
	actorBitmaps map[uint32]*roaring.Bitmap // Actor ID -> sequence IDs bitmap
	entryCount   int                        // Number of entries in buffer
	maxSize      int                        // Maximum number of entries
	chunkStart   time.Time
}

// NewBuffer creates a new buffer with the specified maximum size.
func NewBuffer(maxSize int) *Buffer {
	return &Buffer{
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

// GetEntries returns all entries in the buffer by parsing the raw data.
func (b *Buffer) GetEntries() []codec.LogEntry {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.parseEntries(b.data)
}

// GetData returns a copy of the raw buffer data.
func (b *Buffer) GetData() []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	data := make([]byte, len(b.data))
	copy(data, b.data)
	return data
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

// GetActorEntries returns entries for a specific actor within a time range.
func (b *Buffer) GetActorEntries(actorID uint32, dayStart time.Time, from, to time.Time) []codec.LogEntry {
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

// GetActorBitmap returns a copy of the bitmap for a specific actor.
func (b *Buffer) GetActorBitmap(actorID uint32) *roaring.Bitmap {
	b.mu.RLock()
	defer b.mu.RUnlock()

	bitmap := b.actorBitmaps[actorID]
	if bitmap == nil {
		return roaring.New()
	}

	return bitmap.Clone()
}

// GetAllActorBitmaps returns copies of all actor bitmaps.
func (b *Buffer) GetAllActorBitmaps() map[uint32]*roaring.Bitmap {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make(map[uint32]*roaring.Bitmap)
	for actorID, bitmap := range b.actorBitmaps {
		result[actorID] = bitmap.Clone()
	}
	return result
}

// Size returns the current number of entries in the buffer.
func (b *Buffer) Size() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.entryCount
}

// IsFull returns true if the buffer is at maximum capacity.
func (b *Buffer) IsFull() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.entryCount >= b.maxSize
}

// Clear clears the buffer and resets the chunk start time.
func (b *Buffer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.data = b.data[:0]
	b.entryCount = 0
	b.actorBitmaps = make(map[uint32]*roaring.Bitmap)
	b.chunkStart = time.Now()
}

// GetChunkStart returns the start time of the current chunk.
func (b *Buffer) GetChunkStart() time.Time {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.chunkStart
}

// SetChunkStart sets the start time of the current chunk.
func (b *Buffer) SetChunkStart(t time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.chunkStart = t
}

// GetActorIDs returns all actor IDs that have entries in the buffer.
func (b *Buffer) GetActorIDs() []uint32 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	actorIDs := make([]uint32, 0, len(b.actorBitmaps))
	for actorID := range b.actorBitmaps {
		actorIDs = append(actorIDs, actorID)
	}
	return actorIDs
}

// timeToSequenceID converts a time to a sequence ID for comparison.
// This is a helper function for range queries.
func timeToSequenceID(t time.Time, dayStart time.Time) uint32 {
	minutesFromDayStart := uint32(t.Sub(dayStart).Minutes())
	if minutesFromDayStart > 1439 {
		minutesFromDayStart = 1439
	}
	// Use 0 for the counter part since we're doing range comparisons
	return minutesFromDayStart << 20
}

// IsEmpty returns true if the buffer contains no entries.
func (b *Buffer) IsEmpty() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.entryCount == 0
}
