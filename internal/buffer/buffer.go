// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package buffer

import (
	"iter"
	"slices"
	"time"

	"github.com/kelindar/roaring"
	"github.com/kelindar/tales/internal/codec"
)

// Buffer represents the in-memory buffer for the current chunk.
type Buffer struct {
	codec   *codec.Codec               // Codec for compression
	data    []byte                     // Raw concatenated log entries
	index   map[uint32]*roaring.Bitmap // Actor ID -> sequence IDs bitmap
	length  int                        // Number of entries in buffer
	maxSize int                        // Maximum number of entries
	start   time.Time                  // Start time of the buffer
	time    [2]uint32                  // Time bounds [min, max] (Unix seconds)
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
func (b *Buffer) Add(entry codec.LogEntry, entryTime time.Time) bool {
	// Check if buffer is full
	if b.length >= b.maxSize {
		return false
	}

	// Add entry to buffer data
	b.data = append(b.data, entry...)
	b.length++

	// Update time bounds
	timestamp := uint32(entryTime.Unix())
	if b.length == 1 {
		b.time[0] = timestamp
		b.time[1] = timestamp
	} else {
		b.time[0] = min(b.time[0], timestamp)
		b.time[1] = max(b.time[1], timestamp)
	}

	// Update actor bitmaps
	for actorID := range entry.Actors() {
		bitmap, ok := b.index[actorID]
		if !ok || bitmap == nil {
			bitmap = roaring.New()
			b.index[actorID] = bitmap
		}
		bitmap.Set(entry.ID())
	}

	return true
}

// Query returns entries for a specific actor within a time range.
func (b *Buffer) Query(actorID uint32, dayStart time.Time, from, to time.Time) iter.Seq[codec.LogEntry] {
	return b.QueryActors(dayStart, from, to, []uint32{actorID})
}

// QueryActors returns entries that contain ALL specified actors within a time range.
func (b *Buffer) QueryActors(dayStart time.Time, from, to time.Time, actors []uint32) iter.Seq[codec.LogEntry] {
	return func(yield func(codec.LogEntry) bool) {
		if len(actors) == 0 {
			return
		}

		t0 := asSequence(from.UTC(), dayStart)
		t1 := asSequence(to.UTC(), dayStart) | counterMask

		for buffer := b.data; len(buffer) > 0; {
			entry := codec.LogEntry(buffer)
			size := int(entry.Size())
			if size == 0 || size > len(buffer) {
				return
			}

			// Check if this entry contains ALL required actors
			if id := entry.ID(); id >= t0 && id <= t1 {
				entryActors := entry.Actors()
				if containsAllActors(entryActors, actors) {
					if !yield(entry[:size]) {
						return
					}
				}
			}

			buffer = buffer[size:]
		}
	}
}

// containsAllActors checks if entryActors contains all required actors.
func containsAllActors(entryActors iter.Seq[uint32], requiredActors []uint32) bool {
	if len(requiredActors) == 0 {
		return false
	}

	// Convert iterator to slice for efficient searching
	var actorSlice []uint32
	for actor := range entryActors {
		actorSlice = append(actorSlice, actor)
	}

	for _, required := range requiredActors {
		if !slices.Contains(actorSlice, required) {
			return false
		}
	}
	return true
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
	Time  [2]uint32 // Time bounds [min, max] (Unix seconds)
}

// Flush atomically extracts the current buffer contents and resets the buffer.
// It returns a deep-copied snapshot so the caller owns the returned slices/maps
// without needing additional synchronization.
func (b *Buffer) Flush() (Flush, error) {
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
	index := make([]Index, 0, len(b.index))
	for actorID, bm := range b.index {
		if bm == nil {
			continue
		}

		bitmapData := bm.ToBytes()

		// Do not compress bitmap, just store raw bytes
		index = append(index, Index{
			ActorID: actorID,
			Binary: Binary{
				UncompressedSize: uint32(len(bitmapData)),
				CompressedSize:   uint32(len(bitmapData)),
				CompressedData:   bitmapData,
			},
		})
	}

	// Capture time bounds before reset
	dt := b.time
	if b.length == 0 {
		now := uint32(time.Now().Unix())
		dt = [2]uint32{now, now}
	}

	// Reset buffer state
	b.reset()

	return Flush{Data: dataCopy, Index: index, Time: dt}, nil
}

// reset resets the buffer's internal state.
func (b *Buffer) reset() {
	b.data = b.data[:0]
	b.length = 0
	b.time[0] = 0
	b.time[1] = 0
	for k := range b.index {
		b.index[k].Clear()
	}
	b.start = time.Now()
}

// asSequence converts a time to a sequence ID for range queries.
const counterMask = (1 << 20) - 1

func asSequence(t, dayStart time.Time) uint32 {
	return uint32(t.Sub(dayStart).Minutes()) << 20
}
