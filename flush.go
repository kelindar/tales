package threads

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/kelindar/threads/internal/codec"
)

// TailMetadata represents the tail metadata structure in log files.
type TailMetadata struct {
	Magic      [4]byte            // "TAIL" magic bytes
	Version    uint32             // File format version
	DayStart   int64              // Day start timestamp (unix nano)
	ChunkCount uint32             // Number of chunks
	Chunks     []codec.ChunkEntry // Chunk metadata entries
	TailSize   uint32             // Size of tail metadata
}

// flushBuffer flushes the current buffer to S3.
func (l *Logger) flushBuffer() error {
	l.flushMu.Lock()
	defer l.flushMu.Unlock()

	if l.buffer.IsEmpty() {
		return nil
	}

	// Get current buffer data
	entriesData := l.buffer.GetData()
	actorBitmaps := l.buffer.GetAllActorBitmaps()

	l.mu.RLock()
	dayStart := l.dayStart
	l.mu.RUnlock()

	dateString := getDateString(time.Now())

	// Build S3 keys
	logKey := fmt.Sprintf("%s/threads.log", dateString)
	bitmapKey := fmt.Sprintf("%s/threads.rbm", dateString)
	indexKey := fmt.Sprintf("%s/threads.idx", dateString)

	// 1. Read current tail metadata from log file
	tailMetadata, err := l.readTailMetadata(logKey, dayStart)
	if err != nil {
		return fmt.Errorf("failed to read tail metadata: %w", err)
	}

	// 2. Compress log entries (data is already concatenated in buffer)
	compressedEntries, err := codec.Compress(entriesData)
	if err != nil {
		return fmt.Errorf("failed to compress log entries: %w", err)
	}

	// Calculate new chunk metadata
	var chunkOffset uint64
	if len(tailMetadata.Chunks) > 0 {
		lastChunk := tailMetadata.Chunks[len(tailMetadata.Chunks)-1]
		chunkOffset = lastChunk.Offset() + uint64(lastChunk.CompressedSize())
	}

	newChunk := codec.NewChunkEntry(chunkOffset, uint32(len(compressedEntries)), uint32(len(entriesData)))

	// 3. Append compressed bitmaps and build index entries
	indexEntries, err := l.appendBitmaps(bitmapKey, actorBitmaps, dayStart)
	if err != nil {
		return fmt.Errorf("failed to append bitmaps: %w", err)
	}

	// 4. Append log chunk
	if err := l.s3Client.AppendData(l.ctx, logKey, compressedEntries); err != nil {
		return fmt.Errorf("failed to append log chunk: %w", err)
	}

	// 5. Append index entries
	if err := l.appendIndexEntries(indexKey, indexEntries); err != nil {
		return fmt.Errorf("failed to append index entries: %w", err)
	}

	// 6. Update tail metadata
	tailMetadata.Chunks = append(tailMetadata.Chunks, newChunk)
	tailMetadata.ChunkCount = uint32(len(tailMetadata.Chunks))

	if err := l.updateTailMetadata(logKey, tailMetadata); err != nil {
		return fmt.Errorf("failed to update tail metadata: %w", err)
	}

	// 7. Clear buffer
	l.buffer.Clear()

	return nil
}

// readTailMetadata reads the tail metadata from a log file.
func (l *Logger) readTailMetadata(logKey string, dayStart time.Time) (*TailMetadata, error) {
	// Try to read the tail (last 1024 bytes should be enough for most cases)
	tailData, err := l.s3Client.DownloadTail(l.ctx, logKey, 1024)
	if err != nil {
		// If file doesn't exist, create new metadata
		return &TailMetadata{
			Magic:      [4]byte{'T', 'A', 'I', 'L'},
			DayStart:   dayStart.UnixNano(),
			ChunkCount: 0,
			Chunks:     []codec.ChunkEntry{},
			TailSize:   0,
		}, nil
	}

	if len(tailData) < 4 {
		return nil, ErrFormat{Format: "tail metadata", Err: fmt.Errorf("tail data too short")}
	}

	// Read tail size from the last 4 bytes
	tailSize := binary.LittleEndian.Uint32(tailData[len(tailData)-4:])

	if tailSize > uint32(len(tailData)) {
		return nil, ErrFormat{Format: "tail metadata", Err: fmt.Errorf("invalid tail size")}
	}

	// Extract the actual tail metadata
	metadataStart := len(tailData) - int(tailSize)
	metadataBytes := tailData[metadataStart:]

	return l.decodeTailMetadata(metadataBytes)
}

// decodeTailMetadata decodes tail metadata from binary format.
func (l *Logger) decodeTailMetadata(data []byte) (*TailMetadata, error) {
	if len(data) < 24 { // Minimum size: magic(4) + version(4) + dayStart(8) + chunkCount(4) + tailSize(4)
		return nil, ErrFormat{Format: "tail metadata", Err: fmt.Errorf("data too short")}
	}

	buf := bytes.NewReader(data)
	metadata := &TailMetadata{}

	// Read magic
	if _, err := buf.Read(metadata.Magic[:]); err != nil {
		return nil, ErrFormat{Format: "tail metadata", Err: err}
	}

	if string(metadata.Magic[:]) != codec.TailMagic {
		return nil, ErrFormat{Format: "tail metadata", Err: fmt.Errorf("invalid magic")}
	}

	// Read version
	if err := binary.Read(buf, binary.LittleEndian, &metadata.Version); err != nil {
		return nil, ErrFormat{Format: "tail metadata", Err: err}
	}

	// Read day start
	if err := binary.Read(buf, binary.LittleEndian, &metadata.DayStart); err != nil {
		return nil, ErrFormat{Format: "tail metadata", Err: err}
	}

	// Read chunk count
	if err := binary.Read(buf, binary.LittleEndian, &metadata.ChunkCount); err != nil {
		return nil, ErrFormat{Format: "tail metadata", Err: err}
	}

	// Read chunk entries
	metadata.Chunks = make([]codec.ChunkEntry, metadata.ChunkCount)
	for i := uint32(0); i < metadata.ChunkCount; i++ {
		chunkData := make([]byte, codec.ChunkEntrySize)
		if _, err := buf.Read(chunkData); err != nil {
			return nil, ErrFormat{Format: "tail metadata", Err: err}
		}
		metadata.Chunks[i] = codec.ChunkEntry(chunkData)
	}

	// Read tail size
	if err := binary.Read(buf, binary.LittleEndian, &metadata.TailSize); err != nil {
		return nil, ErrFormat{Format: "tail metadata", Err: err}
	}

	return metadata, nil
}

// appendBitmaps appends compressed bitmaps to the bitmap file and returns index entries.
func (l *Logger) appendBitmaps(bitmapKey string, actorBitmaps map[uint32]*roaring.Bitmap, dayStart time.Time) ([]codec.IndexEntry, error) {
	var indexEntries []codec.IndexEntry

	// Get current bitmap file size to calculate offsets
	currentSize, err := l.s3Client.GetObjectSize(l.ctx, bitmapKey)
	if err != nil {
		// File doesn't exist, start from 0
		currentSize = 0
	}

	currentOffset := uint64(currentSize)
	timestamp := uint32(time.Now().Sub(dayStart).Seconds())

	// Process each actor bitmap
	for actorID, bitmap := range actorBitmaps {
		// Serialize bitmap
		bitmapData, err := bitmap.ToBytes()
		if err != nil {
			return nil, fmt.Errorf("failed to serialize bitmap for actor %d: %w", actorID, err)
		}

		// Compress bitmap
		compressedBitmap, err := codec.Compress(bitmapData)
		if err != nil {
			return nil, fmt.Errorf("failed to compress bitmap for actor %d: %w", actorID, err)
		}

		// Append to bitmap file
		if err := l.s3Client.AppendData(l.ctx, bitmapKey, compressedBitmap); err != nil {
			return nil, fmt.Errorf("failed to append bitmap for actor %d: %w", actorID, err)
		}

		// Create index entry
		indexEntry := codec.NewIndexEntry(timestamp, actorID, currentOffset, uint32(len(compressedBitmap)))
		indexEntries = append(indexEntries, indexEntry)

		currentOffset += uint64(len(compressedBitmap))
	}

	return indexEntries, nil
}

// appendIndexEntries appends index entries to the index file.
func (l *Logger) appendIndexEntries(indexKey string, entries []codec.IndexEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// Encode index entries
	buf := &bytes.Buffer{}
	for _, entry := range entries {
		buf.Write(entry)
	}

	// Append to index file
	return l.s3Client.AppendData(l.ctx, indexKey, buf.Bytes())
}

// updateTailMetadata updates the tail metadata in the log file.
func (l *Logger) updateTailMetadata(logKey string, metadata *TailMetadata) error {
	// Encode metadata
	encodedMetadata, err := encodeTailMetadata(metadata)
	if err != nil {
		return fmt.Errorf("failed to encode tail metadata: %w", err)
	}

	metadata.TailSize = uint32(len(encodedMetadata))

	// Re-encode with correct tail size
	encodedMetadata, err = encodeTailMetadata(metadata)
	if err != nil {
		return fmt.Errorf("failed to re-encode tail metadata: %w", err)
	}

	// Append to log file
	return l.s3Client.AppendData(l.ctx, logKey, encodedMetadata)
}

// encodeTailMetadata encodes tail metadata to binary format.
func encodeTailMetadata(tail *TailMetadata) ([]byte, error) {
	buf := &bytes.Buffer{}

	// Write magic (4 bytes)
	if _, err := buf.Write(tail.Magic[:]); err != nil {
		return nil, fmt.Errorf("failed to write magic: %w", err)
	}

	// Write version (4 bytes)
	if err := binary.Write(buf, binary.LittleEndian, tail.Version); err != nil {
		return nil, fmt.Errorf("failed to write version: %w", err)
	}

	// Write day start (8 bytes)
	if err := binary.Write(buf, binary.LittleEndian, tail.DayStart); err != nil {
		return nil, fmt.Errorf("failed to write day start: %w", err)
	}

	// Write chunk count (4 bytes)
	if err := binary.Write(buf, binary.LittleEndian, tail.ChunkCount); err != nil {
		return nil, fmt.Errorf("failed to write chunk count: %w", err)
	}

	// Write chunk entries
	for _, chunk := range tail.Chunks {
		buf.Write(chunk)
	}

	// Write tail size (4 bytes)
	if err := binary.Write(buf, binary.LittleEndian, tail.TailSize); err != nil {
		return nil, fmt.Errorf("failed to write tail size: %w", err)
	}

	return buf.Bytes(), nil
}
