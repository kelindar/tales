package threads

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	// Magic constants for file format validation
	TailMagic   = "TAIL"
	FileVersion = uint32(1)
	
	// Fixed sizes for binary structures
	ChunkEntrySize = 16 // 8 bytes offset + 4 bytes compressed size + 4 bytes uncompressed size
	IndexEntrySize = 20 // 4 bytes timestamp + 4 bytes actor_id + 8 bytes offset + 4 bytes size
)

// LogEntry represents a single log entry in memory.
type LogEntry struct {
	SequenceID uint32
	Text       string
	Actors     []uint32
}

// ChunkEntry represents metadata for a compressed chunk in the log file.
type ChunkEntry struct {
	Offset           uint64 // Chunk start offset in file
	CompressedSize   uint32 // Size of compressed chunk
	UncompressedSize uint32 // Size of uncompressed chunk
}

// TailMetadata represents the tail metadata structure in log files.
type TailMetadata struct {
	Magic      [4]byte      // "TAIL" magic bytes
	Version    uint32       // File format version
	DayStart   int64        // Day start timestamp (unix nano)
	ChunkCount uint32       // Number of chunks
	Chunks     []ChunkEntry // Chunk metadata entries
	TailSize   uint32       // Size of tail metadata
}

// IndexEntry represents an entry in the index file.
type IndexEntry struct {
	Timestamp uint32 // Timestamp (seconds from day start)
	ActorID   uint32 // Actor ID
	Offset    uint64 // Offset in bitmap file
	Size      uint32 // Size of compressed bitmap
}

// encodeLogEntry encodes a log entry to binary format.
func encodeLogEntry(entry *LogEntry) ([]byte, error) {
	buf := &bytes.Buffer{}
	
	// Write sequence ID (4 bytes)
	if err := binary.Write(buf, binary.LittleEndian, entry.SequenceID); err != nil {
		return nil, fmt.Errorf("failed to write sequence ID: %w", err)
	}
	
	// Write text length (varint)
	if err := writeVarint(buf, uint64(len(entry.Text))); err != nil {
		return nil, fmt.Errorf("failed to write text length: %w", err)
	}
	
	// Write actor count (varint)
	if err := writeVarint(buf, uint64(len(entry.Actors))); err != nil {
		return nil, fmt.Errorf("failed to write actor count: %w", err)
	}
	
	// Write text (UTF-8)
	if _, err := buf.WriteString(entry.Text); err != nil {
		return nil, fmt.Errorf("failed to write text: %w", err)
	}
	
	// Write actor IDs (4 bytes each)
	for _, actor := range entry.Actors {
		if err := binary.Write(buf, binary.LittleEndian, actor); err != nil {
			return nil, fmt.Errorf("failed to write actor ID: %w", err)
		}
	}
	
	return buf.Bytes(), nil
}

// decodeLogEntry decodes a log entry from binary format.
func decodeLogEntry(data []byte) (*LogEntry, error) {
	buf := bytes.NewReader(data)
	entry := &LogEntry{}
	
	// Read sequence ID (4 bytes)
	if err := binary.Read(buf, binary.LittleEndian, &entry.SequenceID); err != nil {
		return nil, fmt.Errorf("failed to read sequence ID: %w", err)
	}
	
	// Read text length (varint)
	textLen, err := readVarint(buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read text length: %w", err)
	}
	
	// Read actor count (varint)
	actorCount, err := readVarint(buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read actor count: %w", err)
	}
	
	// Read text (UTF-8)
	textBytes := make([]byte, textLen)
	if _, err := io.ReadFull(buf, textBytes); err != nil {
		return nil, fmt.Errorf("failed to read text: %w", err)
	}
	entry.Text = string(textBytes)
	
	// Read actor IDs (4 bytes each)
	entry.Actors = make([]uint32, actorCount)
	for i := uint64(0); i < actorCount; i++ {
		if err := binary.Read(buf, binary.LittleEndian, &entry.Actors[i]); err != nil {
			return nil, fmt.Errorf("failed to read actor ID: %w", err)
		}
	}
	
	return entry, nil
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
		if err := binary.Write(buf, binary.LittleEndian, chunk.Offset); err != nil {
			return nil, fmt.Errorf("failed to write chunk offset: %w", err)
		}
		if err := binary.Write(buf, binary.LittleEndian, chunk.CompressedSize); err != nil {
			return nil, fmt.Errorf("failed to write compressed size: %w", err)
		}
		if err := binary.Write(buf, binary.LittleEndian, chunk.UncompressedSize); err != nil {
			return nil, fmt.Errorf("failed to write uncompressed size: %w", err)
		}
	}
	
	// Write tail size (4 bytes)
	if err := binary.Write(buf, binary.LittleEndian, tail.TailSize); err != nil {
		return nil, fmt.Errorf("failed to write tail size: %w", err)
	}
	
	return buf.Bytes(), nil
}

// writeVarint writes a varint to the buffer.
func writeVarint(buf *bytes.Buffer, value uint64) error {
	for value >= 0x80 {
		if err := buf.WriteByte(byte(value) | 0x80); err != nil {
			return err
		}
		value >>= 7
	}
	return buf.WriteByte(byte(value))
}

// readVarint reads a varint from the reader.
func readVarint(r io.Reader) (uint64, error) {
	var result uint64
	var shift uint
	
	for {
		b := make([]byte, 1)
		if _, err := r.Read(b); err != nil {
			return 0, err
		}
		
		result |= uint64(b[0]&0x7F) << shift
		if b[0]&0x80 == 0 {
			break
		}
		shift += 7
		if shift >= 64 {
			return 0, fmt.Errorf("varint too long")
		}
	}
	
	return result, nil
}
