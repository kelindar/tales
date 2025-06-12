package threads

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/kelindar/threads/internal/buffer"
	"github.com/kelindar/threads/internal/codec"
)

// TailMetadata represents the tail metadata structure in log files.
type TailMetadata struct {
	Magic           [4]byte
	Version         uint32
	DayStart        int64
	ChunkCount      uint32
	Chunks          []codec.ChunkEntry
	TailSize        uint32
	TotalDataSize   uint64
	TotalBitmapSize uint64
}

// flushBuffer flushes the current buffer to S3 using a separate metadata file.
func (l *Logger) flushBuffer(buf *buffer.Buffer) error {
	ctx := context.Background()

	if buf.Size() == 0 {
		return nil
	}

	res, err := buf.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush buffer: %w", err)
	}

	if res.Data.UncompressedSize == 0 {
		return nil
	}

	now := time.Now()
	dayStart := getDayStart(now)
	dateString := getDateString(now)
	flushTimeSeconds := uint32(now.Sub(dayStart).Seconds())

	logKey := fmt.Sprintf("%s/threads.log", dateString)
	metaKey := fmt.Sprintf("%s/threads.meta", dateString)
	bitmapKey := fmt.Sprintf("%s/threads.rbm", dateString)
	indexKey := fmt.Sprintf("%s/threads.idx", dateString)

	// 1. Read existing metadata file.
	metaBytes, err := l.s3Client.DownloadData(ctx, metaKey)
	var tailMetadata *TailMetadata
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			// Metadata file doesn't exist, create a new one.
			tailMetadata = &TailMetadata{
				Magic:    [4]byte{'T', 'A', 'I', 'L'},
				Version:  1,
				DayStart: dayStart.UnixNano(),
			}
		} else {
			return fmt.Errorf("failed to download metadata: %w", err)
		}
	} else {
		// Metadata file exists, decode it.
		tailMetadata, err = decodeTailMetadata(metaBytes)
		if err != nil {
			return fmt.Errorf("failed to decode metadata: %w", err)
		}
	}

	// 2. Append new log data to the log file.
	if err := l.s3Client.AppendData(ctx, logKey, res.Data.CompressedData); err != nil {
		return fmt.Errorf("failed to append to log file: %w", err)
	}

	// 3. Update tail metadata with new chunk info.
	chunkOffset := tailMetadata.TotalDataSize
	newChunk := codec.NewChunkEntry(chunkOffset, res.Data.CompressedSize, res.Data.UncompressedSize)
	tailMetadata.Chunks = append(tailMetadata.Chunks, newChunk)
	tailMetadata.TotalDataSize += uint64(res.Data.CompressedSize)
	tailMetadata.ChunkCount = uint32(len(tailMetadata.Chunks))

	// 4. Handle bitmaps and index entries.
	bitmapChunkOffset := tailMetadata.TotalBitmapSize
	for _, rbm := range res.Index {
		if err := l.s3Client.AppendData(ctx, bitmapKey, rbm.CompressedData); err != nil {
			return err
		}
		indexEntry := codec.NewIndexEntry(flushTimeSeconds, rbm.ActorID, bitmapChunkOffset, rbm.CompressedSize, rbm.UncompressedSize)
		if err := l.s3Client.AppendData(ctx, indexKey, indexEntry); err != nil {
			return err
		}
		bitmapChunkOffset += uint64(rbm.CompressedSize)
	}
	tailMetadata.TotalBitmapSize = bitmapChunkOffset

	// 5. Encode the new tail metadata and upload it, overwriting the old one.
	encodedTail, err := encodeTailMetadata(tailMetadata)
	if err != nil {
		return err
	}
	tailMetadata.TailSize = uint32(len(encodedTail))
	encodedTail, err = encodeTailMetadata(tailMetadata) // Re-encode with correct size
	if err != nil {
		return err
	}

       return l.s3Client.UploadData(ctx, metaKey, encodedTail)
}

// decodeTailMetadata decodes tail metadata from binary format.
func decodeTailMetadata(data []byte) (*TailMetadata, error) {
	if len(data) < 24 { // Minimum size
		return nil, fmt.Errorf("tail metadata too short")
	}

	buf := bytes.NewReader(data)
	metadata := &TailMetadata{}

	if _, err := buf.Read(metadata.Magic[:]); err != nil || string(metadata.Magic[:]) != codec.TailMagic {
		return nil, fmt.Errorf("invalid magic in tail metadata")
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.Version); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.DayStart); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.ChunkCount); err != nil {
		return nil, err
	}

	metadata.Chunks = make([]codec.ChunkEntry, metadata.ChunkCount)
	for i := uint32(0); i < metadata.ChunkCount; i++ {
		chunkData := make([]byte, codec.ChunkEntrySize)
		if _, err := buf.Read(chunkData); err != nil {
			return nil, err
		}
		metadata.Chunks[i] = codec.ChunkEntry(chunkData)
	}

	if err := binary.Read(buf, binary.LittleEndian, &metadata.TailSize); err != nil {
		return nil, err
	}

	return metadata, nil
}

// encodeTailMetadata encodes tail metadata into binary format.
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
		if _, err := buf.Write(chunk); err != nil {
			return nil, fmt.Errorf("failed to write chunk: %w", err)
		}
	}

	// Write tail size (4 bytes)
	if err := binary.Write(buf, binary.LittleEndian, tail.TailSize); err != nil {
		return nil, fmt.Errorf("failed to write tail size: %w", err)
	}

	return buf.Bytes(), nil
}
