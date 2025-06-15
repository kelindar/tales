package tales

import (
	"context"
	"fmt"
	"time"

	"github.com/kelindar/tales/internal/buffer"
	"github.com/kelindar/tales/internal/codec"
	"github.com/kelindar/tales/internal/s3"
	"github.com/kelindar/tales/internal/seq"
)

// flushBuffer flushes the current buffer to S3 using a separate metadata file.
func (l *Service) flushBuffer(ctx context.Context, buf *buffer.Buffer) error {
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
	day := seq.DayOf(now)
	date := seq.FormatDate(now)
	flushTimeMinutes := uint32(now.Sub(day).Minutes())

	tidx := buildMetadataKey(date)

	// 1. Read existing metadata file.
	var meta *codec.Metadata
	metaBytes, err := l.s3Client.Download(ctx, tidx)
	switch {
	case err != nil && s3.IsNoSuchKey(err):
		meta = codec.NewMetadata(day)
	case err != nil:
		return fmt.Errorf("failed to download metadata: %w", err)
	default:
		meta, err = codec.DecodeMetadata(metaBytes)
		if err != nil {
			return fmt.Errorf("failed to decode metadata: %w", err)
		}
	}

	// 2. Create merged file with index + bitmap + log sections
	chunkNumber := uint64(meta.ChunkCount)
	chunkKey := buildChunkKey(date, chunkNumber)

	// Build index section
	var indexData []byte
	var bitmapData []byte
	var bitmapOffset uint32
	for _, rbm := range res.Index {
		indexEntry := codec.NewIndexEntry(flushTimeMinutes, rbm.ActorID, uint64(bitmapOffset), rbm.CompressedSize, rbm.UncompressedSize)
		indexData = append(indexData, indexEntry...)
		bitmapData = append(bitmapData, rbm.CompressedData...)
		bitmapOffset += rbm.CompressedSize
	}

	// Create merged file: [index section] + [bitmap section] + [log section]
	mergedData := make([]byte, 0, len(indexData)+len(bitmapData)+len(res.Data.CompressedData))
	mergedData = append(mergedData, indexData...)
	mergedData = append(mergedData, bitmapData...)
	mergedData = append(mergedData, res.Data.CompressedData...)

	// Upload merged file
	if err := l.s3Client.Upload(ctx, chunkKey, mergedData); err != nil {
		return fmt.Errorf("failed to upload merged chunk: %w", err)
	}

	// 3. Update metadata with section sizes
	chunkOffset := chunkNumber
	meta.Update(chunkOffset, uint32(len(indexData)), uint32(len(bitmapData)), res.Data.CompressedSize)

	// 4. Encode the metadata as JSON and upload it, overwriting the old one.
	encodedMeta, err := codec.EncodeMetadata(meta)
	if err != nil {
		return fmt.Errorf("failed to encode metadata: %w", err)
	}
	return l.s3Client.Upload(ctx, tidx, encodedMeta)
}
