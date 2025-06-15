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

	_, tidx, _, _ := buildDailyKeys(date)

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

	// 2. Upload new log chunk as a separate file.
	chunkNumber := uint64(meta.ChunkCount)
	chunkKey := buildChunkKey(date, chunkNumber)
	if err := l.s3Client.Upload(ctx, chunkKey, res.Data.CompressedData); err != nil {
		return fmt.Errorf("failed to upload log chunk: %w", err)
	}

	// 3. Update metadata with new chunk info (offset stores chunk number).
	chunkOffset := chunkNumber

	// 4. Handle bitmaps and index entries as separate files for this chunk.
	var bitmapData []byte
	var indexData []byte
	var bitmapOffset uint32
	for _, rbm := range res.Index {
		indexEntry := codec.NewIndexEntry(flushTimeMinutes, rbm.ActorID, uint64(bitmapOffset), rbm.CompressedSize, rbm.UncompressedSize)
		indexData = append(indexData, indexEntry...)
		bitmapData = append(bitmapData, rbm.CompressedData...)
		bitmapOffset += rbm.CompressedSize
	}

	if len(bitmapData) > 0 {
		if err := l.s3Client.Upload(ctx, buildBitmapKey(date, chunkNumber), bitmapData); err != nil {
			return err
		}
		if err := l.s3Client.Upload(ctx, buildIndexKey(date, chunkNumber), indexData); err != nil {
			return err
		}
	}

	meta.Update(chunkOffset, res.Data.CompressedSize, res.Data.UncompressedSize)

	// 5. Encode the metadata as JSON and upload it, overwriting the old one.
	encodedMeta, err := codec.EncodeMetadata(meta)
	if err != nil {
		return fmt.Errorf("failed to encode metadata: %w", err)
	}
	return l.s3Client.Upload(ctx, tidx, encodedMeta)
}
