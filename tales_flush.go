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

	_, tidx, alog, aidx := buildDailyKeys(date)

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

	// 4. Handle bitmaps and index entries.
	bitmapChunkOffset := meta.TotalBitmapSize
	for _, rbm := range res.Index {
		if err := l.s3Client.Append(ctx, alog, rbm.CompressedData); err != nil {
			return err
		}
		indexEntry := codec.NewIndexEntry(flushTimeMinutes, rbm.ActorID, uint64(bitmapChunkOffset), rbm.CompressedSize, rbm.UncompressedSize)
		if err := l.s3Client.Append(ctx, aidx, indexEntry); err != nil {
			return err
		}
		bitmapChunkOffset += rbm.CompressedSize
	}

	meta.Update(chunkOffset, res.Data.CompressedSize, res.Data.UncompressedSize, bitmapChunkOffset)

	// 5. Encode the metadata as JSON and upload it, overwriting the old one.
	encodedMeta, err := codec.EncodeMetadata(meta)
	if err != nil {
		return fmt.Errorf("failed to encode metadata: %w", err)
	}
	meta.Size = uint32(len(encodedMeta))
	return l.s3Client.Upload(ctx, tidx, encodedMeta)
}
