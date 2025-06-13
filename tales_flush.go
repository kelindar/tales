package threads

import (
	"context"
	"fmt"
	"time"

	"github.com/kelindar/threads/internal/buffer"
	"github.com/kelindar/threads/internal/codec"
	"github.com/kelindar/threads/internal/s3"
	"github.com/kelindar/threads/internal/seq"
)

// flushBuffer flushes the current buffer to S3 using a separate metadata file.
func (l *Service) flushBuffer(buf *buffer.Buffer) error {
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
	day := seq.DayOf(now)
	date := seq.FormatDate(now)
	flushTimeMinutes := uint32(now.Sub(day).Minutes())

	tlog := fmt.Sprintf("%s/threads.log", date)
	tidx := fmt.Sprintf("%s/threads.idx", date)
	alog := fmt.Sprintf("%s/actors.log", date)
	aidx := fmt.Sprintf("%s/actors.idx", date)

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

	// 2. Append new log data to the log file.
	if err := l.s3Client.Append(ctx, tlog, res.Data.CompressedData); err != nil {
		return fmt.Errorf("failed to append to log file: %w", err)
	}

	// 3. Update metadata with new chunk info.
	chunkOffset := uint64(meta.TotalDataSize)

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

	// Re-encode with correct size
	encodedMeta, err = codec.EncodeMetadata(meta)
	if err != nil {
		return fmt.Errorf("failed to re-encode metadata: %w", err)
	}

	return l.s3Client.Upload(ctx, tidx, encodedMeta)
}
