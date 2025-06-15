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

	// Build index entries without intermediate allocations
	indexEntries := make([]codec.IndexEntry, 0, len(res.Index))
	var bitmapOffset uint32
	for _, rbm := range res.Index {
		indexEntry := codec.NewIndexEntry(flushTimeMinutes, rbm.ActorID, uint64(bitmapOffset), rbm.CompressedSize, rbm.UncompressedSize)
		indexEntries = append(indexEntries, indexEntry)
		bitmapOffset += rbm.CompressedSize
	}

	// Calculate section sizes for metadata
	indexSize := uint32(len(indexEntries) * codec.IndexEntrySize)
	var bitmapSize uint32
	for _, bm := range res.Index {
		bitmapSize += bm.CompressedSize
	}
	logSize := res.Data.CompressedSize

	// Upload merged file using streaming upload
	reader := newLogReader(indexEntries, res.Index, res.Data.CompressedData)
	if err := l.s3Client.UploadReader(ctx, chunkKey, reader, reader.Size()); err != nil {
		return fmt.Errorf("failed to upload merged chunk: %w", err)
	}

	// 3. Update metadata with section sizes
	chunkOffset := chunkNumber
	meta.Update(chunkOffset, indexSize, bitmapSize, logSize)

	// 4. Encode the metadata as JSON and upload it, overwriting the old one.
	encodedMeta, err := codec.EncodeMetadata(meta)
	if err != nil {
		return fmt.Errorf("failed to encode metadata: %w", err)
	}
	return l.s3Client.Upload(ctx, tidx, encodedMeta)
}

// newLogReader creates a streaming reader for chunk data without intermediate allocations.
// It builds a MultiReaderAt from index entries, bitmaps, and log data.
func newLogReader(indexEntries []codec.IndexEntry, bitmaps []buffer.Index, logData []byte) *s3.MultiReader {
	sections := make([][]byte, 0, len(indexEntries)+len(bitmaps)+1)

	// Add index entries (no copying, just references)
	for _, entry := range indexEntries {
		sections = append(sections, []byte(entry))
	}

	// Add bitmap data (no copying, just references)
	for _, bm := range bitmaps {
		sections = append(sections, bm.CompressedData)
	}

	// Add log data (no copying, just reference)
	if len(logData) > 0 {
		sections = append(sections, logData)
	}

	return s3.NewMultiReader(sections...)
}
