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

	// Read existing metadata file.
	meta, err := l.downloadMetadata(ctx, now)
	if err != nil {
		return err
	}

	// Create merged file with index + bitmap + log sections
	date := seq.FormatDate(now)
	flushTime := uint32(now.Sub(seq.DayOf(now)).Minutes())
	chunkNumber := uint64(meta.ChunkCount)
	chunkKey := keyOfChunk(date, chunkNumber)

	// Build index entries without intermediate allocations
	indexEntries := make([]codec.IndexEntry, 0, len(res.Index))
	var bitmapOffset uint32
	for _, rbm := range res.Index {
		indexEntry := codec.NewIndexEntry(flushTime, rbm.ActorID, uint64(bitmapOffset), rbm.CompressedSize, rbm.UncompressedSize)
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

	// Append a new chunk entry to the metadata and encode it
	encodedMeta, err := codec.EncodeMetadata(
		meta.Append(chunkNumber, indexSize, bitmapSize, logSize),
	)
	if err != nil {
		return fmt.Errorf("failed to encode metadata: %w", err)
	}

	// Upload metadata, overwriting the old one
	return l.s3Client.Upload(ctx, keyOfMetadata(date), encodedMeta)
}

// downloadMetadata downloads the metadata file for the provided date.
func (l *Service) downloadMetadata(ctx context.Context, now time.Time) (meta *codec.Metadata, err error) {
	day := seq.FormatDate(now)
	buf, err := l.s3Client.Download(ctx, keyOfMetadata(day))
	switch { // Create a new one if it doesn't exist
	case err != nil && s3.IsNoSuchKey(err):
		meta = codec.NewMetadata(seq.DayOf(now))
	case err != nil:
		return nil, fmt.Errorf("failed to download metadata: %w", err)
	default: // Downloaded successfully
		meta, err = codec.DecodeMetadata(buf)
		if err != nil {
			return nil, fmt.Errorf("failed to decode metadata: %w", err)
		}
	}
	return meta, nil
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
