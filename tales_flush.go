// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

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

	// Read existing metadata file. If it doesn't exist, create a new one.
	meta, err := l.downloadMetadata(ctx, now)
	if err != nil {
		return fmt.Errorf("failed to download metadata: %w", err)
	}

	// Create merged file with bitmap + log sections
	date := seq.FormatDate(now)
	flushTime := uint32(now.Sub(seq.DayOf(now)).Minutes())
	chunkNumber := uint64(meta.Length)
	chunkKey := keyOfChunk(date, chunkNumber)

	// Build metadata for actor bitmaps
	actorMap := make(map[uint32]codec.IndexEntry, len(res.Index))
	var bitmapOffset uint32
	var bitmapSize uint32
	for _, rbm := range res.Index {
		actorMap[rbm.ActorID] = codec.NewIndexEntry(flushTime, uint64(bitmapOffset), rbm.CompressedSize)
		bitmapOffset += rbm.CompressedSize
		bitmapSize += rbm.CompressedSize
	}

	logSize := res.Data.CompressedSize

	// Upload merged file using streaming upload
	reader := newLogReader(res.Index, res.Data.CompressedData)
	if err := l.s3Client.UploadReader(ctx, chunkKey, reader, reader.Size()); err != nil {
		return fmt.Errorf("failed to upload merged chunk: %w", err)
	}

	// Use time bounds from buffer flush result
	encodedMeta, err := codec.EncodeMetadata(
		meta.Append(chunkNumber, bitmapSize, logSize, res.Time[0], res.Time[1], actorMap),
	)
	if err != nil {
		return fmt.Errorf("failed to encode metadata: %w", err)
	}

	// Upload metadata, overwriting the old one
	l.metaLRU.Add(date, meta)
	return l.s3Client.Upload(ctx, keyOfMetadata(date), encodedMeta)
}

// downloadMetadata downloads the metadata file for the provided date.
func (l *Service) downloadMetadata(ctx context.Context, now time.Time) (meta *codec.Metadata, err error) {
	date := seq.FormatDate(now)
	meta, ok := l.metaLRU.Get(date)
	if ok {
		return meta, nil
	}

	buf, err := l.s3Client.Download(ctx, keyOfMetadata(date))
	switch { // Create a new one if it doesn't exist
	case err != nil && s3.IsNoSuchKey(err):
		meta = codec.NewMetadata(seq.DayOf(now))
	case err != nil:
		return nil, err
	default: // Downloaded successfully
		meta, err = codec.DecodeMetadata(buf)
		if err != nil {
			return nil, fmt.Errorf("failed to decode metadata: %w", err)
		}

		// Cache the metadata
		l.metaLRU.Add(date, meta)
	}
	return meta, nil
}

// newLogReader creates a streaming reader for chunk data without intermediate allocations.
// It builds a MultiReaderAt from index entries, bitmaps, and log data.
func newLogReader(bitmaps []buffer.Index, logData []byte) *s3.MultiReader {
	sections := make([][]byte, 0, len(bitmaps)+1)

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
