// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package tales

import (
	"context"
	"fmt"
	"iter"
	"time"

	"github.com/kelindar/roaring"
	"github.com/kelindar/tales/internal/codec"
	"github.com/kelindar/tales/internal/seq"
)

// queryWarm queries the in-memory buffer for entries.
func (l *Service) queryWarm(actors []uint32, from, to time.Time, yield func(time.Time, string) bool) {
	ret := make(chan iter.Seq[codec.LogEntry], 1)
	l.commands <- command{query: &queryCmd{actors: actors, from: from, to: to, ret: ret}}

	day := seq.DayOf(from)
	for entry := range <-ret {
		if !yield(entry.Time(day), entry.Text()) {
			return
		}
	}
}

// queryCold implements the S3 historical query logic.
func (l *Service) queryCold(ctx context.Context, actors []uint32, from, to time.Time, yield func(time.Time, string) bool) {
	t0 := seq.DayOf(from)
	t1 := seq.DayOf(to).Add(24 * time.Hour)
	for ; t0.Before(t1); t0 = t0.Add(24 * time.Hour) {
		if !l.queryDay(ctx, actors, t0, from, to, yield) {
			return // yield returned false, stop iteration
		}
	}
}

// queryDay queries S3 data for a specific day.
func (l *Service) queryDay(ctx context.Context, actors []uint32, day time.Time, from, to time.Time, yield func(time.Time, string) bool) bool {
	if len(actors) == 0 {
		return true
	}

	// Retrieve metadata from cache or S3
	meta, err := l.downloadMetadata(ctx, day)
	if err != nil {
		return true
	}

	// Pre-compute time range in minutes once per day
	t0 := uint32(max(0, int(from.Sub(day).Minutes())))
	t1 := uint32(min(1440, int(to.Sub(day).Minutes()))) // 1440 minutes = 24 hours

	// Pre-compute time range in seconds for chunk filtering
	fromSec := uint32(from.Unix())
	toSec := uint32(to.Unix())

	// For each chunk, load all relevant bitmaps and compute intersection
	for _, chunk := range meta.Chunks {
		if !chunk.Between(fromSec, toSec) {
			continue
		}

		chunkKey := keyOfChunk(seq.FormatDate(day), chunk.Offset())

		var index *roaring.Bitmap
		for _, a := range actors {
			idx, ok := chunk.Actors[a]
			if !ok || uint32(idx[0]) < t0 || uint32(idx[0]) > t1 {
				index = nil
				break
			}

			bitmap, err := l.loadBitmap(ctx, chunkKey, idx)
			if err != nil {
				index = nil
				break
			}

			switch {
			case index == nil:
				index = bitmap
			case index.Count() > 0:
				index.And(bitmap)
			}
		}

		// Query log section with intersection bitmap
		if index != nil && index.Count() > 0 {
			if !l.queryChunk(ctx, chunkKey, chunk, index, day, from, to, yield) {
				return false
			}
		}
	}

	return true
}

// loadBitmap downloads and decodes a single bitmap for a given index entry.
func (l *Service) loadBitmap(ctx context.Context, key string, entry codec.IndexEntry) (*roaring.Bitmap, error) {
	i0 := int64(entry[1])
	i1 := i0 + int64(entry[2]) - 1

	data, err := l.s3Client.DownloadRange(ctx, key, i0, i1)
	if err != nil {
		return nil, fmt.Errorf("failed to download bitmap chunk: %w", err)
	}

	// Bitmaps are stored uncompressed, so just deserialize
	bm := roaring.FromBytes(data)
	return bm, nil
}

// queryChunk queries a specific log chunk for sequence IDs using an optimized bitmap iterator.
// This function efficiently filters log entries by leveraging the sorted nature of both the log entries
// and the bitmap, avoiding unnecessary bitmap lookups for entries that don't match.
func (l *Service) queryChunk(ctx context.Context, chunkKey string, chunk codec.ChunkEntry, sids *roaring.Bitmap, day, from, to time.Time, yield func(time.Time, string) bool) bool {
	if sids.Count() == 0 {
		return true
	}

	entries, err := l.rangeChunks(ctx, chunkKey, chunk, sids)
	if err != nil {
		return true // Skip chunks that fail to process
	}

	// Process only the filtered entries
	for entry := range entries {
		ts := entry.Time(day)
		if !ts.Before(from) && !ts.After(to) && !yield(ts, entry.Text()) {
			return false // Stop iteration
		}
	}

	return true
}

// rangeChunks downloads the log section from a chunk file, decompresses it, and returns
// an iterator over log entries that are filtered using an optimized bitmap iterator merge algorithm.
func (l *Service) rangeChunks(ctx context.Context, chunkKey string, chunk codec.ChunkEntry, sids *roaring.Bitmap) (iter.Seq[codec.LogEntry], error) {
	if chunk.DataSize() == 0 || sids.Count() == 0 {
		return func(yield func(codec.LogEntry) bool) {}, nil // Empty iterator
	}

	i0 := int64(chunk.DataAt())
	i1 := i0 + int64(chunk.DataSize()) - 1
	compressed, err := l.s3Client.DownloadRange(ctx, chunkKey, i0, i1)
	if err != nil {
		return nil, fmt.Errorf("failed to download log section: %w", err)
	}

	// Decompress chunk and parse log entries
	buffer, err := l.codec.Decompress(compressed)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress log section: %w", err)
	}

	return func(yield func(codec.LogEntry) bool) {
		sids.Range(func(sidToFind uint32) bool {
			for len(buffer) > 4 {
				entry := codec.LogEntry(buffer)
				size := entry.Size()
				if size == 0 || uint32(len(buffer)) < size {
					return false // Invalid size or not enough data, stop iteration
				}

				entryID := entry.ID()
				if entryID < sidToFind {
					buffer = buffer[size:] // Advance buffer
					continue               // Continue scanning buffer for current sid
				}

				// If we found the entry, yield it. If we overshot, the entry is not
				// in the buffer, so we just move to the next sid.
				if entryID == sidToFind {
					if !yield(entry[:size]) {
						return false // Stop iteration if yield returns false
					}
					buffer = buffer[size:] // Advance buffer
				}

				// We either found the entry and yielded it, or we've overshot.
				// In both cases, we are done with this sid and should get the next one.
				return true // Continue to next sid
			}
			return false // Buffer is exhausted, stop iteration
		})
	}, nil
}
