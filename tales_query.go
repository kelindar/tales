// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package tales

import (
	"context"
	"fmt"
	"github.com/kelindar/async"
	"iter"
	"time"

	"github.com/kelindar/tales/internal/codec"
	"github.com/kelindar/tales/internal/seq"
	"github.com/weaviate/sroar"
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
	t0 := uint32(from.Sub(day).Minutes())
	t1 := uint32(to.Sub(day).Minutes())

	// Pre-compute time range in seconds for chunk filtering
	fromSec := uint32(from.Unix())
	toSec := uint32(to.Unix())

	// Launch chunk tasks concurrently
	var tasks []async.Task[iter.Seq[codec.LogEntry]]
	for _, chunk := range meta.Chunks {
		if !chunk.Between(fromSec, toSec) {
			continue
		}
		chunkKey := keyOfChunk(seq.FormatDate(day), chunk.Offset())
		task := l.chunkTask(ctx, chunkKey, chunk, actors, t0, t1)
		l.jobs <- task
		tasks = append(tasks, task)
	}

	// Consume results in order
	for i, task := range tasks {
		entries, err := task.Outcome()
		if err != nil {
			continue
		}
		for entry := range entries {
			ts := seq.TimeOf(entry.ID(), day)
			if !ts.Before(from) && !ts.After(to) && !yield(ts, entry.Text()) {
				for _, t := range tasks[i+1:] {
					t.Cancel()
				}
				return false
			}
		}
	}

	return true
}

// loadBitmap downloads and decodes a single bitmap for a given index entry.
func (l *Service) loadBitmap(ctx context.Context, key string, entry codec.IndexEntry) (*sroar.Bitmap, error) {
	i0 := int64(entry[1])
	i1 := i0 + int64(entry[2]) - 1

	data, err := l.s3Client.DownloadRange(ctx, key, i0, i1)
	if err != nil {
		return nil, fmt.Errorf("failed to download bitmap chunk: %w", err)
	}

	// Bitmaps are stored uncompressed, so just deserialize
	bm := sroar.FromBuffer(data)
	return bm, nil
}

// queryChunk queries a specific log chunk for sequence IDs using an optimized bitmap iterator.
// This function efficiently filters log entries by leveraging the sorted nature of both the log entries
// and the bitmap, avoiding unnecessary bitmap lookups for entries that don't match.
func (l *Service) queryChunk(ctx context.Context, chunkKey string, chunk codec.ChunkEntry, sids sroar.Bitmap, day, from, to time.Time, yield func(time.Time, string) bool) bool {
	if sids.IsEmpty() {
		return true
	}

	entries, err := l.rangeChunks(ctx, chunkKey, chunk, &sids)
	if err != nil {
		return true // Skip chunks that fail to process
	}

	// Process only the filtered entries
	for entry := range entries {
		id := entry.ID()
		ts := seq.TimeOf(id, day)
		if !ts.Before(from) && !ts.After(to) && !yield(ts, entry.Text()) {
			return false // Stop iteration
		}
	}

	return true
}

// rangeChunks downloads the log section from a chunk file, decompresses it, and returns
// an iterator over log entries that are filtered using an optimized bitmap iterator merge algorithm.
func (l *Service) rangeChunks(ctx context.Context, chunkKey string, chunk codec.ChunkEntry, sids *sroar.Bitmap) (iter.Seq[codec.LogEntry], error) {
	if chunk.DataSize() == 0 || sids.IsEmpty() {
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
		iter := sids.NewIterator()
		idx := iter.Next()
		for len(buffer) > 4 && idx != 0 {
			entry := codec.LogEntry(buffer)
			size := entry.Size()
			if size == 0 || uint32(len(buffer)) < size {
				return // Invalid size or not enough data, stop iteration
			}

			// Advance bitmap iterator until we find a target >= current entry ID
			entryID := uint64(entry.ID())
			for idx != 0 && idx < entryID {
				idx = iter.Next()
			}

			// If we've exhausted all targets, we're done
			if idx == 0 {
				return
			}

			// If current entry matches current target, yield it
			if entryID == idx {
				if !yield(entry[:size]) {
					return // Stop iteration if yield returns false
				}
				idx = iter.Next()
			}

			// Always advance buffer after processing each entry
			buffer = buffer[size:]
		}
	}, nil
}

// chunkTask prepares a task that downloads bitmaps and data for a single chunk.
func (l *Service) chunkTask(ctx context.Context, chunkKey string, chunk codec.ChunkEntry, actors []uint32, t0, t1 uint32) async.Task[iter.Seq[codec.LogEntry]] {
	return async.NewTask(func(ctx context.Context) (iter.Seq[codec.LogEntry], error) {
		var index *sroar.Bitmap
		for _, a := range actors {
			idx, ok := chunk.Actors[a]
			if !ok || uint32(idx[0]) < t0 || uint32(idx[0]) > t1 {
				index = nil
				break
			}

			bitmap, err := l.loadBitmap(ctx, chunkKey, idx)
			if err != nil {
				return nil, err
			}

			switch {
			case index == nil:
				index = bitmap
			case !index.IsEmpty():
				index.And(bitmap)
			}
		}

		if index == nil || index.IsEmpty() {
			return func(yield func(codec.LogEntry) bool) {}, nil
		}

		return l.rangeChunks(ctx, chunkKey, chunk, index)
	})
}
