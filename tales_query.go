// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package tales

import (
	"context"
	"fmt"
	"iter"
	"time"

	"github.com/kelindar/tales/internal/codec"
	"github.com/kelindar/tales/internal/seq"
	"github.com/weaviate/sroar"
)

// chunkTask represents a chunk processing task
type chunkTask struct {
	index int
	chunk codec.ChunkEntry
	key   string
}

// chunkResult represents the result of processing a chunk
type chunkResult struct {
	index   int
	entries []chunkEntry
	err     error
}

// chunkEntry represents a single log entry with its timestamp
type chunkEntry struct {
	timestamp time.Time
	text      string
}

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

	// Filter relevant chunks first
	var tasks []chunkTask
	for i, chunk := range meta.Chunks {
		// Skip chunks that don't overlap with query time range
		if !chunk.Between(fromSec, toSec) {
			continue
		}
		tasks = append(tasks, chunkTask{
			index: i,
			chunk: chunk,
			key:   keyOfChunk(seq.FormatDate(day), chunk.Offset()),
		})
	}

	if len(tasks) == 0 {
		return true
	}

	// Process chunks in parallel
	return l.processChunksParallel(ctx, tasks, actors, t0, t1, day, from, to, yield)
}

// processChunksParallel processes chunks using a worker pool to parallelize downloads
func (l *Service) processChunksParallel(ctx context.Context, tasks []chunkTask, actors []uint32, t0, t1 uint32, day, from, to time.Time, yield func(time.Time, string) bool) bool {
	if len(tasks) == 0 {
		return true
	}

	// Use sequential processing if only one task or parallel downloads is 1
	if len(tasks) == 1 || l.config.ParallelDownloads == 1 {
		return l.processChunkSequential(ctx, tasks[0], actors, t0, t1, day, from, to, yield)
	}

	// Create channels for work distribution
	taskChan := make(chan chunkTask, len(tasks))
	resultChan := make(chan chunkResult, len(tasks))

	// Create worker pool
	numWorkers := l.config.ParallelDownloads
	if numWorkers > len(tasks) {
		numWorkers = len(tasks)
	}

	// Start workers
	for i := 0; i < numWorkers; i++ {
		go l.chunkWorker(ctx, taskChan, resultChan, actors, t0, t1, day, from, to)
	}

	// Send tasks to workers
	go func() {
		defer close(taskChan)
		for _, task := range tasks {
			select {
			case taskChan <- task:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Collect results and maintain order
	results := make([]chunkResult, len(tasks))
	for i := 0; i < len(tasks); i++ {
		select {
		case result := <-resultChan:
			results[result.index] = result
		case <-ctx.Done():
			return false
		}
	}

	// Process results in order to maintain chronological order
	for _, result := range results {
		if result.err != nil {
			continue // Skip chunks that failed to process
		}
		for _, entry := range result.entries {
			if !yield(entry.timestamp, entry.text) {
				return false
			}
		}
	}

	return true
}

// processChunkSequential processes a single chunk (used for single chunk or when parallelism is disabled)
func (l *Service) processChunkSequential(ctx context.Context, task chunkTask, actors []uint32, t0, t1 uint32, day, from, to time.Time, yield func(time.Time, string) bool) bool {
	var index *sroar.Bitmap
	for _, a := range actors {
		idx, ok := task.chunk.Actors[a]
		if !ok || uint32(idx[0]) < t0 || uint32(idx[0]) > t1 {
			index = nil
			break
		}

		bitmap, err := l.loadBitmap(ctx, task.key, idx)
		if err != nil {
			index = nil
			break
		}

		switch {
		case index == nil:
			index = bitmap
		case !index.IsEmpty():
			index.And(bitmap)
		}
	}

	// Query log section with intersection bitmap
	if index != nil && !index.IsEmpty() {
		return l.queryChunk(ctx, task.key, task.chunk, *index, day, from, to, yield)
	}
	return true
}

// chunkWorker processes chunks from the task channel
func (l *Service) chunkWorker(ctx context.Context, taskChan <-chan chunkTask, resultChan chan<- chunkResult, actors []uint32, t0, t1 uint32, day, from, to time.Time) {
	for {
		select {
		case task, ok := <-taskChan:
			if !ok {
				return
			}
			
			result := chunkResult{index: task.index}
			
			// Process bitmap intersection
			var index *sroar.Bitmap
			for _, a := range actors {
				idx, ok := task.chunk.Actors[a]
				if !ok || uint32(idx[0]) < t0 || uint32(idx[0]) > t1 {
					index = nil
					break
				}

				bitmap, err := l.loadBitmap(ctx, task.key, idx)
				if err != nil {
					result.err = err
					index = nil
					break
				}

				switch {
				case index == nil:
					index = bitmap
				case !index.IsEmpty():
					index.And(bitmap)
				}
			}

			// Process log entries if we have a valid bitmap
			if index != nil && !index.IsEmpty() {
				entries, err := l.collectChunkEntries(ctx, task.key, task.chunk, *index, day, from, to)
				result.entries = entries
				result.err = err
			}

			select {
			case resultChan <- result:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// collectChunkEntries collects all entries from a chunk into a slice
func (l *Service) collectChunkEntries(ctx context.Context, chunkKey string, chunk codec.ChunkEntry, sids sroar.Bitmap, day, from, to time.Time) ([]chunkEntry, error) {
	if sids.IsEmpty() {
		return nil, nil
	}

	entries, err := l.rangeChunks(ctx, chunkKey, chunk, &sids)
	if err != nil {
		return nil, err
	}

	var result []chunkEntry
	for entry := range entries {
		id := entry.ID()
		ts := seq.TimeOf(id, day)
		if !ts.Before(from) && !ts.After(to) {
			result = append(result, chunkEntry{
				timestamp: ts,
				text:      entry.Text(),
			})
		}
	}

	return result, nil
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
