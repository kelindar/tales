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

	meta, err := l.downloadMetadata(ctx, day)
	if err != nil {
		return true
	}

	t0 := uint32(max(0, int(from.Sub(day).Minutes())))
	t1 := uint32(min(1440, int(to.Sub(day).Minutes()))) // 1440 minutes = 24 hours
	fromSec := uint32(from.Unix())
	toSec := uint32(to.Unix())

	for _, chunk := range meta.Chunks {
		if !chunk.Between(fromSec, toSec) {
			continue
		}
		if !l.queryDayChunk(ctx, actors, day, from, to, t0, t1, chunk, yield) {
			return false
		}
	}
	return true
}

// queryDayChunk downloads and queries a single chunk for the given actors.
func (l *Service) queryDayChunk(ctx context.Context, actors []uint32, day, from, to time.Time, t0, t1 uint32, chunk codec.ChunkEntry, yield func(time.Time, string) bool) bool {
	indexes, start, ok := chunkActorIndexes(chunk, actors, t0, t1)
	if !ok {
		return true
	}

	chunkKey := keyOfChunk(seq.FormatDate(day), chunk.Offset())
	data, err := l.s3Client.DownloadRange(ctx, chunkKey, start, int64(chunk.DataAt())+int64(chunk.DataSize())-1)
	if err != nil {
		return true
	}

	index := intersectIndexes(data, indexes, start)
	if index == nil || index.Count() == 0 {
		return true
	}

	dataAt := int64(chunk.DataAt()) - start
	if dataAt < 0 || dataAt > int64(len(data)) {
		return true
	}
	return l.queryChunk(data[dataAt:], index, day, from, to, yield)
}

// chunkActorIndexes collects actor index entries within [t0, t1] and the earliest bitmap offset.
func chunkActorIndexes(chunk codec.ChunkEntry, actors []uint32, t0, t1 uint32) ([]codec.IndexEntry, int64, bool) {
	if chunk.DataSize() == 0 {
		return nil, 0, false
	}

	indexes := make([]codec.IndexEntry, 0, len(actors))
	start := int64(chunk.DataAt())
	for _, a := range actors {
		idx, ok := chunk.Actors[a]
		if !ok || uint32(idx[0]) < t0 || uint32(idx[0]) > t1 {
			return nil, 0, false
		}
		indexes = append(indexes, idx)
		if offset := int64(idx[1]); offset < start {
			start = offset
		}
	}
	return indexes, start, len(indexes) > 0
}

// intersectIndexes ANDs actor bitmaps sliced from downloaded chunk data.
func intersectIndexes(data []byte, indexes []codec.IndexEntry, start int64) *roaring.Bitmap {
	var index *roaring.Bitmap
	for _, idx := range indexes {
		i0 := int64(idx[1]) - start
		i1 := i0 + int64(idx[2])
		if i0 < 0 || i1 < i0 || i1 > int64(len(data)) {
			return nil
		}
		bitmap := roaring.FromBytes(data[i0:i1])
		switch {
		case index == nil:
			index = bitmap
		case index.Count() > 0:
			index.And(bitmap)
		}
	}
	return index
}

// queryChunk queries a specific log chunk for sequence IDs using an optimized bitmap iterator.
// This function efficiently filters log entries by leveraging the sorted nature of both the log entries
// and the bitmap, avoiding unnecessary bitmap lookups for entries that don't match.
func (l *Service) queryChunk(compressed []byte, sids *roaring.Bitmap, day, from, to time.Time, yield func(time.Time, string) bool) bool {
	if sids.Count() == 0 {
		return true
	}

	entries, err := l.rangeChunks(compressed, sids)
	if err != nil {
		return true // Skip chunks that fail to process
	}

	for entry := range entries {
		ts := entry.Time(day)
		if !ts.Before(from) && !ts.After(to) && !yield(ts, entry.Text()) {
			return false
		}
	}
	return true
}

// rangeChunks decompresses a log section and returns entries filtered by sequence ID.
func (l *Service) rangeChunks(compressed []byte, sids *roaring.Bitmap) (iter.Seq[codec.LogEntry], error) {
	if len(compressed) == 0 || sids.Count() == 0 {
		return func(yield func(codec.LogEntry) bool) {}, nil
	}

	buffer, err := l.codec.Decompress(compressed)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress log section: %w", err)
	}

	return func(yield func(codec.LogEntry) bool) {
		sids.Range(func(sidToFind uint32) bool {
			return scanForSID(&buffer, sidToFind, yield)
		})
	}, nil
}

// scanForSID advances buffer until sidToFind is found or passed, yielding on match.
func scanForSID(buffer *[]byte, sidToFind uint32, yield func(codec.LogEntry) bool) bool {
	for len(*buffer) > 4 {
		entry := codec.LogEntry(*buffer)
		size := entry.Size()
		if size == 0 || uint32(len(*buffer)) < size {
			return false
		}

		entryID := entry.ID()
		switch {
		case entryID < sidToFind:
			*buffer = (*buffer)[size:]
			continue
		case entryID == sidToFind:
			if !yield(entry[:size]) {
				return false
			}
			*buffer = (*buffer)[size:]
		}
		return true
	}
	return false
}
