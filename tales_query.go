package tales

import (
	"context"
	"iter"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
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
	fromMin := uint32(from.Sub(day).Minutes())
	toMin := uint32(to.Sub(day).Minutes())

	// Build a quick membership map for actors
	actorSet := make(map[uint32]struct{}, len(actors))
	for _, a := range actors {
		actorSet[a] = struct{}{}
	}

	// For each chunk, load all relevant bitmaps and compute intersection
	scratch := make([]byte, 0, 64*1024)
	for _, chunk := range meta.Chunks {
		if chunk.IndexSize() == 0 {
			continue // Skip empty chunks
		}

		// Download entire chunk once
		chunkKey := keyOfChunk(seq.FormatDate(day), chunk.Offset())
		indexData, bitmapData, logData, err := l.loadChunkSections(ctx, chunkKey, chunk)
		if err != nil {
			continue
		}

		// Iterate index entries and compute intersection
		var index *roaring.Bitmap
		for i := 0; i < len(indexData); i += codec.IndexEntrySize {
			entry := codec.IndexEntry(indexData[i : i+codec.IndexEntrySize])
			if _, ok := actorSet[entry.Actor()]; !ok {
				continue
			}
			if !filterEntry(entry, entry.Actor(), fromMin, toMin) {
				continue
			}

			start := int(entry.Offset())
			end := start + int(entry.CompressedSize())
			bmBuf, err := l.codec.DecompressInto(bitmapData[start:end], scratch)
			if err != nil {
				continue
			}
			scratch = bmBuf

			bm := roaring.New()
			if _, err := bm.FromBuffer(bmBuf); err != nil {
				continue
			}

			if index == nil {
				index = bm.Clone()
			} else {
				index.And(bm)
			}
		}

		if index == nil || index.IsEmpty() {
			continue
		}

		logBuf, err := l.codec.DecompressInto(logData, scratch)
		if err != nil {
			continue
		}
		scratch = logBuf

		// Filter and yield matching entries from the log
		buffer := logBuf
		for len(buffer) > 4 {
			entry := codec.LogEntry(buffer)
			size := entry.Size()
			if size == 0 || uint32(len(buffer)) < size {
				break
			}
			id := entry.ID()
			if index.Contains(id) {
				ts := seq.TimeOf(id, day)
				if !ts.Before(from) && !ts.After(to) && !yield(ts, entry.Text()) {
					return false
				}
			}
			buffer = buffer[size:]
		}
	}

	return true
}

// loadIndex downloads and parses the index section from a chunk file, yielding filtered entries.

// filterEntry filters a single index entry by actor and time range.
func filterEntry(entry codec.IndexEntry, actor uint32, fromMin, toMin uint32) bool {
	return entry.Actor() == actor && entry.Time() >= fromMin && entry.Time() <= toMin
}

// loadChunkSections downloads a full chunk and splits it into its three sections.
func (l *Service) loadChunkSections(ctx context.Context, key string, chunk codec.ChunkEntry) ([]byte, []byte, []byte, error) {
	total := chunk.TotalSize()
	data, err := l.s3Client.DownloadRange(ctx, key, 0, int64(total)-1)
	if err != nil {
		return nil, nil, nil, err
	}

	indexEnd := chunk.IndexSize()
	bitmapEnd := indexEnd + chunk.BitmapSize()
	return data[:indexEnd], data[indexEnd:bitmapEnd], data[bitmapEnd:], nil
}
