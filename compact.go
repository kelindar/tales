// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package tales

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sort"
	"time"

	"github.com/kelindar/roaring"
	s3lib "github.com/kelindar/s3"
	"github.com/kelindar/tales/internal/codec"
	"github.com/kelindar/tales/internal/s3"
)

const cleanupGrace = 24 * time.Hour

// Compact commits an immutable merged index for an eligible historical UTC day.
func (l *Service) Compact(ctx context.Context, value time.Time) error {
	if ctx == nil {
		return fmt.Errorf("nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := l.begin(); err != nil {
		return err
	}
	defer l.active.Done()

	day := dayOf(value)
	today := dayOf(l.config.now())
	if !day.Before(today.AddDate(0, 0, -1)) {
		return fmt.Errorf("day %s is too recent to compact", dayKey(day))
	}
	dayName := dayKey(day)
	if existing, ok, err := l.compactMetadata(ctx, dayName); err != nil {
		return err
	} else if ok {
		return l.cleanupCompacted(ctx, existing)
	}

	manifests, err := l.discoverManifests(ctx, day)
	if err != nil {
		return err
	}
	type sourceChunk struct {
		writer string
		chunk  codec.ChunkEntry
		key    string
		base   uint64
	}
	var chunks []sourceChunk
	var total uint64
	for _, found := range manifests {
		for _, chunk := range found.manifest.Chunks {
			if total+uint64(chunk.Entries) > uint64(math.MaxUint32)+1 {
				return fmt.Errorf("daily ordinal capacity exceeded")
			}
			chunks = append(chunks, sourceChunk{writer: found.writer, chunk: chunk, key: keyOfChunk(dayName, found.writer, uint64(chunk.Sequence)), base: total})
			total += uint64(chunk.Entries)
		}
	}
	if len(chunks) == 0 {
		return nil
	}

	merged := make(map[uint32]*roaring.Bitmap)
	for _, source := range chunks {
		actors := make([]uint32, 0, len(source.chunk.Actors))
		for actor := range source.chunk.Actors {
			actors = append(actors, actor)
		}
		slices.Sort(actors)
		for _, actor := range actors {
			r := source.chunk.Actors[actor]
			data, err := l.s3Client.DownloadRange(ctx, source.key, source.chunk.ETag, r.Offset, r.Size)
			if err != nil {
				return err
			}
			bitmap, err := decodeBitmap(data, uint64(source.chunk.Entries))
			if err != nil {
				return fmt.Errorf("decode source bitmap: %w", err)
			}
			destination := merged[actor]
			if destination == nil {
				destination = roaring.New()
				merged[actor] = destination
			}
			var overflow bool
			bitmap.Range(func(ordinal uint32) bool {
				shifted := source.base + uint64(ordinal)
				if shifted > math.MaxUint32 {
					overflow = true
					return false
				}
				destination.Set(uint32(shifted))
				return true
			})
			if overflow {
				return fmt.Errorf("daily ordinal capacity exceeded")
			}
		}
	}

	sources := make([]codec.CompactSource, len(chunks))
	parts := make([]s3lib.CopyPart, 0, min(len(chunks), l.config.composeParts))
	copied := make([]int, 0, cap(parts))
	for i, source := range chunks {
		sources[i] = codec.CompactSource{
			Writer:   source.writer,
			Sequence: source.chunk.Sequence,
			Base:     source.base,
			Entries:  source.chunk.Entries,
			Time:     source.chunk.Time,
			Payload:  codec.ObjectRange{Key: source.key, ETag: source.chunk.ETag, Offset: source.chunk.Data.Offset, Size: source.chunk.Data.Size},
			Source:   source.key,
		}
		if source.chunk.Data.Size >= s3lib.MinPartSize && len(parts) < l.config.composeParts {
			parts = append(parts, s3lib.CopyPart{SourceKey: source.key, ETag: source.chunk.ETag, Offset: source.chunk.Data.Offset, Size: source.chunk.Data.Size})
			copied = append(copied, i)
		}
	}

	if len(parts) > 0 {
		etag, err := l.s3Client.Compose(ctx, keyOfCompactData(dayName), parts)
		if err != nil && !s3.IsComposeUnsupported(err) {
			return err
		}
		if err == nil {
			var offset int64
			for i, sourceIndex := range copied {
				sources[sourceIndex].Payload = codec.ObjectRange{Key: keyOfCompactData(dayName), ETag: etag, Offset: offset, Size: parts[i].Size}
				sources[sourceIndex].Copied = true
				offset += parts[i].Size
			}
			object, err := l.s3Client.Stat(ctx, keyOfCompactData(dayName))
			if err != nil {
				return err
			}
			if object.ETag != etag || object.Size != offset {
				return fmt.Errorf("composed payload validation failed")
			}
		}
	}

	actorIDs := make([]uint32, 0, len(merged))
	for actor := range merged {
		actorIDs = append(actorIDs, actor)
	}
	slices.Sort(actorIDs)
	actorRanges := make(map[uint32]codec.Range, len(actorIDs))
	var indexData []byte
	for _, actor := range actorIDs {
		data := merged[actor].ToBytes()
		actorRanges[actor] = codec.Range{Offset: int64(len(indexData)), Size: int64(len(data))}
		indexData = append(indexData, data...)
		if _, err := decodeBitmap(data, total); err != nil {
			return fmt.Errorf("validate generated compact index: %w", err)
		}
	}
	indexETag, err := l.s3Client.Upload(ctx, keyOfCompactIndex(dayName), indexData)
	if err != nil {
		return err
	}
	indexObject, err := l.s3Client.Stat(ctx, keyOfCompactIndex(dayName))
	if err != nil {
		return err
	}
	if indexObject.ETag != indexETag || indexObject.Size != int64(len(indexData)) {
		return fmt.Errorf("compacted index validation failed")
	}

	for _, source := range sources {
		if source.Copied {
			continue
		}
		object, err := l.s3Client.Stat(ctx, source.Source)
		if err != nil {
			return err
		}
		if object.ETag != source.Payload.ETag || source.Payload.Offset+source.Payload.Size > object.Size {
			return fmt.Errorf("direct payload validation failed")
		}
	}

	meta := &codec.CompactMetadata{
		Day:         dayName,
		PublishedAt: l.config.now().UTC().UnixMilli(),
		Index:       codec.ObjectRange{Key: keyOfCompactIndex(dayName), ETag: indexETag, Size: int64(len(indexData))},
		Actors:      actorRanges,
		Sources:     sources,
	}
	if err := codec.ValidateCompact(meta, dayName); err != nil {
		return err
	}
	encoded, err := codec.Encode(meta)
	if err != nil {
		return err
	}
	if _, err := l.s3Client.Upload(ctx, keyOfCompactMeta(dayName), encoded); err != nil {
		return err
	}
	l.cacheMu.Lock()
	l.compactMeta[dayName] = meta
	l.cacheMu.Unlock()
	return nil
}

func (l *Service) cleanupCompacted(ctx context.Context, meta *codec.CompactMetadata) error {
	published := time.UnixMilli(meta.PublishedAt)
	if l.config.now().UTC().Sub(published) < cleanupGrace {
		return nil
	}
	sources := slices.Clone(meta.Sources)
	sort.Slice(sources, func(i, j int) bool { return sources[i].Source < sources[j].Source })
	for _, source := range sources {
		if !source.Copied {
			continue
		}
		if err := l.s3Client.Delete(ctx, source.Source); err != nil && !s3.IsNoSuchKey(err) {
			return fmt.Errorf("cleanup compacted source: %w", err)
		}
	}
	return nil
}
