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

type sourceChunk struct {
	writer string
	chunk  codec.ChunkEntry
	key    string
	base   uint64
}

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
	chunks, total, err := collectSourceChunks(manifests, dayName)
	if err != nil {
		return err
	}
	if len(chunks) == 0 {
		return nil
	}

	merged, err := l.mergeSourceBitmaps(ctx, chunks)
	if err != nil {
		return err
	}

	sources, parts, copied := buildCompactSources(chunks, l.config.composeParts)
	if err := l.composeCompactPayload(ctx, dayName, sources, parts, copied); err != nil {
		return err
	}
	index, actorRanges, err := l.uploadCompactIndex(ctx, dayName, merged, total)
	if err != nil {
		return err
	}
	if err := l.validateDirectPayloads(ctx, sources); err != nil {
		return err
	}

	meta := &codec.CompactMetadata{
		Day:         dayName,
		PublishedAt: l.config.now().UTC().UnixMilli(),
		Index:       index,
		Actors:      actorRanges,
		Sources:     sources,
	}
	return l.commitCompactMetadata(ctx, meta)
}

func buildCompactSources(chunks []sourceChunk, partLimit int) ([]codec.CompactSource, []s3lib.CopyPart, []int) {
	sources := make([]codec.CompactSource, len(chunks))
	parts := make([]s3lib.CopyPart, 0, min(len(chunks), partLimit))
	copied := make([]int, 0, cap(parts))
	for i, source := range chunks {
		sources[i] = codec.CompactSource{
			Writer: source.writer, Sequence: source.chunk.Sequence, Base: source.base,
			Entries: source.chunk.Entries, Time: source.chunk.Time,
			Payload: codec.ObjectRange{Key: source.key, ETag: source.chunk.ETag, Offset: source.chunk.Data.Offset, Size: source.chunk.Data.Size},
			Source:  source.key,
		}
		if source.chunk.Data.Size >= s3lib.MinPartSize && len(parts) < partLimit {
			parts = append(parts, s3lib.CopyPart{SourceKey: source.key, ETag: source.chunk.ETag, Offset: source.chunk.Data.Offset, Size: source.chunk.Data.Size})
			copied = append(copied, i)
		}
	}
	return sources, parts, copied
}

func (l *Service) composeCompactPayload(ctx context.Context, day string, sources []codec.CompactSource, parts []s3lib.CopyPart, copied []int) error {
	if len(parts) == 0 {
		return nil
	}
	key := keyOfCompactData(day)
	etag, err := l.s3Client.Compose(ctx, key, parts)
	if err != nil {
		if s3.IsComposeUnsupported(err) {
			return nil
		}
		return err
	}
	var offset int64
	for i, sourceIndex := range copied {
		sources[sourceIndex].Payload = codec.ObjectRange{Key: key, ETag: etag, Offset: offset, Size: parts[i].Size}
		sources[sourceIndex].Copied = true
		offset += parts[i].Size
	}
	object, err := l.s3Client.Stat(ctx, key)
	if err != nil {
		return err
	}
	if object.ETag != etag || object.Size != offset {
		return fmt.Errorf("composed payload validation failed")
	}
	return nil
}

func (l *Service) uploadCompactIndex(ctx context.Context, day string, merged map[uint32]*roaring.Bitmap, entries uint64) (codec.ObjectRange, map[uint32]codec.Range, error) {
	actors := make([]uint32, 0, len(merged))
	for actor := range merged {
		actors = append(actors, actor)
	}
	slices.Sort(actors)
	ranges := make(map[uint32]codec.Range, len(actors))
	var data []byte
	for _, actor := range actors {
		bitmap := merged[actor].ToBytes()
		ranges[actor] = codec.Range{Offset: int64(len(data)), Size: int64(len(bitmap))}
		data = append(data, bitmap...)
		if _, err := decodeBitmap(bitmap, entries); err != nil {
			return codec.ObjectRange{}, nil, fmt.Errorf("validate generated compact index: %w", err)
		}
	}
	key := keyOfCompactIndex(day)
	etag, err := l.s3Client.Upload(ctx, key, data)
	if err != nil {
		return codec.ObjectRange{}, nil, err
	}
	object, err := l.s3Client.Stat(ctx, key)
	if err != nil {
		return codec.ObjectRange{}, nil, err
	}
	if object.ETag != etag || object.Size != int64(len(data)) {
		return codec.ObjectRange{}, nil, fmt.Errorf("compacted index validation failed")
	}
	return codec.ObjectRange{Key: key, ETag: etag, Size: int64(len(data))}, ranges, nil
}

func (l *Service) validateDirectPayloads(ctx context.Context, sources []codec.CompactSource) error {
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
	return nil
}

func (l *Service) commitCompactMetadata(ctx context.Context, meta *codec.CompactMetadata) error {
	if err := codec.ValidateCompact(meta, meta.Day); err != nil {
		return err
	}
	encoded, err := codec.Encode(meta)
	if err != nil {
		return err
	}
	if _, err := l.s3Client.Upload(ctx, keyOfCompactMeta(meta.Day), encoded); err != nil {
		return err
	}
	l.cacheMu.Lock()
	l.compactMeta[meta.Day] = meta
	l.cacheMu.Unlock()
	return nil
}

func collectSourceChunks(manifests []*codec.Manifest, day string) ([]sourceChunk, uint64, error) {
	var chunks []sourceChunk
	var total uint64
	for _, manifest := range manifests {
		for _, chunk := range manifest.Chunks {
			if total+uint64(chunk.Entries) > uint64(math.MaxUint32)+1 {
				return nil, 0, fmt.Errorf("daily ordinal capacity exceeded")
			}
			chunks = append(chunks, sourceChunk{writer: manifest.Writer, chunk: chunk, key: keyOfChunk(day, manifest.Writer, uint64(chunk.Sequence)), base: total})
			total += uint64(chunk.Entries)
		}
	}
	return chunks, total, nil
}

func (l *Service) mergeSourceBitmaps(ctx context.Context, chunks []sourceChunk) (map[uint32]*roaring.Bitmap, error) {
	merged := make(map[uint32]*roaring.Bitmap)
	for _, source := range chunks {
		if err := l.mergeSourceBitmap(ctx, source, merged); err != nil {
			return nil, err
		}
	}
	return merged, nil
}

func (l *Service) mergeSourceBitmap(ctx context.Context, source sourceChunk, merged map[uint32]*roaring.Bitmap) error {
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
		if err := shiftBitmap(destination, bitmap, source.base); err != nil {
			return err
		}
	}
	return nil
}

func shiftBitmap(destination, source *roaring.Bitmap, base uint64) error {
	var overflow bool
	source.Range(func(ordinal uint32) bool {
		shifted := base + uint64(ordinal)
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
