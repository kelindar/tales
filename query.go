// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package tales

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/kelindar/roaring"
	"github.com/kelindar/tales/internal/codec"
	"github.com/kelindar/tales/internal/s3"
)

type discoveryCache struct {
	at        time.Time
	manifests []*codec.Manifest
}

type writerCutoff struct {
	sequence  uint64
	committed bool
}

type localChunk struct {
	day      time.Time
	sequence uint64
	entries  uint32
	raw      []byte
}

type querySnapshot struct {
	local   []localChunk
	cutoffs map[string]writerCutoff
}

type eventRef struct {
	event    Event
	writer   string
	sequence uint64
	ordinal  uint32
}

type invalidBitmapError struct{ err error }

func (e invalidBitmapError) Error() string { return e.err.Error() }
func (e invalidBitmapError) Unwrap() error { return e.err }

func (l *Service) query(ctx context.Context, snapshot querySnapshot, from, to time.Time, actors []uint32, yield func(Event, error) bool) {
	actors = uniqueActors(actors)
	for day := dayOf(from); !day.After(dayOf(to)); day = day.AddDate(0, 0, 1) {
		if err := ctx.Err(); err != nil {
			yield(Event{}, err)
			return
		}
		refs, err := l.queryDay(ctx, snapshot, day, from, to, actors)
		if err != nil {
			yield(Event{}, err)
			return
		}
		sort.Slice(refs, func(i, j int) bool {
			a, b := refs[i], refs[j]
			if order := a.event.Time().Compare(b.event.Time()); order != 0 {
				return order < 0
			}
			if a.writer != b.writer {
				return a.writer < b.writer
			}
			if a.sequence != b.sequence {
				return a.sequence < b.sequence
			}
			return a.ordinal < b.ordinal
		})
		for _, ref := range refs {
			if !yield(ref.event, nil) {
				return
			}
		}
	}
}

func (l *Service) queryDay(ctx context.Context, snapshot querySnapshot, day, from, to time.Time, actors []uint32) ([]eventRef, error) {
	key := dayKey(day)
	var refs []eventRef
	meta, compact, err := l.compactMetadata(ctx, key)
	if err != nil {
		return nil, err
	}
	if compact {
		refs, err = l.queryCompactDay(ctx, day, from, to, actors, meta)
		var invalid invalidBitmapError
		if s3.IsNoSuchKey(err) || errors.As(err, &invalid) {
			l.cacheMu.Lock()
			delete(l.compactMeta, key)
			l.cacheMu.Unlock()
			refs, err = l.queryWriterDay(ctx, snapshot, day, from, to, actors)
		}
	} else {
		refs, err = l.queryWriterDay(ctx, snapshot, day, from, to, actors)
	}
	if err != nil {
		return nil, err
	}
	for _, local := range snapshot.local {
		if local.day.Equal(day) {
			localRefs, err := collectRaw(local.raw, local.entries, day, from, to, actors, l.config.WriterID, local.sequence, nil)
			if err != nil {
				return nil, fmt.Errorf("query local snapshot: %w", err)
			}
			refs = append(refs, localRefs...)
		}
	}
	return refs, nil
}

func (l *Service) queryWriterDay(ctx context.Context, snapshot querySnapshot, day, from, to time.Time, actors []uint32) ([]eventRef, error) {
	dayName := dayKey(day)
	manifests, err := l.discoverManifests(ctx, day)
	if err != nil {
		return nil, err
	}
	var refs []eventRef
	for _, manifest := range manifests {
		for _, chunk := range manifest.Chunks {
			chunkRefs, err := l.queryWriterChunk(ctx, snapshot, day, dayName, from, to, actors, manifest.Writer, chunk)
			if err != nil {
				return nil, err
			}
			refs = append(refs, chunkRefs...)
		}
	}
	return refs, nil
}

func (l *Service) queryWriterChunk(ctx context.Context, snapshot querySnapshot, day time.Time, dayName string, from, to time.Time, actors []uint32, writer string, chunk codec.ChunkEntry) ([]eventRef, error) {
	sequence := uint64(chunk.Sequence)
	if writer == l.config.WriterID {
		if cutoff, ok := snapshot.cutoffs[dayName]; ok && (!cutoff.committed || sequence > cutoff.sequence) {
			return nil, nil
		}
	}
	fromMillis, toMillis := queryMillis(day, from, to)
	if !chunk.Between(fromMillis, toMillis) {
		return nil, nil
	}
	key := keyOfChunk(dayName, writer, sequence)
	selected, err := l.chunkOrdinals(ctx, key, chunk.ETag, chunk.Actors, uint64(chunk.Entries), actors)
	if err != nil || selected == nil || selected.Count() == 0 {
		return nil, err
	}
	compressed, err := l.s3Client.DownloadRange(ctx, key, chunk.ETag, chunk.Data.Offset, chunk.Data.Size)
	if err != nil {
		return nil, err
	}
	raw, err := l.codec.Decompress(compressed)
	if err != nil {
		return nil, fmt.Errorf("decompress writer chunk: %w", err)
	}
	return collectRaw(raw, chunk.Entries, day, from, to, actors, writer, sequence, selected)
}

func (l *Service) queryCompactDay(ctx context.Context, day, from, to time.Time, actors []uint32, meta *codec.CompactMetadata) ([]eventRef, error) {
	selected, err := l.chunkOrdinals(ctx, meta.Index.Key, meta.Index.ETag, meta.Actors, compactEntries(meta), actors)
	if err != nil {
		return nil, err
	}
	if selected == nil || selected.Count() == 0 {
		return nil, nil
	}
	var refs []eventRef
	fromMillis, toMillis := queryMillis(day, from, to)
	for _, source := range meta.Sources {
		if source.Time[0] > toMillis || source.Time[1] < fromMillis || !overlapsGlobal(selected, source.Base, source.Entries) {
			continue
		}
		compressed, err := l.s3Client.DownloadRange(ctx, source.Payload.Key, source.Payload.ETag, source.Payload.Offset, source.Payload.Size)
		if err != nil {
			return nil, err
		}
		raw, err := l.codec.Decompress(compressed)
		if err != nil {
			return nil, fmt.Errorf("decompress compact source: %w", err)
		}
		local := roaring.New()
		selected.Range(func(value uint32) bool {
			if uint64(value) >= source.Base && uint64(value) < source.Base+uint64(source.Entries) {
				local.Set(uint32(uint64(value) - source.Base))
			}
			return true
		})
		chunkRefs, err := collectRaw(raw, source.Entries, day, from, to, actors, source.Writer, uint64(source.Sequence), local)
		if err != nil {
			return nil, err
		}
		refs = append(refs, chunkRefs...)
	}
	return refs, nil
}

func (l *Service) chunkOrdinals(ctx context.Context, key, etag string, indexes map[uint32]codec.Range, entries uint64, actors []uint32) (*roaring.Bitmap, error) {
	var selected *roaring.Bitmap
	for _, actor := range actors {
		r, ok := indexes[actor]
		if !ok {
			return nil, nil
		}
		data, err := l.s3Client.DownloadRange(ctx, key, etag, r.Offset, r.Size)
		if err != nil {
			return nil, err
		}
		bitmap, err := decodeBitmap(data, entries)
		if err != nil {
			return nil, invalidBitmapError{err: fmt.Errorf("decode actor %d bitmap: %w", actor, err)}
		}
		if selected == nil {
			selected = bitmap
		} else {
			selected.And(bitmap)
		}
		if selected.Count() == 0 {
			return selected, nil
		}
	}
	return selected, nil
}

func collectRaw(raw []byte, expected uint32, day, from, to time.Time, actors []uint32, writer string, sequence uint64, selected *roaring.Bitmap) ([]eventRef, error) {
	entries, err := codec.ValidateEntries(raw, expected)
	if err != nil {
		return nil, err
	}
	refs := make([]eventRef, 0, len(entries))
	for ordinal, entry := range entries {
		if selected != nil && !selected.Contains(uint32(ordinal)) {
			continue
		}
		if selected == nil && !containsActors(entry, actors) {
			continue
		}
		event := codec.NewEvent(day, entry)
		if event.Time().Before(from) || event.Time().After(to) {
			continue
		}
		refs = append(refs, eventRef{event: event, writer: writer, sequence: sequence, ordinal: uint32(ordinal)})
	}
	return refs, nil
}

func (l *Service) discoverManifests(ctx context.Context, day time.Time) ([]*codec.Manifest, error) {
	key := dayKey(day)
	now := l.config.now().UTC()
	historical := day.Before(dayOf(now).AddDate(0, 0, -1))
	l.cacheMu.Lock()
	cached, ok := l.discovery[key]
	l.cacheMu.Unlock()
	if ok && (historical || now.Sub(cached.at) < l.config.ChunkInterval) {
		return cached.manifests, nil
	}

	prefix := key + "/writers"
	var writers []string
	for object, err := range l.s3Client.List(ctx, prefix) {
		if err != nil {
			return nil, err
		}
		if object.Dir {
			writer := path.Base(object.Key)
			if len(writer) == 16 {
				writers = append(writers, writer)
			}
		} else if strings.HasSuffix(object.Key, "/manifest.json") {
			writers = append(writers, path.Base(path.Dir(object.Key)))
		}
	}
	sort.Strings(writers)
	writers = slices.Compact(writers)
	manifests := make([]*codec.Manifest, 0, len(writers))
	for _, writer := range writers {
		manifest, err := l.downloadManifest(ctx, key, writer)
		if err != nil {
			return nil, err
		}
		manifests = append(manifests, manifest)
	}
	l.cacheMu.Lock()
	l.discovery[key] = discoveryCache{at: now, manifests: manifests}
	l.cacheMu.Unlock()
	return manifests, nil
}

func (l *Service) compactMetadata(ctx context.Context, day string) (*codec.CompactMetadata, bool, error) {
	l.cacheMu.Lock()
	cached := l.compactMeta[day]
	l.cacheMu.Unlock()
	if cached != nil {
		return cached, true, nil
	}
	data, err := l.s3Client.Download(ctx, keyOfCompactMeta(day))
	switch {
	case s3.IsNoSuchKey(err):
		return nil, false, nil
	case err != nil:
		return nil, false, err
	}
	meta, err := codec.Decode[codec.CompactMetadata](data)
	if err != nil || codec.ValidateCompact(meta, day) != nil {
		return nil, false, nil
	}
	index, err := l.s3Client.Stat(ctx, meta.Index.Key)
	switch {
	case s3.IsNoSuchKey(err):
		return nil, false, nil
	case err != nil:
		return nil, false, err
	case index.Size != meta.Index.Size || index.ETag != meta.Index.ETag:
		return nil, false, nil
	}
	l.cacheMu.Lock()
	l.compactMeta[day] = meta
	l.cacheMu.Unlock()
	return meta, true, nil
}

func decodeBitmap(data []byte, entries uint64) (*roaring.Bitmap, error) {
	bitmap := roaring.New()
	n, err := bitmap.ReadFrom(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	if n != int64(len(data)) {
		return nil, fmt.Errorf("trailing bitmap bytes")
	}
	if max, ok := bitmap.Max(); ok && (entries == 0 || uint64(max) >= entries) {
		return nil, fmt.Errorf("bitmap ordinal %d exceeds entry count %d", max, entries)
	}
	return bitmap, nil
}

func uniqueActors(actors []uint32) []uint32 {
	actors = slices.Clone(actors)
	slices.Sort(actors)
	return slices.Compact(actors)
}

func containsActors(entry codec.LogEntry, actors []uint32) bool {
	if len(actors) == 0 {
		return false
	}
	have := make(map[uint32]struct{})
	for actor := range entry.Actors() {
		have[actor] = struct{}{}
	}
	for _, actor := range actors {
		if _, ok := have[actor]; !ok {
			return false
		}
	}
	return true
}

func queryMillis(day, from, to time.Time) (uint32, uint32) {
	start := max(time.Duration(0), from.Sub(day))
	end := min(24*time.Hour-time.Millisecond, to.Sub(day))
	return uint32(start / time.Millisecond), uint32(max(time.Duration(0), end) / time.Millisecond)
}

func compactEntries(meta *codec.CompactMetadata) uint64 {
	if len(meta.Sources) == 0 {
		return 0
	}
	last := meta.Sources[len(meta.Sources)-1]
	return last.Base + uint64(last.Entries)
}

func overlapsGlobal(bitmap *roaring.Bitmap, base uint64, count uint32) bool {
	found := false
	bitmap.Range(func(value uint32) bool {
		found = uint64(value) >= base && uint64(value) < base+uint64(count)
		return !found
	})
	return found
}
