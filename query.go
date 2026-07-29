// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package tales

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strconv"
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
	base     uint64
	entries  uint32
	raw      []byte
}

type querySnapshot struct {
	local   []localChunk
	cutoffs map[string]writerCutoff
}

type eventRef struct {
	event    Event
	millis   int64
	writer   uint64
	position uint64
}

type invalidBitmapError struct{ err error }

func (e invalidBitmapError) Error() string { return e.err.Error() }
func (e invalidBitmapError) Unwrap() error { return e.err }

// Page returns at most limit matching events from one inclusive bound toward
// the other, ascending when from <= to and descending otherwise. The cursor is
// exclusive; an empty next cursor ends iteration.
func (l *Service) Page(ctx context.Context, from, to time.Time, cursor Cursor, limit int, actors ...uint32) ([]Event, Cursor, error) {
	if err := validatePage(ctx, limit, actors); err != nil {
		return nil, Zero, err
	}
	position, err := decodeCursor(cursor)
	if err != nil {
		return nil, Zero, err
	}
	window, err := newPageWindow(from.UTC(), to.UTC(), position)
	if err != nil {
		return nil, Zero, err
	}
	if err := l.begin(); err != nil {
		return nil, Zero, err
	}
	defer l.active.Done()

	snapshot, err := l.acquireSnapshot(ctx)
	if err != nil {
		return nil, Zero, err
	}
	events, last, more, err := l.collectPage(ctx, snapshot, window, position, uniqueActors(actors), limit)
	if err != nil {
		return nil, Zero, err
	}
	if !more {
		return events, Zero, nil
	}
	next, err := encodeCursor(last)
	if err != nil {
		return nil, Zero, err
	}
	return events, next, nil
}

func validatePage(ctx context.Context, limit int, actors []uint32) error {
	switch {
	case ctx == nil:
		return fmt.Errorf("nil context")
	case ctx.Err() != nil:
		return ctx.Err()
	case len(actors) == 0:
		return fmt.Errorf("no actors specified")
	case limit < 1:
		return fmt.Errorf("limit must be at least 1")
	case limit > 1000:
		return fmt.Errorf("limit exceeds maximum of 1000")
	}
	return nil
}

type pageWindow struct {
	ascending         bool
	lower, upper      time.Time
	firstDay, lastDay time.Time
	step              int
}

func newPageWindow(from, to time.Time, position *cursorPosition) (pageWindow, error) {
	ascending := !from.After(to)
	lower, upper := minTime(from, to), maxTime(from, to)
	if position != nil {
		at := time.UnixMilli(position.millis)
		if at.Before(lower) || at.After(upper) {
			return pageWindow{}, fmt.Errorf("cursor timestamp outside query range")
		}
		switch {
		case ascending:
			lower = at
		default:
			upper = at
		}
	}
	firstDay, lastDay, step := dayOf(lower), dayOf(upper), 1
	if !ascending {
		firstDay, lastDay, step = lastDay, firstDay, -1
	}
	return pageWindow{
		ascending: ascending,
		lower:     lower,
		upper:     upper,
		firstDay:  firstDay,
		lastDay:   lastDay,
		step:      step,
	}, nil
}

func (l *Service) collectPage(ctx context.Context, snapshot querySnapshot, window pageWindow, position *cursorPosition, actors []uint32, limit int) ([]Event, eventRef, bool, error) {
	events := make([]Event, 0, limit)
	var last eventRef
	for day := window.firstDay; ; day = day.AddDate(0, 0, window.step) {
		if err := ctx.Err(); err != nil {
			return nil, eventRef{}, false, err
		}
		dayFrom := maxTime(window.lower, day)
		dayTo := minTime(window.upper, day.Add(24*time.Hour-time.Millisecond))
		found, err := l.queryDay(ctx, snapshot, day, dayFrom, dayTo, actors)
		if err != nil {
			return nil, eventRef{}, false, err
		}
		sortEventRefs(found, window.ascending)

		var full bool
		events, last, full = takePageEvents(events, last, found, position, window.ascending, limit)
		if full || day.Equal(window.lastDay) {
			return events, last, full, nil
		}
	}
}

func takePageEvents(events []Event, last eventRef, found []eventRef, position *cursorPosition, ascending bool, limit int) ([]Event, eventRef, bool) {
	for _, ref := range found {
		if skipBeforeCursor(ref, position, ascending) {
			continue
		}
		if len(events) == limit {
			return events, last, true
		}
		events = append(events, ref.event)
		last = ref
	}
	return events, last, false
}

func skipBeforeCursor(ref eventRef, position *cursorPosition, ascending bool) bool {
	if position == nil {
		return false
	}
	order := compareEventCursor(ref, *position)
	return ascending && order <= 0 || !ascending && order >= 0
}

func (l *Service) scan(ctx context.Context, snapshot querySnapshot, from, to time.Time, actors []uint32, yield func(Event, error) bool) {
	actors = uniqueActors(actors)
	ascending := !from.After(to)
	lower, upper := minTime(from, to), maxTime(from, to)
	firstDay, lastDay, step := dayOf(lower), dayOf(upper), 1
	if !ascending {
		firstDay, lastDay, step = lastDay, firstDay, -1
	}
	for day := firstDay; ; day = day.AddDate(0, 0, step) {
		if err := ctx.Err(); err != nil {
			yield(Event{}, err)
			return
		}
		refs, err := l.queryDay(ctx, snapshot, day, lower, upper, actors)
		if err != nil {
			yield(Event{}, err)
			return
		}
		sortEventRefs(refs, ascending)
		for _, ref := range refs {
			if !yield(ref.event, nil) {
				return
			}
		}
		if day.Equal(lastDay) {
			break
		}
	}
}

func compareEventRefs(a, b eventRef) int {
	if a.millis != b.millis {
		return cmp.Compare(a.millis, b.millis)
	}
	if a.writer != b.writer {
		return cmp.Compare(a.writer, b.writer)
	}
	return cmp.Compare(a.position, b.position)
}

func compareEventCursor(ref eventRef, cursor cursorPosition) int {
	if ref.millis != cursor.millis {
		return cmp.Compare(ref.millis, cursor.millis)
	}
	if ref.writer != cursor.writer {
		return cmp.Compare(ref.writer, cursor.writer)
	}
	return cmp.Compare(ref.position, cursor.position)
}

func sortEventRefs(refs []eventRef, ascending bool) {
	slices.SortFunc(refs, compareEventRefs)
	if !ascending {
		slices.Reverse(refs)
	}
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
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
			localRefs, err := collectRaw(local.raw, local.entries, day, from, to, actors, l.config.WriterID, local.base, nil)
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
		var base uint64
		for _, chunk := range manifest.Chunks {
			chunkRefs, err := l.queryWriterChunk(ctx, snapshot, day, dayName, from, to, actors, manifest.Writer, chunk, base)
			if err != nil {
				return nil, err
			}
			refs = append(refs, chunkRefs...)
			base += uint64(chunk.Entries)
		}
	}
	return refs, nil
}

func (l *Service) queryWriterChunk(ctx context.Context, snapshot querySnapshot, day time.Time, dayName string, from, to time.Time, actors []uint32, writer string, chunk codec.ChunkEntry, base uint64) ([]eventRef, error) {
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
	return collectRaw(raw, chunk.Entries, day, from, to, actors, writer, base, selected)
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
	var writer string
	var base uint64
	for _, source := range meta.Sources {
		if source.Writer != writer {
			writer, base = source.Writer, 0
		}
		if source.Time[0] > toMillis || source.Time[1] < fromMillis || !overlapsGlobal(selected, source.Base, source.Entries) {
			base += uint64(source.Entries)
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
		chunkRefs, err := collectRaw(raw, source.Entries, day, from, to, actors, source.Writer, base, local)
		if err != nil {
			return nil, err
		}
		refs = append(refs, chunkRefs...)
		base += uint64(source.Entries)
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

func collectRaw(raw []byte, expected uint32, day, from, to time.Time, actors []uint32, writer string, base uint64, selected *roaring.Bitmap) ([]eventRef, error) {
	entries, err := codec.ValidateEntries(raw, expected)
	if err != nil {
		return nil, err
	}
	writerID, err := strconv.ParseUint(writer, 16, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid writer ID %q", writer)
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
		eventTime := event.Time()
		if eventTime.Before(from) || eventTime.After(to) {
			continue
		}
		position := base + uint64(ordinal)
		refs = append(refs, eventRef{event: event, millis: eventTime.UnixMilli(), writer: writerID, position: position})
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
	slices.Sort(writers)
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
	if len(actors) < 2 {
		return actors
	}
	actors = slices.Clone(actors)
	slices.Sort(actors)
	return slices.Compact(actors)
}

func containsActors(entry codec.LogEntry, actors []uint32) bool {
	if len(actors) == 0 {
		return false
	}
	// ponytail: nested scans avoid a map allocation for unsynced events;
	// add a reusable set if large actor lists become a measured hot path.
	for _, wanted := range actors {
		found := false
		for actor := range entry.Actors() {
			if actor == wanted {
				found = true
				break
			}
		}
		if !found {
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
