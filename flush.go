// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package tales

import (
	"context"
	"fmt"
	"reflect"

	"github.com/kelindar/tales/internal/buffer"
	"github.com/kelindar/tales/internal/codec"
	"github.com/kelindar/tales/internal/s3"
)

type writerState struct {
	buffer    *buffer.Buffer
	pending   *pendingBatch
	manifests map[string]*codec.Manifest
}

type pendingBatch struct {
	batch    *buffer.Batch
	sequence uint64
	chunk    *codec.ChunkEntry
	err      error
}

func (l *Service) flushState(ctx context.Context, state *writerState) error {
	if state.pending != nil {
		if err := l.persistPending(ctx, state, nil); err != nil {
			return err
		}
	}
	if state.buffer.Size() == 0 {
		return nil
	}

	day := dayKey(state.buffer.Day())
	manifest, err := l.downloadManifest(ctx, day, l.config.WriterID)
	if err != nil {
		return err
	}
	batch, err := state.buffer.Take()
	if err != nil {
		return fmt.Errorf("encode pending batch: %w", err)
	}
	state.pending = &pendingBatch{batch: batch, sequence: manifest.NextSequence()}
	return l.persistPending(ctx, state, manifest)
}

func (l *Service) persistPending(ctx context.Context, state *writerState, manifest *codec.Manifest) error {
	pending := state.pending
	if pending == nil {
		return nil
	}
	day := dayKey(pending.batch.Day)
	var err error
	if manifest == nil {
		manifest, err = l.downloadManifest(ctx, day, l.config.WriterID)
		if err != nil {
			pending.err = err
			return err
		}
	}

	if pending.sequence < uint64(len(manifest.Chunks)) {
		committed := manifest.Chunks[pending.sequence]
		if pending.chunk == nil || !reflect.DeepEqual(committed, *pending.chunk) {
			return fmt.Errorf("writer manifest sequence %d conflicts with pending batch", pending.sequence)
		}
		state.manifests[day] = manifest
		state.pending = nil
		l.invalidateDiscovery(day)
		return nil
	}
	if manifest.NextSequence() != pending.sequence {
		return fmt.Errorf("writer manifest advanced unexpectedly")
	}

	if pending.chunk == nil {
		chunk, err := l.uploadPendingChunk(ctx, day, pending)
		if err != nil {
			pending.err = err
			return err
		}
		pending.chunk = chunk
	}
	if err := l.rejectCompactedDay(ctx, day); err != nil {
		pending.err = err
		return err
	}

	next := &codec.Manifest{Day: day, Writer: l.config.WriterID, Chunks: append([]codec.ChunkEntry(nil), manifest.Chunks...)}
	next.Chunks = append(next.Chunks, *pending.chunk)
	data, err := codec.Encode(next)
	if err != nil {
		return fmt.Errorf("encode writer manifest: %w", err)
	}
	if _, err := l.s3Client.Upload(ctx, keyOfManifest(day, l.config.WriterID), data); err != nil {
		pending.err = fmt.Errorf("publish writer manifest: %w", err)
		return pending.err
	}
	state.manifests[day] = next
	state.pending = nil
	l.invalidateDiscovery(day)
	return nil
}

func (l *Service) uploadPendingChunk(ctx context.Context, day string, pending *pendingBatch) (*codec.ChunkEntry, error) {
	actors := make(map[uint32]codec.Range, len(pending.batch.Indexes))
	var offset int64
	for _, index := range pending.batch.Indexes {
		actors[index.Actor] = codec.Range{Offset: offset, Size: int64(len(index.Data))}
		offset += int64(len(index.Data))
	}
	reader := newLogReader(pending.batch.Indexes, pending.batch.Data.Compressed)
	key := keyOfChunk(day, l.config.WriterID, pending.sequence)
	etag, err := l.s3Client.UploadReader(ctx, key, reader, reader.Size())
	if err != nil {
		return nil, fmt.Errorf("upload writer chunk: %w", err)
	}
	chunk := &codec.ChunkEntry{
		Sequence:   codec.Sequence(pending.sequence),
		Entries:    pending.batch.Entries,
		Time:       pending.batch.Time,
		BitmapSize: offset,
		Data:       codec.Range{Offset: offset, Size: int64(len(pending.batch.Data.Compressed))},
		Size:       reader.Size(),
		ETag:       etag,
		Actors:     actors,
	}
	if err := codec.ValidateChunk(*chunk); err != nil {
		return nil, fmt.Errorf("generated chunk metadata: %w", err)
	}
	return chunk, nil
}

func (l *Service) downloadManifest(ctx context.Context, day, writer string) (*codec.Manifest, error) {
	data, err := l.s3Client.Download(ctx, keyOfManifest(day, writer))
	switch {
	case s3.IsNoSuchKey(err):
		return &codec.Manifest{Day: day, Writer: writer}, nil
	case err != nil:
		return nil, fmt.Errorf("download writer manifest: %w", err)
	}
	manifest, err := codec.Decode[codec.Manifest](data)
	if err != nil {
		return nil, fmt.Errorf("decode writer manifest: %w", err)
	}
	if err := codec.ValidateManifest(manifest, day, writer); err != nil {
		return nil, err
	}
	return manifest, nil
}

func (l *Service) rejectCompactedDay(ctx context.Context, day string) error {
	data, err := l.s3Client.Download(ctx, keyOfCompactMeta(day))
	switch {
	case s3.IsNoSuchKey(err):
		return nil
	case err != nil:
		return fmt.Errorf("check compact metadata: %w", err)
	}
	meta, err := codec.Decode[codec.CompactMetadata](data)
	if err != nil {
		return fmt.Errorf("decode compact metadata: %w", err)
	}
	if err := codec.ValidateCompact(meta, day); err != nil {
		return err
	}
	return fmt.Errorf("day %s is already compacted", day)
}

func newLogReader(bitmaps []buffer.Index, logData []byte) *s3.MultiReader {
	sections := make([][]byte, 0, len(bitmaps)+1)
	for _, bitmap := range bitmaps {
		sections = append(sections, bitmap.Data)
	}
	sections = append(sections, logData)
	return s3.NewMultiReader(sections...)
}

func (l *Service) invalidateDiscovery(day string) {
	l.cacheMu.Lock()
	delete(l.discovery, day)
	l.cacheMu.Unlock()
}

func (l *Service) snapshot(ctx context.Context, state *writerState) (querySnapshot, error) {
	if state.buffer.Size() > 0 {
		day := dayKey(state.buffer.Day())
		_, known := state.manifests[day]
		pendingSameDay := state.pending != nil && state.pending.batch.Day.Equal(state.buffer.Day())
		if !known && !pendingSameDay {
			manifest, err := l.downloadManifest(ctx, day, l.config.WriterID)
			if err != nil {
				return querySnapshot{}, err
			}
			state.manifests[day] = manifest
		}
	}
	snapshot := querySnapshot{cutoffs: make(map[string]writerCutoff)}
	for day, manifest := range state.manifests {
		if len(manifest.Chunks) > 0 {
			snapshot.cutoffs[day] = writerCutoff{sequence: uint64(manifest.Chunks[len(manifest.Chunks)-1].Sequence), committed: true}
		}
	}
	if pending := state.pending; pending != nil {
		day := dayKey(pending.batch.Day)
		snapshot.local = append(snapshot.local, localChunk{day: pending.batch.Day, writer: l.config.WriterID, sequence: pending.sequence, entries: pending.batch.Entries, raw: pending.batch.Raw})
		if _, ok := snapshot.cutoffs[day]; !ok {
			cutoff := writerCutoff{}
			if pending.sequence > 0 {
				cutoff = writerCutoff{sequence: pending.sequence - 1, committed: true}
			}
			snapshot.cutoffs[day] = cutoff
		}
	}
	day, raw, entries := state.buffer.Snapshot()
	if entries > 0 {
		key := dayKey(day)
		sequence := uint64(0)
		if manifest := state.manifests[key]; manifest != nil {
			sequence = manifest.NextSequence()
		}
		if state.pending != nil && state.pending.batch.Day.Equal(day) {
			sequence = state.pending.sequence + 1
		}
		snapshot.local = append(snapshot.local, localChunk{day: day, writer: l.config.WriterID, sequence: sequence, entries: entries, raw: raw})
		if _, ok := snapshot.cutoffs[key]; !ok {
			cutoff := writerCutoff{}
			if sequence > 0 {
				cutoff = writerCutoff{sequence: sequence - 1, committed: true}
			}
			snapshot.cutoffs[key] = cutoff
		}
	}
	return snapshot, nil
}
