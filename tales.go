// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package tales

import (
	"context"
	"fmt"
	"iter"
	"sync"
	"time"

	"github.com/kelindar/tales/internal/buffer"
	"github.com/kelindar/tales/internal/codec"
	"github.com/kelindar/tales/internal/s3"
)

const chunkTarget = 8 << 20

// Event is a validated log event. Its projections are zero-copy and read-only;
// use Clone before retaining or modifying their backing data.
type Event = codec.Event

// Logger accepts events into the service's in-memory writer state.
type Logger interface {
	Log(text string, actors ...uint32) error
}

// Querier reads events in deterministic order within inclusive time bounds.
type Querier interface {
	Query(context.Context, time.Time, time.Time, ...uint32) iter.Seq2[Event, error]
}

// Syncer makes all previously accepted events durable.
type Syncer interface {
	Sync(context.Context) error
}

// Compactor compacts an eligible historical UTC day.
type Compactor interface {
	Compact(context.Context, time.Time) error
}

// Manager is the primary service contract.
type Manager interface {
	Logger
	Querier
	Syncer
	Close() error
}

var (
	_ Manager   = (*Service)(nil)
	_ Compactor = (*Service)(nil)
)

type logCmd struct {
	day   time.Time
	entry codec.LogEntry
	reply chan error
}

type syncCmd struct {
	ctx   context.Context
	reply chan error
}

type snapshotCmd struct {
	ctx   context.Context
	reply chan snapshotResult
}

type snapshotResult struct {
	snapshot querySnapshot
	err      error
}

type closeCmd struct {
	reply chan error
}

type command struct {
	log      *logCmd
	sync     *syncCmd
	snapshot *snapshotCmd
	close    *closeCmd
}

// Service is a distributed S3-backed event log owned by one writer ID.
type Service struct {
	config   config
	s3Client s3.Client
	codec    *codec.Codec
	commands chan command
	worker   sync.WaitGroup

	lifecycle sync.Mutex
	active    sync.WaitGroup
	closing   bool
	closed    bool

	cacheMu     sync.Mutex
	discovery   map[string]discoveryCache
	compactMeta map[string]*codec.CompactMetadata
}

// New opens a service for the given S3 bucket and region.
func New(bucket, region string, opts ...Option) (*Service, error) {
	cfg := config{S3: s3.Config{Bucket: bucket, Region: region}}
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.setDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	c, err := codec.NewCodec()
	if err != nil {
		return nil, fmt.Errorf("create codec: %w", err)
	}
	client, err := newS3Client(cfg)
	if err != nil {
		c.Close()
		return nil, err
	}

	service := &Service{
		config:      cfg,
		s3Client:    client,
		codec:       c,
		commands:    make(chan command, cfg.BufferSize),
		discovery:   make(map[string]discoveryCache),
		compactMeta: make(map[string]*codec.CompactMetadata),
	}
	service.worker.Add(1)
	go service.run()
	return service, nil
}

func newS3Client(cfg config) (s3.Client, error) {
	if cfg.NewClient != nil {
		return cfg.NewClient(cfg.S3)
	}
	return s3.NewClient(cfg.S3)
}

func (l *Service) begin() error {
	l.lifecycle.Lock()
	defer l.lifecycle.Unlock()
	if l.closing || l.closed {
		return fmt.Errorf("tales service is closed")
	}
	l.active.Add(1)
	return nil
}

// Log accepts an event into local memory for every supplied actor.
func (l *Service) Log(text string, actors ...uint32) error {
	switch {
	case text == "":
		return fmt.Errorf("empty log entry")
	case len(actors) == 0:
		return fmt.Errorf("no actors specified")
	case len(actors) > 1000:
		return fmt.Errorf("too many actors (max 1000)")
	}
	now := l.config.now().UTC()
	day := dayOf(now)
	entry, err := codec.NewLogEntry(uint32(now.Sub(day)/time.Millisecond), text, actors)
	if err != nil {
		return err
	}
	if err := l.begin(); err != nil {
		return err
	}
	defer l.active.Done()

	reply := make(chan error, 1)
	l.commands <- command{log: &logCmd{day: day, entry: entry, reply: reply}}
	return <-reply
}

// Sync makes every previously accepted event durable.
func (l *Service) Sync(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("nil context")
	}
	if err := l.begin(); err != nil {
		return err
	}
	defer l.active.Done()

	reply := make(chan error, 1)
	select {
	case l.commands <- command{sync: &syncCmd{ctx: ctx, reply: reply}}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Query yields events containing every actor within inclusive time bounds.
func (l *Service) Query(ctx context.Context, from, to time.Time, actors ...uint32) iter.Seq2[Event, error] {
	return func(yield func(Event, error) bool) {
		switch {
		case ctx == nil:
			yield(Event{}, fmt.Errorf("nil context"))
			return
		case from.After(to) || len(actors) == 0:
			yield(Event{}, fmt.Errorf("invalid query arguments"))
			return
		}
		if err := l.begin(); err != nil {
			yield(Event{}, err)
			return
		}
		defer l.active.Done()

		reply := make(chan snapshotResult, 1)
		select {
		case l.commands <- command{snapshot: &snapshotCmd{ctx: ctx, reply: reply}}:
		case <-ctx.Done():
			yield(Event{}, ctx.Err())
			return
		}
		var result snapshotResult
		select {
		case result = <-reply:
		case <-ctx.Done():
			yield(Event{}, ctx.Err())
			return
		}
		if result.err != nil {
			yield(Event{}, result.err)
			return
		}
		l.query(ctx, result.snapshot, from.UTC(), to.UTC(), actors, yield)
	}
}

// Close syncs pending events and releases the service after a successful flush.
func (l *Service) Close() error {
	l.lifecycle.Lock()
	if l.closing || l.closed {
		l.lifecycle.Unlock()
		return fmt.Errorf("tales service is closed")
	}
	l.closing = true
	l.lifecycle.Unlock()

	l.active.Wait()
	reply := make(chan error, 1)
	l.commands <- command{close: &closeCmd{reply: reply}}
	if err := <-reply; err != nil {
		l.lifecycle.Lock()
		l.closing = false
		l.lifecycle.Unlock()
		return err
	}

	l.worker.Wait()
	l.codec.Close()
	l.lifecycle.Lock()
	l.closed = true
	l.closing = false
	l.lifecycle.Unlock()
	return nil
}

func (l *Service) run() {
	defer l.worker.Done()
	state := writerState{buffer: buffer.New(l.config.BufferSize, l.codec), manifests: make(map[string]*codec.Manifest)}
	ticker := time.NewTicker(l.config.ChunkInterval)
	defer ticker.Stop()

	for {
		select {
		case cmd := <-l.commands:
			switch {
			case cmd.log != nil:
				cmd.log.reply <- l.accept(&state, cmd.log)
			case cmd.sync != nil:
				cmd.sync.reply <- l.flushState(cmd.sync.ctx, &state)
			case cmd.snapshot != nil:
				snapshot, err := l.snapshot(cmd.snapshot.ctx, &state)
				cmd.snapshot.reply <- snapshotResult{snapshot: snapshot, err: err}
			case cmd.close != nil:
				err := l.flushState(context.Background(), &state)
				cmd.close.reply <- err
				if err == nil {
					return
				}
			}
		case <-ticker.C:
			_ = l.flushState(context.Background(), &state)
		}
	}
}

func (l *Service) accept(state *writerState, cmd *logCmd) error {
	if state.buffer.Size() > 0 && !state.buffer.Day().Equal(cmd.day) {
		if err := l.flushState(context.Background(), state); err != nil {
			return err
		}
	}
	if state.buffer.Size() >= l.config.BufferSize {
		if err := l.flushState(context.Background(), state); err != nil {
			return err
		}
	}
	if err := state.buffer.Add(cmd.day, cmd.entry); err != nil {
		return err
	}
	if state.buffer.Bytes() >= chunkTarget {
		return l.flushState(context.Background(), state)
	}
	return nil
}

func dayOf(value time.Time) time.Time {
	y, m, d := value.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func dayKey(day time.Time) string { return day.UTC().Format("2006-01-02") }

func keyOfManifest(day, writer string) string {
	return fmt.Sprintf("%s/writers/%s/manifest.json", day, writer)
}

func keyOfChunk(day, writer string, sequence uint64) string {
	return fmt.Sprintf("%s/writers/%s/%020d.log", day, writer, sequence)
}

func keyOfCompactIndex(day string) string { return day + "/compact/index.bin" }
func keyOfCompactData(day string) string  { return day + "/compact/data.log" }
func keyOfCompactMeta(day string) string  { return day + "/compact/metadata.json" }
