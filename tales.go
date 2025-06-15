package tales

import (
	"context"
	"encoding/binary"
	"fmt"
	"iter"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kelindar/tales/internal/buffer"
	"github.com/kelindar/tales/internal/codec"
	"github.com/kelindar/tales/internal/s3"
	"github.com/kelindar/tales/internal/seq"
)

// logCmd is an alias to a prepared log entry.
type logCmd = codec.LogEntry

// command groups different commands passed through the service channel.
type command struct {
	log   *logCmd
	query *queryCmd
	flush *flushCmd
}

// queryCmd represents a command to query the buffer.
type queryCmd struct {
	actor uint32
	from  time.Time
	to    time.Time
	ret   chan<- iter.Seq[codec.LogEntry]
}

// flushCmd represents a command to flush the buffer.
type flushCmd struct {
	done chan struct{}
}

// Service provides high-performance, memory-efficient logging and querying.
type Service struct {
	config   config
	s3Client s3.Client
	codec    *codec.Codec
	commands chan command
	wg       sync.WaitGroup
	cancel   context.CancelFunc
	closed   int32 // atomic flag
}

// New creates a new logger using the provided S3 bucket and region. Optional
// behaviour can be configured via Option functions.
func New(bucket, region string, opts ...Option) (*Service, error) {
	cfg := config{S3Config: s3.Config{Bucket: bucket, Region: region}}
	for _, opt := range opts {
		opt(&cfg)
	}

	// Set defaults and validate configuration
	cfg.setDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	// Create codec for compression/decompression
	codecInstance, err := codec.NewCodec()
	if err != nil {
		return nil, fmt.Errorf("failed to create codec: %w", err)
	}

	// Create S3 client
	var s3Client s3.Client
	if cfg.NewClient != nil {
		s3Client, err = cfg.NewClient(cfg.S3Config)
	} else {
		s3Client, err = s3.NewClient(cfg.S3Config)
	}
	if err != nil {
		codecInstance.Close()
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	logger := &Service{
		config:   cfg,
		s3Client: s3Client,
		codec:    codecInstance,
		commands: make(chan command, cfg.BufferSize),
		cancel:   cancel,
	}

	// Start the background goroutine
	logger.wg.Add(1)
	go logger.run(ctx)

	return logger, nil
}

// Log adds a log entry with the given text and actors.
func (l *Service) Log(text string, actors ...uint32) error {
	switch {
	case len(text) == 0:
		return fmt.Errorf("empty log entry")
	case len(actors) == 0:
		return fmt.Errorf("no actors specified")
	case len(actors) > 65535:
		return fmt.Errorf("too many actors (max 65535)")
	case len(text) > 65535:
		return fmt.Errorf("text too long (max 65535 bytes)")
	case atomic.LoadInt32(&l.closed) != 0:
		return fmt.Errorf("cannot log to closed logger")
	default:
		entry, _ := codec.NewLogEntry(0, text, actors)
		l.commands <- command{log: (*logCmd)(&entry)}
		return nil
	}
}

// Query returns an iterator over log entries for the specified actor and time range.
func (l *Service) Query(actor uint32, from, to time.Time) iter.Seq2[time.Time, string] {
	return func(yield func(time.Time, string) bool) {
		if atomic.LoadInt32(&l.closed) != 0 {
			return
		}

		// Query both memory and history.
		// Query history first, then memory, to ensure chronological order (past to now).
		l.queryHistory(context.Background(), actor, from, to, yield)
		l.queryMemory(actor, from, to, yield)
	}
}

// Close gracefully shuts down the logger, flushing any remaining data.
func (l *Service) Close() error {
	if !atomic.CompareAndSwapInt32(&l.closed, 0, 1) {
		return fmt.Errorf("logger already closed")
	}

	// Signal the run loop to exit and wait for it to finish
	close(l.commands)
	l.wg.Wait()

	// Close the codec
	l.codec.Close()
	l.cancel()
	return nil
}

// run is the main loop for the logger, handling all state modifications.
func (l *Service) run(ctx context.Context) {
	defer l.wg.Done()

	buf := buffer.New(l.config.BufferSize, l.codec)
	ticker := time.NewTicker(l.config.ChunkInterval)
	defer ticker.Stop()

	// Initialize sequence generator for the current day
	seqGen := seq.NewSequence(time.Now().UTC())

	for {
		select {
		case cmd, ok := <-l.commands:
			if !ok {
				l.flushBuffer(ctx, buf) // Final flush on close
				return
			}

			if c := cmd.log; c != nil {
				now := time.Now().UTC()

				// Check if we need to flush for a new day
				if seq.DayOf(now) != seqGen.Day() {
					l.flushBuffer(ctx, buf)
				}

				sequenceID := seqGen.Next(now)
				binary.LittleEndian.PutUint32((*c)[:4], sequenceID)
				entry := codec.LogEntry(*c)
				if !buf.Add(entry) {
					l.flushBuffer(ctx, buf)
					buf.Add(entry) // Must succeed
				}
				continue
			}

			if c := cmd.query; c != nil {
				results := buf.Query(c.actor, seqGen.Day(), c.from, c.to)
				c.ret <- results
				continue
			}

			if c := cmd.flush; c != nil {
				l.flushBuffer(ctx, buf)
				if c.done != nil {
					close(c.done)
				}
				continue
			}

		case <-ticker.C:
			l.flushBuffer(ctx, buf)
		case <-ctx.Done():
			return
		}
	}
}

// flushBuffer flushes the buffer and waits (for testing).
func (l *Service) flush() {
	done := make(chan struct{})
	l.commands <- command{flush: &flushCmd{done: done}}
	<-done
}

// keyOfMetadata builds the S3 object key for the metadata file for the provided date (YYYY-MM-DD).
func keyOfMetadata(date string) string {
	return fmt.Sprintf("%s/metadata.json", date)
}

// keyOfChunk builds an S3 key for a specific chunk file inside the date folder.
func keyOfChunk(date string, chunk uint64) string {
	return fmt.Sprintf("%s/%d.log", date, chunk)
}
