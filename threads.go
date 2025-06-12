package threads

import (
	"context"
	"fmt"
	"iter"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kelindar/threads/internal/buffer"
	"github.com/kelindar/threads/internal/codec"
	"github.com/kelindar/threads/internal/s3"
)

// logCmd represents a command to log an entry.
type logCmd struct {
	text   string
	actors []uint32
}

// queryCmd represents a command to query the buffer.
type queryCmd struct {
	actor uint32
	from  time.Time
	to    time.Time
	yield func(time.Time, string) bool
	ret   chan<- []codec.LogEntry
}

// flushCmd represents a command to flush the buffer.
type flushCmd struct {
	done chan struct{}
}

// Logger provides high-performance, memory-efficient logging and querying of game events.
type Logger struct {
	config   Config
	s3Client s3.Client
	codec    *codec.Codec
	commands chan interface{}
	wg       sync.WaitGroup
	cancel   context.CancelFunc
	closed   int32 // atomic flag
}

// New creates a new Logger instance with the given configuration.
func New(config Config) (*Logger, error) {
	// Set defaults and validate configuration
	config.setDefaults()
	if err := config.validate(); err != nil {
		return nil, err
	}

	// Create codec for compression/decompression
	codecInstance, err := codec.NewCodec()
	if err != nil {
		return nil, fmt.Errorf("failed to create codec: %w", err)
	}

	// Create S3 client
	var s3Client s3.Client
	if config.NewS3Client != nil {
		s3Client, err = config.NewS3Client(context.Background(), config.S3Config)
	} else {
		s3Client, err = s3.NewClient(context.Background(), config.S3Config)
	}
	if err != nil {
		codecInstance.Close()
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	logger := &Logger{
		config:   config,
		s3Client: s3Client,
		codec:    codecInstance,
		commands: make(chan interface{}, config.BufferSize),
		cancel:   cancel,
	}

	// Start the background goroutine
	logger.wg.Add(1)
	go logger.run(ctx)

	return logger, nil
}

// Log adds a log entry with the given text and actors.
func (l *Logger) Log(text string, actors []uint32) error {
	if atomic.LoadInt32(&l.closed) != 0 {
		return ErrClosed("cannot log to closed logger")
	}

	if len(actors) == 0 {
		return ErrInvalidConfig("at least one actor is required")
	}

	l.commands <- logCmd{text: text, actors: actors}
	return nil
}

// Query returns an iterator over log entries for the specified actor and time range.
func (l *Logger) Query(actor uint32, from, to time.Time) iter.Seq2[time.Time, string] {
	return func(yield func(time.Time, string) bool) {
		if atomic.LoadInt32(&l.closed) != 0 {
			return
		}

		// Query memory buffer first (most recent data)
		l.queryMemoryBuffer(actor, from, to, yield)

		// Query S3 for historical data
		l.queryS3Historical(context.Background(), actor, from, to, yield)
	}
}

// Close gracefully shuts down the logger, flushing any remaining data.
func (l *Logger) Close() error {
	if !atomic.CompareAndSwapInt32(&l.closed, 0, 1) {
		return ErrClosed("logger already closed")
	}

	// Signal the run loop to exit and wait for it to finish
	close(l.commands)
	l.wg.Wait()

	// Close the codec
	l.codec.Close()

	return nil
}

// run is the main loop for the logger, handling all state modifications.
func (l *Logger) run(ctx context.Context) {
	defer l.wg.Done()

	buf := buffer.New(l.config.BufferSize, l.codec)
	ticker := time.NewTicker(l.config.ChunkInterval)
	defer ticker.Stop()

	// Initialize sequence generator for the current day
	seqGen := NewSequenceGenerator(time.Now().UTC())

	for {
		select {
		case cmd, ok := <-l.commands:
			if !ok {
				l.flushBuffer(buf) // Final flush on close
				return
			}

			switch c := cmd.(type) {
			case logCmd:
				now := time.Now().UTC()

				// Check if we need to flush for a new day
				if getDayStart(now) != seqGen.DayStart() {
					l.flushBuffer(buf)
				}

				sequenceID := seqGen.Generate(now)
				entry, _ := codec.NewLogEntry(sequenceID, c.text, c.actors)
				if !buf.Add(entry) {
					l.flushBuffer(buf)
					buf.Add(entry) // Must succeed
				}

			case queryCmd:
				results := buf.Query(c.actor, seqGen.DayStart(), c.from, c.to)
				c.ret <- results

			case flushCmd:
				l.flushBuffer(buf)
				if c.done != nil {
					close(c.done)
				}
			}

		case <-ticker.C:
			l.flushBuffer(buf)
		}
	}
}

// queryMemoryBuffer queries the in-memory buffer for entries.
func (l *Logger) queryMemoryBuffer(actor uint32, from, to time.Time, yield func(time.Time, string) bool) {
	ret := make(chan []codec.LogEntry, 1)
	dayStart := getDayStart(from)
	l.commands <- queryCmd{actor: actor, from: from, to: to, ret: ret}

	for _, entry := range <-ret {
		if !yield(entry.Time(dayStart), entry.Text()) {
			return
		}
	}
}
