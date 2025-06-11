package threads

import (
	"context"
	"iter"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kelindar/threads/internal/s3"
)

// Logger provides high-performance, memory-efficient logging and querying of game events.
type Logger struct {
	config        Config
	s3Client      s3.Client
	buffer        *Buffer
	atomicCounter uint32
	dayStart      time.Time

	// Synchronization
	mu      sync.RWMutex
	flushMu sync.Mutex
	closed  int32 // atomic flag

	// Background operations
	ctx        context.Context
	cancel     context.CancelFunc
	flushTimer *time.Timer
	wg         sync.WaitGroup
}

// New creates a new Logger instance with the given configuration.
func New(config Config) (*Logger, error) {
	// Set defaults and validate configuration
	config.setDefaults()
	if err := config.validate(); err != nil {
		return nil, err
	}

	// Initialize compression
	if err := initCompression(); err != nil {
		return nil, err
	}

	// Create context for background operations
	ctx, cancel := context.WithCancel(context.Background())

	// Create S3 client
	s3Client, err := s3.NewClient(ctx, config.S3Config)
	if err != nil {
		cancel()
		return nil, err
	}

	// Create buffer
	buffer := NewBuffer(config.BufferSize)

	// Initialize day start (ensure UTC)
	dayStart := getDayStart(time.Now().UTC())

	logger := &Logger{
		config:   config,
		s3Client: s3Client,
		buffer:   buffer,
		dayStart: dayStart,
		ctx:      ctx,
		cancel:   cancel,
	}

	// Start background flush timer
	logger.startFlushTimer()

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

	now := time.Now()

	// Check if we need to handle day boundary
	l.mu.RLock()
	currentDayStart := l.dayStart
	l.mu.RUnlock()

	if isNewDay(currentDayStart, now.UTC()) {
		if err := l.handleDayBoundary(now.UTC()); err != nil {
			return err
		}
	}

	// Generate sequence ID (ensure both times are in UTC)
	sequenceID := generateSequenceID(l.dayStart, now.UTC(), &l.atomicCounter)

	// Create log entry
	entry := &LogEntry{
		SequenceID: sequenceID,
		Text:       text,
		Actors:     make([]uint32, len(actors)),
	}
	copy(entry.Actors, actors)

	// Add to buffer
	if !l.buffer.Add(entry) {
		// Buffer is full, trigger immediate flush
		if err := l.flushBuffer(); err != nil {
			return err
		}
		// Try adding again after flush
		if !l.buffer.Add(entry) {
			return ErrInvalidConfig("entry too large for buffer")
		}
	}

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
		l.queryS3Historical(actor, from, to, yield)
	}
}

// Close gracefully shuts down the logger, flushing any remaining data.
func (l *Logger) Close() error {
	if !atomic.CompareAndSwapInt32(&l.closed, 0, 1) {
		return ErrClosed("logger already closed")
	}

	// Cancel background operations
	l.cancel()

	// Stop flush timer
	if l.flushTimer != nil {
		l.flushTimer.Stop()
	}

	// Wait for background operations to complete
	l.wg.Wait()

	// Flush remaining buffer data
	if err := l.flushBuffer(); err != nil {
		return err
	}

	// Close compression resources
	closeCompression()

	return nil
}

// startFlushTimer starts the background timer for periodic buffer flushing.
func (l *Logger) startFlushTimer() {
	l.flushTimer = time.NewTimer(l.config.ChunkInterval)

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()

		for {
			select {
			case <-l.ctx.Done():
				return
			case <-l.flushTimer.C:
				if atomic.LoadInt32(&l.closed) == 0 {
					l.flushBuffer()
					l.flushTimer.Reset(l.config.ChunkInterval)
				}
			}
		}
	}()
}

// handleDayBoundary handles the transition to a new day.
func (l *Logger) handleDayBoundary(now time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Flush current buffer before changing day
	if err := l.flushBuffer(); err != nil {
		return err
	}

	// Update day start and reset atomic counter
	l.dayStart = getDayStart(now)
	atomic.StoreUint32(&l.atomicCounter, 0)

	return nil
}

// queryMemoryBuffer queries the in-memory buffer for entries.
func (l *Logger) queryMemoryBuffer(actor uint32, from, to time.Time, yield func(time.Time, string) bool) {
	l.mu.RLock()
	dayStart := l.dayStart
	l.mu.RUnlock()

	entries := l.buffer.GetActorEntries(actor, dayStart, from, to)

	for _, entry := range entries {
		timestamp := reconstructTimestamp(entry.SequenceID, dayStart)
		// Convert all times to UTC for comparison
		timestampUTC := timestamp.UTC()
		fromUTC := from.UTC()
		toUTC := to.UTC()

		// Use >= and <= for inclusive range check
		if (timestampUTC.Equal(fromUTC) || timestampUTC.After(fromUTC)) && (timestampUTC.Equal(toUTC) || timestampUTC.Before(toUTC)) {
			if !yield(timestamp, entry.Text) {
				return
			}
		}
	}
}
