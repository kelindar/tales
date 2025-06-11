# Threads Library Design Document

## Overview

The threads library provides high-performance, memory-efficient logging and querying of game events with actor-centric access patterns. It's designed for logging speech and other game events with the ability to query historical data for specific actors.

## Core API

```go
type Logger struct {
    // private implementation
}

func New(config Config) (*Logger, error)
func (l *Logger) Log(text string, actors []uint32) error
func (l *Logger) Query(actor uint32, from, to time.Time) iter.Seq2[time.Time, string]
func (l *Logger) Close() error
```

## Architecture

### Storage Design

**S3-Only Storage with In-Memory Buffering**:
- **S3 Daily Log**: Append-only file with tail-based chunk metadata
- **S3 Consolidated Bitmaps**: All actor bitmaps in single file, appended every 5 minutes
- **S3 Index File**: Actor lookup table with timestamp+actor+offset triplets
- **In-Memory Buffer**: Current 5-minute chunk buffered in memory only
- **No Local Storage**: Everything operates directly from S3 + current memory buffer

### S3 File Layout

```
bucket/prefix/YYYY-MM-DD/threads.log   # Daily log: compressed 5-min chunks + tail metadata
bucket/prefix/YYYY-MM-DD/threads.rbm   # Consolidated actor bitmaps (all actors)
bucket/prefix/YYYY-MM-DD/threads.idx   # Index: timestamp+actor+bitmap_offset+size entries
```

## File Formats

### Daily Log File (threads.log)
**Structure**: Append-only with tail-based metadata
```
[Chunk 0: ZSTD compressed log entries]
[Chunk 1: ZSTD compressed log entries]
...
[Chunk N: ZSTD compressed log entries]
[Tail Metadata:]
  [4 bytes: magic "TAIL"]
  [4 bytes: version]
  [8 bytes: day start timestamp (unix nano)]
  [4 bytes: chunk count]
  [chunk_count * 16 bytes: chunk entries]
    - [8 bytes: chunk start offset]
    - [4 bytes: compressed size]
    - [4 bytes: uncompressed size]
  [4 bytes: tail size]
```

**Log Entry Format** (within decompressed chunk):
```
[4 bytes: sequence ID (uint32)]
[varint: text length]
[varint: actor count]
[N bytes: text (UTF-8)]
[M*4 bytes: actor IDs (uint32)]
```

**Sequence ID Format**: `(minutes_from_day_start << 20) | atomic_counter`
- High 12 bits: minutes from day start (0-1439, fits in 12 bits)
- Low 20 bits: atomic counter within that minute (0-1048575)
- Guarantees lexicographic ordering and uniqueness

### Consolidated Bitmap File (threads.rbm)
**Structure**: Sequential compressed bitmaps
```
[Bitmap 1: ZSTD compressed roaring32 bitmap]
[Bitmap 2: ZSTD compressed roaring32 bitmap]
...
[Bitmap N: ZSTD compressed roaring32 bitmap]
```

### Index File (threads.idx)
**Structure**: Fixed-size entries for bitmap lookup
```
[Entry 1: 4 bytes timestamp + 4 bytes actor_id + 8 bytes offset + 4 bytes size]
[Entry 2: 4 bytes timestamp + 4 bytes actor_id + 8 bytes offset + 4 bytes size]
...
[Entry N: 4 bytes timestamp + 4 bytes actor_id + 8 bytes offset + 4 bytes size]
```

**Bitmap Contents**: Each bitmap contains uint32 sequence IDs for that actor during a 5-minute period. The sequence IDs maintain chronological order and provide second-level precision.

## Sequence ID Design

### Format Benefits
- **Roaring32 Efficiency**: Smaller memory footprint and faster operations than roaring64
- **Lexicographic Ordering**: Natural chronological sorting without additional timestamp comparisons
- **Uniqueness Guarantee**: Atomic counter prevents duplicate sequence IDs within same second
- **Second Precision**: Adequate granularity for game event logging
- **Compact Storage**: 4 bytes vs 8 bytes for nanosecond timestamps

### Implementation
```go
// Generate sequence ID
func generateSequenceID(dayStart time.Time, now time.Time, atomicCounter *uint32) uint32 {
    secondsFromDayStart := uint32(now.Sub(dayStart).Seconds())
    counter := atomic.AddUint32(atomicCounter, 1) & 0xFFFF // Wrap at 65536
    return (secondsFromDayStart << 16) | counter
}

// Reconstruct timestamp from sequence ID
func reconstructTimestamp(sequenceID uint32, dayStart time.Time) time.Time {
    seconds := sequenceID >> 16
    return dayStart.Add(time.Duration(seconds) * time.Second)
}
```

## Operations

### Write Operations
1. **Log Entry Buffering**:
   - Buffer log entries in memory for current 5-minute chunk
   - Generate sequence IDs using atomic counter within current second
   - Update actor roaring32 bitmaps in memory with sequence IDs
2. **Chunk Flush** (every 5 minutes):
   - Read current tail metadata from S3
   - Compress buffered log entries and append to threads.log
   - Compress updated actor bitmaps and append to threads.rbm
   - Append index entries to threads.idx
   - Write updated tail metadata

### Query Operations
1. **Memory Buffer Check**:
   - Check in-memory buffer for recent entries (last 5 minutes)
   - Filter by actor and time range
2. **S3 Historical Query**:
   - Download full index file
   - Filter index entries by actor and time range
   - Use S3 byte-range to fetch relevant bitmap chunks
   - Merge bitmap chunks for the actor
   - Use tail metadata to locate log chunks
   - Use S3 byte-range to fetch needed log chunks
   - Decompress and filter log entries
3. **Result Merging**:
   - Combine memory buffer and S3 results
   - Yield entries in chronological order

## Key Benefits

### Storage Efficiency
- **ZSTD Compression**: 60-80% compression ratio for text data
- **Consolidated Bitmaps**: All actor indices in single S3 object
- **Roaring32 Bitmaps**: Smaller memory footprint than roaring64
- **Second Precision**: Adequate for game event logging
- **Sequence Ordering**: Lexicographic ordering guarantees chronological results
- **No Local Storage**: Zero local disk usage

### Query Performance
- **Memory-First**: Recent data served from memory buffer
- **Targeted S3 Access**: Only download needed bitmap and log chunks
- **Batch Operations**: Combine adjacent S3 byte-range requests
- **On-Demand Processing**: No caching overhead

### Operational Simplicity
- **S3-Only**: No local file management or rotation
- **Tail-Based Append**: Simple append operations with metadata
- **Consolidated Files**: Only 3 S3 objects per day
- **Predictable Memory**: Fixed 5-minute buffer size

## Configuration

```go
type Config struct {
    S3Config       S3Config      // S3 configuration (required)
    ChunkInterval  time.Duration // Chunk completion interval (default: 5 minutes)
    BufferSize     int           // Memory buffer size (default: 1000 entries)
}

type S3Config struct {
    Bucket          string
    Region          string
    Prefix          string
    MaxConcurrent   int    // Max concurrent S3 requests (default: 10)
    RetryAttempts   int    // Retry attempts for failed requests (default: 3)
    // AWS credentials via environment or IAM roles
}
```

## S3 Access Patterns

### Byte-Range Operations
- **Log Tail**: `Range: bytes=-1024` to read tail metadata from threads.log
- **Index File**: Download full threads.idx file (typically 10-100KB)
- **Bitmap Chunks**: `Range: bytes=offset-offset+size-1` for specific actor bitmaps from threads.rbm
- **Log Chunks**: `Range: bytes=chunkOffset-chunkOffset+compressedSize-1` for 5-minute chunks from threads.log
- **Batch Operations**: Combine adjacent reads when possible

## Query Implementation Example

```go
// Pseudocode for querying actor 12345 from 14:30-16:15 on 2024-01-15
func (l *Logger) Query(actor uint32, from, to time.Time) iter.Seq2[time.Time, string] {
    return func(yield func(time.Time, string) bool) {
        // 1. Check memory buffer first
        for _, entry := range l.currentChunk {
            if entry.actorID == actor && inTimeRange(entry, from, to) {
                if !yield(entry.timestamp, entry.text) {
                    return
                }
            }
        }

        // 2. Query S3 for historical data
        indexEntries := downloadIndexFile(date) // Download threads.idx
        relevantBitmaps := filterByActorAndTime(indexEntries, actor, from, to)

        // 3. Fetch and merge bitmap chunks from threads.rbm
        allSequenceIDs := mergeBitmapChunks(relevantBitmaps)

        // 4. Group by log chunks and fetch from threads.log
        chunkGroups := groupByLogChunks(allSequenceIDs)
        for chunkIndex, sequenceIDs := range chunkGroups {
            entries := fetchAndDecompressLogChunk(chunkIndex)
            for _, entry := range entries {
                if contains(sequenceIDs, entry.sequenceID) {
                    timestamp := reconstructTimestamp(entry.sequenceID, dayStart)
                    if !yield(timestamp, entry.text) {
                        return
                    }
                }
            }
        }
    }
}
```

## Implementation Details

### Required Dependencies
```go
import (
    "github.com/RoaringBitmap/roaring/v2"  // Actor sequence ID indexing (roaring32)
    "github.com/klauspost/compress/zstd"   // Log chunk compression
    "github.com/stretchr/testify/assert"   // Unit testing
    "github.com/aws/aws-sdk-go-v2"         // S3 integration
)
```

## Usage Examples

### Basic Setup and Logging
```go
logger, err := threads.New(threads.Config{
    S3Config: threads.S3Config{
        Bucket: "game-logs-bucket",
        Region: "us-west-2",
        Prefix: "production/threads/",
    },
    ChunkInterval: 5 * time.Minute,
})
if err != nil {
    return err
}
defer logger.Close()

// Log speech from player 12345 to nearby players
// Entry goes into in-memory buffer, flushed to S3 every 5 minutes
err = logger.Log("Hello, world!", []uint32{12345, 67890, 11111})
```

### Querying Recent Data
```go
// Query recent data (likely served from memory buffer)
from := time.Now().Add(-2 * time.Minute)
to := time.Now()

for timestamp, text := range logger.Query(12345, from, to) {
    fmt.Printf("Recent: %s: %s\n", timestamp.Format(time.RFC3339), text)
}
```

### Querying Historical Data
```go
// Query historical data (served from S3)
from := time.Now().AddDate(0, 0, -7)  // 7 days ago
to := time.Now()

for timestamp, text := range logger.Query(12345, from, to) {
    fmt.Printf("Historical: %s: %s\n", timestamp.Format(time.RFC3339), text)
}
```

## Implementation Status

✅ **Completed Features:**
- Core API (New, Log, Query, Close)
- S3-based storage with in-memory buffering
- Sequence ID generation with minute-level precision
- ZSTD compression for all file formats
- Roaring bitmap indexing for efficient actor queries
- Thread-safe operations with proper synchronization
- Graceful shutdown and error handling
- Comprehensive integration tests with mock S3 server

✅ **File Formats Implemented:**
- `threads.log` - Compressed log entries with tail metadata
- `threads.rbm` - Compressed roaring bitmaps for actor indexing
- `threads.idx` - Binary index for bitmap lookups

✅ **Query Features:**
- Memory-first queries for recent data
- S3 historical queries with byte-range requests
- Iterator-based API using Go 1.23 iter.Seq2
- Efficient bitmap merging for multi-chunk queries

## Installation

```bash
go get github.com/kelindar/threads
```
