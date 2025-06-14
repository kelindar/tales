# Tales

`tales` is a lightweight event logging library that keeps recent data in memory while persisting compressed chunks to S3. Each log entry is associated with one or more actors, allowing fast per-actor queries over both memory and historical files.

## Features

- **S3 storage** with optional key prefix
- **In-memory buffer** flushed on a configurable interval
- **Actor indexes** using roaring bitmaps for efficient queries
- **ZSTD compression** for log data and bitmaps
- **Simple API** using Go iterators

## Quick Start

```go
logger, err := tales.New(
    "my-bucket",
    "us-east-1",
    tales.WithPrefix("events"),
    tales.WithChunkInterval(5*time.Minute),
    tales.WithBufferSize(1000),
)
if err != nil {
    log.Fatal(err)
}
defer logger.Close()

logger.Log("Player joined", 12345)
logger.Log("Player attacked", 12345, 67890)

from := time.Now().Add(-time.Hour)
to := time.Now()
for ts, msg := range logger.Query(12345, from, to) {
    fmt.Printf("%s: %s\n", ts.Format(time.RFC3339), msg)
}
```

## Options

```go
func WithChunkInterval(d time.Duration) Option
func WithBufferSize(size int) Option
func WithPrefix(prefix string) Option
func WithConcurrency(n int) Option
func WithRetries(n int) Option
func WithClient(fn func(context.Context, s3.Config) (s3.Client, error)) Option
```

## Installation

```bash
go get github.com/kelindar/tales
```

Tales stores data under `YYYY-MM-DD/` prefixes on S3 using four objects per day:

```
tales.log     # log data chunks
tales.idx     # JSON metadata
actors.log    # compressed bitmaps
actors.idx    # index entries
```

Each log entry uses a 32‑bit sequence ID `(minute << 20) | counter` to reconstruct timestamps and keep results ordered.
