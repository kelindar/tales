<p align="center">
<img width="300" height="100" src=".github/logo.png" border="0" alt="kelindar/tales">
<br>
<img src="https://img.shields.io/github/go-mod/go-version/kelindar/tales" alt="Go Version">
<a href="https://pkg.go.dev/github.com/kelindar/tales"><img src="https://pkg.go.dev/badge/github.com/kelindar/tales" alt="PkgGoDev"></a>
<a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License"></a>
</p>

## Tales: Multi-Actor Event Logging in S3

This library provides an **append-only event log for Go**, backed by S3. It buffers events in memory, stores compressed chunks, and uses bitmap indexes to find events by actor ID. An actor is a `uint32` representing a user, account, device, conversation, or game object.

## Features

- **Actor Queries:** Find events for one actor or events shared by several actors.
- **Multiple Writers:** Application instances write independently under the same bucket and prefix.
- **Compressed Storage:** Zstd compression and bitmap indexes keep event history compact.
- **Selective Reads:** Fetch matching payload blocks with S3 range requests.
- **Explicit Durability:** `Sync` confirms that accepted events are committed to S3.
- **Historical Compaction:** Merge indexes and consolidate payloads for older days.

**Use When:**

- ✅ Recording actor history, conversations, audit trails, or game events.
- ✅ Keeping an append-only event history in S3.
- ✅ Writing from several application instances under the same prefix.

**Not For:**

- ❌ Updating or deleting individual events.
- ❌ Full-text search, joins, arbitrary JSON filters, or transactions.
- ❌ Workloads that require every `Log` call to write immediately to S3.

## Documentation

- [Quick Start](#quick-start)
- [Actor Queries](#actor-queries)
- [Pagination](#pagination)
- [Multiple Writers](#multiple-writers)
- [Historical Compaction](#historical-compaction)
- [Options](#options)
- [Storage Layout](#storage-layout)
- [Benchmarks](#benchmarks)

## Quick Start

Install the library with:

```sh
go get github.com/kelindar/tales
```

Create a logger, append events, and query an actor's history:

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/kelindar/tales"
)

func main() {
	ctx := context.Background()
	logger, err := tales.New("my-bucket", "us-east-1",
		tales.WithPrefix("events"),
	)
	if err != nil {
		log.Fatal(err)
	}

	if err := logger.Log("Player joined", 12345); err != nil {
		log.Fatal(err)
	}
	if err := logger.Log("Player attacked monster", 12345, 67890); err != nil {
		log.Fatal(err)
	}
	if err := logger.Sync(ctx); err != nil {
		log.Fatal(err)
	}

	from, to := time.Now().Add(-time.Hour), time.Now()
	for event, err := range logger.Scan(ctx, from, to, 12345) {
		if err != nil {
			log.Fatal(err)
		}
		log.Println(event.Text())
	}

	if err := logger.Close(); err != nil {
		log.Fatal(err)
	}
}
```

`Log` accepts an event into memory. Call `Sync` at the point where it must be durable. `Close` flushes pending events and releases resources. If flushing fails, the service remains open so you can retry `Sync` or `Close`.

The default client uses ambient AWS credentials through [kelindar/s3](https://github.com/kelindar/s3). Use `WithKey` to supply a signing key, or `WithBackblaze` to use Backblaze B2.

## Actor Queries

Pass one actor to read its history, or several actors to find events containing **all** of them. For example, this query finds interactions between a player and a monster:

```go
for event, err := range logger.Scan(ctx, from, to, 12345, 67890) {
	if err != nil {
		log.Fatal(err)
	}
	log.Println(event.Time(), event.Text())
}
```

Both time bounds are inclusive. Results are oldest first when `from <= to`; reverse the bounds for newest first. Queries include the service's buffered events as well as committed data in S3.

Events are read-only views over log data. Call `event.Clone()` when you need an independent copy to retain or modify.

## Pagination

Use `Page` to read a bounded number of events. Start with `tales.Zero` and pass the returned cursor to the next call:

```go
cursor := tales.Zero
for {
	events, next, err := logger.Page(ctx, to, from, cursor, 50, 12345)
	if err != nil {
		log.Fatal(err)
	}
	for _, event := range events { // Newest first because to > from.
		log.Println(event.Text())
	}
	if next == tales.Zero {
		break
	}
	cursor = next
}
```

Page sizes range from 1 to 1,000. The cursor is exclusive and an empty next cursor ends iteration. `Cursor` is an opaque URL-safe string: use `cursor.String()` to store it and `tales.Cursor(value)` to restore it. Reuse it with the same time bounds and actors; malformed cursors return an error.

## Multiple Writers

Each service owns one writer ID and writes its own immutable chunks. Services sharing a bucket and prefix can query each other's committed events.

Tales generates a writer ID by default. Use `WithWriterID("game-server-1")` to derive a stable ID from a deployment name. Give each live process a distinct writer ID.

## Historical Compaction

Call `Compact` from a scheduled job to merge indexes and consolidate payloads for older days. Run one compactor per prefix. The earliest eligible day is two UTC days ago:

```go
day := time.Now().UTC().AddDate(0, 0, -2)
if err := logger.Compact(ctx, day); err != nil {
	log.Fatal(err)
}
```

Compaction is optional and safe to retry. Queries use writer files until compacted metadata is committed.

## Options

Pass options to `tales.New` to configure the logger:

| Option | Description | Default |
| --- | --- | --- |
| `WithPrefix` | S3 key prefix for this event log. | Bucket root |
| `WithWriterID` | Stable name hashed into a writer ID. | Generated ID |
| `WithInterval` | Flush interval, at least one minute. | 5 minutes |
| `WithBuffer` | Maximum entries in the in-memory buffer. | 1,048,576 |
| `WithKey` | Signing key for S3 requests. | Ambient credentials |
| `WithBackblaze` | Use Backblaze B2. | AWS S3 |

## Storage Layout

Objects are grouped by UTC day below the configured prefix:

```text
YYYY-MM-DD/writers/<writer>/manifest.json
YYYY-MM-DD/writers/<writer>/<sequence>.log
YYYY-MM-DD/compact/index.bin
YYYY-MM-DD/compact/data.log
YYYY-MM-DD/compact/metadata.json
```

Chunk sequences start at zero and use 20 decimal digits. Each chunk contains actor bitmaps followed by independent zstd blocks of at most 256 KiB of raw event data. Version 1 JSON metadata records block offsets, event counts, and optional timestamp bounds.

Queries intersect actor bitmaps and coalesce adjacent matching blocks into range requests. Pagination skips blocks outside the time window, before the cursor, or unable to contribute to the page. Compaction preserves the compressed blocks and their bounds.

Readers also accept original single-payload chunks and block directories without timestamp bounds. Unknown versions and incomplete directories return an error.

## Benchmarks

Benchmarks use an in-process mock S3 server. Run the append, scan, pagination, and compaction benchmarks with:

```sh
cd cmd/bench
go run .
```

Sync benchmarks keep history and batch sizes fixed so each iteration measures the same workload:

```sh
go test -run '^$' -bench '^BenchmarkSync$' -benchmem -benchtime=100x
```

From the repository root, benchmark page reads across dense and sparse actor queries, both directions, and first or deep cursors:

```sh
go test -run '^$' -bench '^BenchmarkPageReads$' -benchmem
```

Example results for Page 50 over 10,000 event frames of 600 bytes from two writers. These are three-run medians using the mock server on Windows amd64, Intel Core i7-13700K:

| Ascending query | Time/op | B/op | Allocs/op | Read bytes/op |
| --- | --- | --- | --- | --- |
| Dense, first page | 0.73 ms | 861,904 | 82 | 317,394 |
| Dense, deep cursor | 1.14 ms | 1,255,216 | 83 | 711,242 |
| Sparse actor intersection, first page | 2.98 ms | 2,866,616 | 136 | 1,272,383 |

## Contributing

Contributions are welcome. Open an issue or pull request.

## License

Tales is licensed under the [MIT License](LICENSE).
