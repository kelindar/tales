<p align="center">
  <img width="300" height="100" src=".github/logo.png" border="0" alt="kelindar/tales">
  <br>
  <img src="https://img.shields.io/github/go-mod/go-version/kelindar/tales" alt="Go Version">
  <a href="https://pkg.go.dev/github.com/kelindar/tales"><img src="https://pkg.go.dev/badge/github.com/kelindar/tales" alt="PkgGoDev"></a>
  <a href="https://goreportcard.com/report/github.com/kelindar/tales"><img src="https://goreportcard.com/badge/github.com/kelindar/tales" alt="Go Report Card"></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License"></a>
</p>

## Event logs in S3

Tales is a Go package for recording events in an S3 bucket and reading them back by actor. An actor is a numeric ID for something involved in an event, such as a user, account, device, conversation, or game object. One event can belong to several actors.

Events are append-only: Tales writes new events but does not update existing ones. It buffers and compresses events before saving them to S3. Multiple application instances can write under the same S3 prefix because each instance writes to its own daily files.

The main operations are:

- `Log` accepts an event into the current process's memory.
- `Sync` waits until events accepted before it have been saved to S3.
- `Query` reads events within a time range that contain all requested actors.
- `Compact` prepares older days so queries need fewer S3 requests.

### Use when

- Events naturally belong to one or more numeric actors.
- You want S3 to be the persistent store and do not need a database alongside it.
- Your application mostly appends events and reads actor history.
- Several application instances need to write to the same bucket and prefix.

### Not for

- Updating or deleting individual events after they are written.
- Full-text search, arbitrary JSON filtering, joins, or database transactions.
- Sharing one writer ID between two live processes.
- Treating a successful `Log` call as proof that S3 already has the event; call `Sync` when that guarantee is needed.

Each running service owns one writer ID. Tales creates a process-specific ID by default. Use `WithWriterID` to derive a stable ID from a deployment name, and ensure only one live process uses that name at a time.

## Quick start

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
		tales.WithWriterID("game-server-1"),
	)
	if err != nil {
		log.Fatal(err)
	}

	if err := logger.Log(`{"kind":"joined"}`, 12345); err != nil {
		log.Fatal(err)
	}
	if err := logger.Log(`{"kind":"attacked"}`, 12345, 67890); err != nil {
		log.Fatal(err)
	}
	if err := logger.Sync(ctx); err != nil {
		log.Fatal(err)
	}

	from := time.Now().Add(-time.Hour)
	to := time.Now()
	for event, err := range logger.Query(ctx, from, to, 12345, 67890) {
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("%s %s", event.Time(), event.Text())
	}

	if err := logger.Close(); err != nil {
		log.Fatal(err)
	}
}
```

The query above asks for events containing both actor `12345` and actor `67890`. Actor arguments are combined with AND, so an event must contain every requested actor. The start and end times are included in the query range.

When events have the same timestamp, Tales orders them by writer ID, chunk sequence, and their position within the chunk. This makes repeated queries return events in the same order.

An `Event` provides its timestamp, text, JSON bytes, actors, and payload bytes. These values normally refer directly to the downloaded data to avoid a copy. Treat byte and JSON views as read-only. If an event must be kept or modified independently, call `Event.Clone`.

## Historical compaction

Compaction is optional maintenance for historical data. It combines the actor indexes for one UTC day and, where supported, asks S3 to combine large payload ranges without downloading them through the application.

Call `Compact` from an external scheduled job, with only one compactor running for a prefix. Today, yesterday, and future UTC days are rejected; the earliest eligible day is two days ago.

```go
day := time.Now().UTC().AddDate(0, 0, -2)
if err := logger.Compact(context.Background(), day); err != nil {
	log.Fatal(err)
}
```

Compaction is safe to retry. Its metadata is written last, so queries continue using the original writer files if an earlier step fails. Copied source files remain for 24 hours and are removed by a later run. Smaller source files that are still referenced are kept.

## Storage layout

Tales creates these keys below the configured prefix. Most users do not need to read these files directly.

```text
YYYY-MM-DD/writers/<writer>/manifest.json
YYYY-MM-DD/writers/<writer>/<sequence>.log
YYYY-MM-DD/compact/index.bin
YYYY-MM-DD/compact/data.log
YYYY-MM-DD/compact/metadata.json
```

Chunk sequences are zero-based and formatted as 20 decimal digits in keys and JSON metadata.

This release uses a new format and API with no legacy decoder or migration layer.

## Installation

```bash
go get github.com/kelindar/tales
```

## License

Tales is released under the [MIT License](https://opensource.org/licenses/MIT).
