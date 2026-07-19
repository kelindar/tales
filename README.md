<p align="center">
  <img width="300" height="100" src=".github/logo.png" border="0" alt="kelindar/tales">
  <br>
  <img src="https://img.shields.io/github/go-mod/go-version/kelindar/tales" alt="Go Version">
  <a href="https://pkg.go.dev/github.com/kelindar/tales"><img src="https://pkg.go.dev/badge/github.com/kelindar/tales" alt="PkgGoDev"></a>
  <a href="https://goreportcard.com/report/github.com/kelindar/tales"><img src="https://goreportcard.com/badge/github.com/kelindar/tales" alt="Go Report Card"></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License"></a>
</p>

## Multi-Actor Event Logging in S3

Tales is an append-only event log for Go. It buffers events in memory, stores them as compressed files in S3, and finds them by actor ID. An actor can be a user, account, device, conversation, game object, or anything else represented by a `uint32`.

- **Actor Queries:** Find events for one actor or events shared by several actors.
- **Multiple Writers:** Each application instance writes its own files under the same prefix.
- **Explicit Durability:** `Sync` confirms that previously accepted events are stored in S3.
- **Historical Compaction:** `Compact` reduces the number of files needed to query older days.

### Use When

- Events belong to one or more numeric actors.
- Workloads mostly append events and read actor history.
- S3 should be the persistent store.
- Several application instances need to write under the same prefix.

### Not For

- Updating or deleting individual events.
- Full-text search, joins, arbitrary JSON filters, or transactions.
- Sharing one writer ID between live processes.
- Workflows where every `Log` call must immediately write to S3; use `Sync` at the required durability boundary.

## Quick Start

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

	from := time.Now().Add(-time.Hour)
	to := time.Now()
	for event, err := range logger.Query(ctx, from, to, 12345) {
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

`Log` returns after the event has been accepted into memory. Call `Sync` when it must be committed to S3. A failed `Close` leaves the service open so `Sync` or `Close` can be retried.

Actor arguments to `Query` are combined with AND. Both time bounds are inclusive, and results are returned in a stable order. Event views are read-only and may refer to downloaded data; call `Clone` before retaining or modifying one independently.

Each service owns one writer ID. Tales creates one automatically, or `WithWriterID("game-server-1")` can derive a stable ID from a deployment name. Only one live process may use a given writer ID.

## Historical compaction

Compaction is optional maintenance for older data. Call it from an external scheduled job, with one compactor running per prefix. The earliest eligible day is two UTC days ago.

```go
day := time.Now().UTC().AddDate(0, 0, -2)
if err := logger.Compact(context.Background(), day); err != nil {
	log.Fatal(err)
}
```

Compaction is safe to retry. Queries continue using writer files until compacted metadata is committed.

## Storage layout

Tales creates these keys below the configured prefix:

```text
YYYY-MM-DD/writers/<writer>/manifest.json
YYYY-MM-DD/writers/<writer>/<sequence>.log
YYYY-MM-DD/compact/index.bin
YYYY-MM-DD/compact/data.log
YYYY-MM-DD/compact/metadata.json
```

Chunk sequences are zero-based and use 20 decimal digits. This format has no legacy decoder or migration path.

## Installation

```bash
go get github.com/kelindar/tales
```

## License

Tales is released under the [MIT License](https://opensource.org/licenses/MIT).
