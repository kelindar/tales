<p align="center">
  <img src="https://img.shields.io/github/go-mod/go-version/kelindar/tales" alt="Go Version">
  <a href="https://pkg.go.dev/github.com/kelindar/tales"><img src="https://pkg.go.dev/badge/github.com/kelindar/tales" alt="PkgGoDev"></a>
  <a href="https://goreportcard.com/report/github.com/kelindar/tales"><img src="https://goreportcard.com/badge/github.com/kelindar/tales" alt="Go Report Card"></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License"></a>
</p>

## Tales: High-Performance Event Logging

Tales provides a **high-performance, S3-backed log store** with actor-centric queries. It keeps recent events in memory and persists them to S3 in compressed chunks.

- **Fast Writes:** Non-blocking logging with sequential encoding.
- **Actor Queries:** Roaring bitmaps for quick per-actor lookups.
- **S3 Only:** Zero local storage with compressed daily files.
- **Thread-Safe:** Safe for concurrent logging from multiple goroutines.

**Use When:**
- ✅ Storing chat or gameplay events for many users.
- ✅ Needing history queries scoped to a single actor.
- ✅ Wanting predictable memory usage and S3 as the only storage.

**Not For:**
- ❌ Archiving long-term logs without S3 access.
- ❌ Applications that require random updates to existing entries.

## Quick Start
```go
logger, err := tales.New("my-bucket", "us-east-1",
    tales.WithPrefix("events"),
    tales.WithChunkInterval(5*time.Minute),
)
if err != nil {
    log.Fatal(err)
}
defer logger.Close()

// Log a few events
logger.Log("Player joined", 12345)
logger.Log("Player moved", 12345)

// Query them back
from := time.Now().Add(-10 * time.Minute)
to := time.Now()
for _, text := range logger.Query(12345, from, to) {
    fmt.Println(text)
}
```

It prints:

```
Player joined
Player moved
```

## Installation
```bash
go get github.com/kelindar/tales
```

## License
Tales is released under the [MIT License](https://opensource.org/licenses/MIT).
