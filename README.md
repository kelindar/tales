<p align="center">
  <img width="300" height="110" src=".github/logo.png" border="0" alt="kelindar/tales">
  <br>
  <img src="https://img.shields.io/github/go-mod/go-version/kelindar/tales" alt="Go Version">
  <a href="https://pkg.go.dev/github.com/kelindar/tales"><img src="https://pkg.go.dev/badge/github.com/kelindar/tales" alt="PkgGoDev"></a>
  <a href="https://goreportcard.com/report/github.com/kelindar/tales"><img src="https://goreportcard.com/badge/github.com/kelindar/tales" alt="Go Report Card"></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License"></a>
</p>

## Multi-Actor Event Logging in S3

Tales provides a **multi-actor, S3-backed log store** with actor-centric queries. It keeps recent events in memory and persists them to S3 in compressed chunks.

- **Fast Writes:** Non-blocking logging with sequential encoding.
- **Actor Queries:** Efficient per-actor lookups using bitmaps.
- **S3 Only:** Zero local storage with compressed periodic files.
- **Thread-Safe:** Safe for concurrent logging from multiple goroutines.

**Use When:**
- ✅ Storing chat or gameplay events for many users.
- ✅ Needing history queries scoped to a single actor or intersection of actors.
- ✅ Wanting predictable memory usage and S3 as the only storage.

**Not For:**
- ❌ Archiving long-term logs with complex structure.
- ❌ Applications that require random updates to existing entries.


## Quick Start

```go
import (
    "log"
    "time"
    "github.com/kelindar/tales"
)

logger, err := tales.New("my-bucket", "us-east-1",
    tales.WithPrefix("events"),
    tales.WithChunkInterval(5*time.Minute),
)
if err != nil {
    log.Fatal(err)
}
defer logger.Close()

// Log a few events (returns error)
logger.Log("Player joined", 1)
logger.Log("Player moved", 1)

from := time.Now().Add(-10 * time.Minute)
to := time.Now()

// Query for a single actor (returns an iterator)
for _, text := range logger.Query(from, to, 1) {
    println(text)
}

// Query for entries that contain ALL specified actors (intersection)
for _, text := range logger.Query(from, to, 1, 2) {
    println("Event involving both actors:", text)
}
```

**Note:** `logger.Query` returns an iterator (not a slice). You can use it in a `for _, v := range ...` loop in Go 1.21+.

### Example Output
```
Player joined
Player moved
```

## More Complete Example
```go
// Log some game events
logger.Log("Player joined the game", 12345)
logger.Log("Player moved to position (100, 200)", 12345)
logger.Log("Player attacked monster", 12345, 67890) // Player and monster
logger.Log("Monster died", 67890)
logger.Log("Player gained 100 XP", 12345)

from := time.Now().Add(-1 * time.Hour)
to := time.Now().Add(1 * time.Hour)

// Query events for a specific player
for _, text := range logger.Query(from, to, 12345) {
    println(text)
}

// Query events involving both player and monster
for _, text := range logger.Query(from, to, 12345, 67890) {
    println(text)
}
```

### Output
```
Player joined the game
Player moved to position (100, 200)
Player attacked monster
Player gained 100 XP
Player attacked monster
```

## Installation
```bash
go get github.com/kelindar/tales
```

## License
Tales is released under the [MIT License](https://opensource.org/licenses/MIT).
