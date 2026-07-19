// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package codec

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
)

// Sequence is a writer-local chunk sequence encoded as 20 decimal digits.
type Sequence uint64

func (s Sequence) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%020d"`, uint64(s))), nil
}

func (s *Sequence) UnmarshalJSON(data []byte) error {
	if len(data) != 22 || data[0] != '"' || data[len(data)-1] != '"' {
		return fmt.Errorf("invalid fixed-width sequence")
	}
	value, err := strconv.ParseUint(string(data[1:21]), 10, 64)
	if err != nil {
		return fmt.Errorf("invalid fixed-width sequence: %w", err)
	}
	*s = Sequence(value)
	return nil
}

// Range locates immutable bytes within an object.
type Range struct {
	Offset int64 `json:"offset"`
	Size   int64 `json:"size"`
}

// ChunkEntry describes one immutable writer chunk.
type ChunkEntry struct {
	Sequence   Sequence         `json:"sequence"`
	Entries    uint32           `json:"entries"`
	Time       [2]uint32        `json:"time"`
	BitmapSize int64            `json:"bitmapSize"`
	Data       Range            `json:"data"`
	Size       int64            `json:"size"`
	ETag       string           `json:"etag"`
	Actors     map[uint32]Range `json:"actors,omitempty"`
}

func (c ChunkEntry) Between(from, to uint32) bool {
	return c.Time[0] <= to && c.Time[1] >= from
}

// Manifest is the append-only index owned by one writer for one UTC day.
type Manifest struct {
	Day    string       `json:"day"`
	Writer string       `json:"writer"`
	Chunks []ChunkEntry `json:"chunks"`
}

func (m *Manifest) NextSequence() uint64 {
	if len(m.Chunks) == 0 {
		return 0
	}
	return uint64(m.Chunks[len(m.Chunks)-1].Sequence) + 1
}

type ObjectRange struct {
	Key    string `json:"key"`
	ETag   string `json:"etag"`
	Offset int64  `json:"offset"`
	Size   int64  `json:"size"`
}

type CompactSource struct {
	Writer   string      `json:"writer"`
	Sequence Sequence    `json:"sequence"`
	Base     uint64      `json:"base"`
	Entries  uint32      `json:"entries"`
	Time     [2]uint32   `json:"time"`
	Payload  ObjectRange `json:"payload"`
	Source   string      `json:"source"`
	Copied   bool        `json:"copied"`
}

type CompactMetadata struct {
	Day         string           `json:"day"`
	PublishedAt int64            `json:"publishedAt"`
	Index       ObjectRange      `json:"index"`
	Actors      map[uint32]Range `json:"actors"`
	Sources     []CompactSource  `json:"sources"`
}

func Encode[T any](value T) ([]byte, error) { return json.Marshal(value) }

func Decode[T any](data []byte) (*T, error) {
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, err
	}
	return &value, nil
}

func ValidateManifest(m *Manifest, day, writer string) error {
	if m == nil || m.Day != day || m.Writer != writer || !validWriterID(writer) {
		return fmt.Errorf("manifest identity mismatch")
	}
	for i, chunk := range m.Chunks {
		if chunk.Sequence != Sequence(i) {
			return fmt.Errorf("invalid chunk sequence %d", chunk.Sequence)
		}
		if err := ValidateChunk(chunk); err != nil {
			return fmt.Errorf("chunk %d: %w", i, err)
		}
	}
	return nil
}

func ValidateChunk(chunk ChunkEntry) error {
	switch {
	case chunk.Entries == 0:
		return fmt.Errorf("empty chunk")
	case chunk.ETag == "" || chunk.Size <= 0:
		return fmt.Errorf("invalid object descriptor")
	case chunk.Time[0] > chunk.Time[1] || chunk.Time[1] > MaxMillis:
		return fmt.Errorf("invalid time bounds")
	case chunk.BitmapSize <= 0 || chunk.Data.Offset != chunk.BitmapSize || chunk.Data.Size <= 0:
		return fmt.Errorf("invalid payload range")
	case chunk.Data.Offset > math.MaxInt64-chunk.Data.Size || chunk.Data.Offset+chunk.Data.Size != chunk.Size:
		return fmt.Errorf("invalid object size")
	}
	if err := validateActorRanges(chunk.Actors, chunk.BitmapSize); err != nil {
		return err
	}
	return nil
}

func ValidateCompact(meta *CompactMetadata, day string) error {
	if meta == nil || meta.Day != day || meta.PublishedAt <= 0 || meta.Index.Key == "" || meta.Index.ETag == "" || meta.Index.Offset != 0 || meta.Index.Size <= 0 || len(meta.Sources) == 0 {
		return fmt.Errorf("invalid compact metadata")
	}
	var next uint64
	for i, source := range meta.Sources {
		if source.Base != next || !validWriterID(source.Writer) || source.Entries == 0 || source.Time[0] > source.Time[1] || source.Time[1] > MaxMillis || source.Payload.Key == "" || source.Payload.ETag == "" || source.Payload.Offset < 0 || source.Payload.Size <= 0 || source.Payload.Offset > math.MaxInt64-source.Payload.Size || source.Source == "" {
			return fmt.Errorf("invalid compact source %d", i)
		}
		if i > 0 {
			previous := meta.Sources[i-1]
			if source.Writer < previous.Writer || source.Writer == previous.Writer && source.Sequence != previous.Sequence+1 || source.Writer > previous.Writer && source.Sequence != 0 {
				return fmt.Errorf("compact sources are not ordered")
			}
		} else if source.Sequence != 0 {
			return fmt.Errorf("compact sources are not ordered")
		}
		if !source.Copied && source.Payload.Key != source.Source {
			return fmt.Errorf("direct compact source %d has mismatched payload", i)
		}
		next += uint64(source.Entries)
		if next > uint64(math.MaxUint32)+1 {
			return fmt.Errorf("compact ordinal overflow")
		}
	}
	if err := validateActorRanges(meta.Actors, meta.Index.Size); err != nil {
		return fmt.Errorf("compact index: %w", err)
	}
	return nil
}

func validateActorRanges(ranges map[uint32]Range, size int64) error {
	if len(ranges) == 0 {
		return fmt.Errorf("missing actor bitmap ranges")
	}
	actors := make([]uint32, 0, len(ranges))
	for actor := range ranges {
		actors = append(actors, actor)
	}
	slices.Sort(actors)
	var offset int64
	for _, actor := range actors {
		r := ranges[actor]
		if r.Offset != offset || r.Size <= 0 || r.Offset > math.MaxInt64-r.Size {
			return fmt.Errorf("actor %d has invalid bitmap range", actor)
		}
		offset += r.Size
	}
	if offset != size {
		return fmt.Errorf("actor bitmap ranges do not cover object prefix")
	}
	return nil
}

func validWriterID(value string) bool {
	if len(value) != 16 {
		return false
	}
	for i := range value {
		c := value[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
