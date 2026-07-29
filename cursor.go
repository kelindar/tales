// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package tales

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
)

// Cursor is an opaque URL-safe position in Tales' deterministic event order.
// Reuse it with the same time range and actors.
type Cursor string

// Zero starts paging at the first event in the requested direction.
const Zero Cursor = ""

type cursorPosition struct {
	millis   int64
	writer   uint64
	position uint64
}

// String returns the unpadded URL-safe cursor string.
func (c Cursor) String() string { return string(c) }

func encodeCursor(ref eventRef) (Cursor, error) {
	// ponytail: 32-bit writer-day positions cap paging below 2^32 events/day;
	// restore separate sequence and ordinal fields if that ceiling matters.
	if ref.position >= math.MaxUint32 {
		return Zero, fmt.Errorf("writer-day position exceeds cursor capacity")
	}
	var data [20]byte
	binary.BigEndian.PutUint64(data[0:8], uint64(ref.millis))
	binary.BigEndian.PutUint64(data[8:16], ref.writer)
	binary.BigEndian.PutUint32(data[16:20], uint32(ref.position)+1)
	return Cursor(base64.RawURLEncoding.EncodeToString(data[:])), nil
}

func decodeCursor(cursor Cursor) (*cursorPosition, error) {
	switch {
	case cursor == Zero:
		return nil, nil
	case len(cursor) != 27:
		return nil, fmt.Errorf("invalid cursor length %d", len(cursor))
	}
	var data [20]byte
	n, err := base64.RawURLEncoding.Decode(data[:], []byte(cursor))
	switch {
	case err != nil:
		return nil, fmt.Errorf("decode cursor: %w", err)
	case n != len(data):
		return nil, fmt.Errorf("invalid cursor length %d", n)
	}
	position := binary.BigEndian.Uint32(data[16:20])
	if position == 0 {
		return nil, fmt.Errorf("invalid cursor position")
	}
	return &cursorPosition{
		millis:   int64(binary.BigEndian.Uint64(data[0:8])),
		writer:   binary.BigEndian.Uint64(data[8:16]),
		position: uint64(position - 1),
	}, nil
}
