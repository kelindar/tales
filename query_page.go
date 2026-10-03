// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package tales

import (
	"slices"

	"github.com/kelindar/tales/internal/codec"
)

// eventSelection owns query references. A zero limit retains every match;
// pages keep only the first limit references after the exclusive cursor.
type eventSelection struct {
	refs      []eventRef
	limit     int
	ascending bool
	position  *cursorPosition
}

func (s *eventSelection) reserve(count int) {
	if s.limit > 0 {
		count = min(count, s.limit-len(s.refs))
	}
	s.refs = slices.Grow(s.refs, count)
}

func (s *eventSelection) add(ref eventRef) {
	if s.limit == 0 {
		s.refs = append(s.refs, ref)
		return
	}
	if skipBeforeCursor(ref, s.position, s.ascending) {
		return
	}
	if len(s.refs) < s.limit {
		s.refs = append(s.refs, ref)
		if len(s.refs) == s.limit {
			for i := len(s.refs)/2 - 1; i >= 0; i-- {
				s.down(i)
			}
		}
		return
	}
	if !s.before(&ref, &s.refs[0]) {
		return
	}
	s.refs[0] = ref
	s.down(0)
}

func (s *eventSelection) before(a, b *eventRef) bool {
	if a.millis != b.millis {
		return (a.millis < b.millis) == s.ascending
	}
	if a.writer != b.writer {
		return (a.writer < b.writer) == s.ascending
	}
	return a.position != b.position && (a.position < b.position) == s.ascending
}

// Bounds are optimistic: timestamp extrema need not occur at ordinal extrema.
// Including writer and ordinal makes pruning safe even when timestamps tie.
func (s *eventSelection) skipBlock(block codec.Block, dayMillis int64, writer, base uint64) bool {
	if s.limit == 0 || block.Time == nil {
		return false
	}
	first := eventRef{millis: dayMillis + int64(block.Time[0]), writer: writer, position: base + uint64(block.First)}
	last := eventRef{millis: dayMillis + int64(block.Time[1]), writer: writer, position: base + uint64(block.First) + uint64(block.Entries) - 1}
	if !s.ascending {
		first, last = last, first
	}
	if skipBeforeCursor(last, s.position, s.ascending) {
		return true
	}
	return len(s.refs) == s.limit && !s.before(&first, &s.refs[0])
}

// Partial blocks can yield fewer matches than their bitmap count. Coalesce
// them, and competing blocks once full, to avoid follow-up reads.
func (s *eventSelection) blockLimit(block codec.Block, dayMillis int64, from, to uint32) int {
	if s.limit == 0 || len(s.refs) == s.limit || block.Time == nil {
		return 0
	}
	first, last := dayMillis+int64(block.Time[0]), dayMillis+int64(block.Time[1])
	switch {
	case block.Time[0] < from || block.Time[1] > to:
		return 0
	case s.position != nil && s.position.millis >= first && s.position.millis <= last:
		return 0
	}
	return s.limit - len(s.refs)
}

// The worst retained candidate stays at the root, so rejected events need
// only one comparison.
func (s *eventSelection) down(index int) {
	ref := s.refs[index]
	for child := index*2 + 1; child < len(s.refs); child = index*2 + 1 {
		if child+1 < len(s.refs) && s.before(&s.refs[child], &s.refs[child+1]) {
			child++
		}
		if !s.before(&ref, &s.refs[child]) {
			break
		}
		s.refs[index] = s.refs[child]
		index = child
	}
	s.refs[index] = ref
}
