// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package buffer

import (
	"testing"
	"time"

	"github.com/kelindar/tales/internal/codec"
	"github.com/kelindar/tales/internal/seq"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	c, _ := codec.NewCodec()
	buf := New(100, c)

	assert.NotNil(t, buf)
	assert.Equal(t, 100, buf.maxSize)
	assert.NotNil(t, buf.codec)
	assert.NotNil(t, buf.data)
	assert.NotNil(t, buf.index)
	assert.Equal(t, 0, buf.length)
}

func TestAdd(t *testing.T) {
	c, _ := codec.NewCodec()
	buf := New(2, c)
	now := time.Now()

	entry1, _ := codec.NewLogEntry(1, "test1", []uint32{10, 20})
	entry2, _ := codec.NewLogEntry(2, "test2", []uint32{30})
	entry3, _ := codec.NewLogEntry(3, "test3", []uint32{40})

	assert.True(t, buf.Add(entry1, now))
	assert.Equal(t, 1, buf.length)
	assert.True(t, buf.Add(entry2, now.Add(time.Minute)))
	assert.Equal(t, 2, buf.length)
	assert.False(t, buf.Add(entry3, now.Add(2*time.Minute))) // Buffer is full
	assert.Equal(t, 2, buf.length)
}

func TestFlush(t *testing.T) {
	c, _ := codec.NewCodec()
	buf := New(10, c)
	now := time.Now()

	entry, _ := codec.NewLogEntry(1, "test", []uint32{10, 20})
	buf.Add(entry, now)

	flushResult, err := buf.Flush()
	assert.NoError(t, err)

	assert.True(t, flushResult.Data.UncompressedSize > 0)
	assert.True(t, flushResult.Data.CompressedSize > 0)
	assert.NotEmpty(t, flushResult.Data.CompressedData)

	assert.Equal(t, uint32(now.Unix()), flushResult.Time[0])
	assert.Equal(t, uint32(now.Unix()), flushResult.Time[1])

	assert.Len(t, flushResult.Index, 2)
	for _, index := range flushResult.Index {
		assert.Contains(t, []uint32{10, 20}, index.ActorID)
		assert.True(t, index.UncompressedSize > 0)
		assert.True(t, index.CompressedSize > 0)
		assert.NotEmpty(t, index.CompressedData)
	}

	assert.Equal(t, 0, buf.length)
	assert.Empty(t, buf.data)
}

func TestQuery(t *testing.T) {
	tests := map[string]func(*testing.T){
		"by actor":     testQueryByActor,
		"inclusive to": testQueryInclusiveTo,
		"all actors":   testQueryActors,
	}
	for name, fn := range tests {
		t.Run(name, fn)
	}
}

func testQueryByActor(t *testing.T) {
	c, _ := codec.NewCodec()
	buf := New(10, c)
	dayStart := time.Now().UTC().Truncate(24 * time.Hour)

	entry1, _ := codec.NewLogEntry(1, "test1", []uint32{10, 20})
	entry2, _ := codec.NewLogEntry(2, "test2", []uint32{10, 30})
	buf.Add(entry1, dayStart)
	buf.Add(entry2, dayStart.Add(time.Minute))

	results := buf.Query(10, dayStart, dayStart, dayStart.Add(time.Hour))
	count := 0
	for range results {
		count++
	}
	assert.Equal(t, 2, count)

	results = buf.Query(20, dayStart, dayStart, dayStart.Add(time.Hour))
	entries := []codec.LogEntry{}
	for entry := range results {
		entries = append(entries, entry)
	}
	assert.Len(t, entries, 1)
	assert.Equal(t, entry1.ID(), entries[0].ID())

	results = buf.Query(30, dayStart, dayStart, dayStart.Add(time.Hour))
	entries = entries[:0]
	for entry := range results {
		entries = append(entries, entry)
	}
	assert.Len(t, entries, 1)
	assert.Equal(t, entry2.ID(), entries[0].ID())

	results = buf.Query(99, dayStart, dayStart, dayStart.Add(time.Hour))
	count = 0
	for range results {
		count++
	}
	assert.Equal(t, 0, count)
}

func testQueryInclusiveTo(t *testing.T) {
	c, _ := codec.NewCodec()
	buf := New(10, c)
	dayStart := time.Now().UTC().Truncate(24 * time.Hour)
	sg := seq.NewSequence(dayStart)

	ts := dayStart.Add(10*time.Minute + 30*time.Second)
	id := sg.Next(ts)
	entry, _ := codec.NewLogEntry(id, "test", []uint32{1})
	buf.Add(entry, ts)

	results := buf.Query(1, dayStart, dayStart, ts)
	count := 0
	for range results {
		count++
	}
	assert.Equal(t, 1, count)
}

func testQueryActors(t *testing.T) {
	c, _ := codec.NewCodec()
	buf := New(10, c)
	dayStart := time.Now().UTC().Truncate(24 * time.Hour)

	entry1, _ := codec.NewLogEntry(1, "test1", []uint32{10, 20})
	entry2, _ := codec.NewLogEntry(2, "test2", []uint32{10, 30})
	entry3, _ := codec.NewLogEntry(3, "test3", []uint32{10, 20, 30})
	entry4, _ := codec.NewLogEntry(4, "test4", []uint32{40})
	buf.Add(entry1, dayStart)
	buf.Add(entry2, dayStart.Add(time.Minute))
	buf.Add(entry3, dayStart.Add(2*time.Minute))
	buf.Add(entry4, dayStart.Add(3*time.Minute))

	results := buf.QueryActors(dayStart, dayStart, dayStart.Add(time.Hour), []uint32{10})
	count := 0
	for range results {
		count++
	}
	assert.Equal(t, 3, count)

	results = buf.QueryActors(dayStart, dayStart, dayStart.Add(time.Hour), []uint32{10, 20})
	entries := []codec.LogEntry{}
	for entry := range results {
		entries = append(entries, entry)
	}
	assert.Len(t, entries, 2)
	assert.Equal(t, entry1.ID(), entries[0].ID())
	assert.Equal(t, entry3.ID(), entries[1].ID())

	results = buf.QueryActors(dayStart, dayStart, dayStart.Add(time.Hour), []uint32{10, 20, 30})
	entries = entries[:0]
	for entry := range results {
		entries = append(entries, entry)
	}
	assert.Len(t, entries, 1)
	assert.Equal(t, entry3.ID(), entries[0].ID())

	results = buf.QueryActors(dayStart, dayStart, dayStart.Add(time.Hour), []uint32{20, 40})
	count = 0
	for range results {
		count++
	}
	assert.Equal(t, 0, count)

	results = buf.QueryActors(dayStart, dayStart, dayStart.Add(time.Hour), []uint32{})
	count = 0
	for range results {
		count++
	}
	assert.Equal(t, 0, count)
}
