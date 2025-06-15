package buffer

import (
	"testing"
	"time"

	"github.com/kelindar/tales/internal/codec"
	"github.com/kelindar/tales/internal/seq"
	"github.com/stretchr/testify/assert"
)

func TestNewBuffer(t *testing.T) {
	c, _ := codec.NewCodec()
	buf := New(100, c)

	assert.NotNil(t, buf)
	assert.Equal(t, 100, buf.maxSize)
	assert.NotNil(t, buf.codec)
	assert.NotNil(t, buf.data)
	assert.NotNil(t, buf.index)
	assert.Equal(t, 0, buf.length)
}

func TestBuffer_Add(t *testing.T) {
	c, _ := codec.NewCodec()
	buf := New(2, c)

	entry1, _ := codec.NewLogEntry(1, "test1", []uint32{10, 20})
	entry2, _ := codec.NewLogEntry(2, "test2", []uint32{30})
	entry3, _ := codec.NewLogEntry(3, "test3", []uint32{40})

	assert.True(t, buf.Add(entry1))
	assert.Equal(t, 1, buf.length)
	assert.True(t, buf.Add(entry2))
	assert.Equal(t, 2, buf.length)
	assert.False(t, buf.Add(entry3)) // Buffer is full
	assert.Equal(t, 2, buf.length)
}

func TestBuffer_Flush(t *testing.T) {
	c, _ := codec.NewCodec()
	buf := New(10, c)

	entry, _ := codec.NewLogEntry(1, "test", []uint32{10, 20})
	buf.Add(entry)

	flushResult, err := buf.Flush()
	assert.NoError(t, err)

	// Check data
	assert.True(t, flushResult.Data.UncompressedSize > 0)
	assert.True(t, flushResult.Data.CompressedSize > 0)
	assert.NotEmpty(t, flushResult.Data.CompressedData)

	// Check bitmaps
	assert.Len(t, flushResult.Index, 2)
	for _, index := range flushResult.Index {
		assert.Contains(t, []uint32{10, 20}, index.ActorID)
		assert.True(t, index.UncompressedSize > 0)
		assert.True(t, index.CompressedSize > 0)
		assert.NotEmpty(t, index.CompressedData)
	}

	// Check if buffer is reset
	assert.Equal(t, 0, buf.length)
	assert.Empty(t, buf.data)
}

func TestBuffer_Query(t *testing.T) {
	c, _ := codec.NewCodec()
	buf := New(10, c)
	dayStart := time.Now().UTC().Truncate(24 * time.Hour)

	entry1, _ := codec.NewLogEntry(1, "test1", []uint32{10, 20})
	entry2, _ := codec.NewLogEntry(2, "test2", []uint32{10, 30})
	buf.Add(entry1)
	buf.Add(entry2)

	// Query for actor 10
	results := buf.Query(10, dayStart, dayStart, dayStart.Add(time.Hour))
	count := 0
	for range results {
		count++
	}
	assert.Equal(t, 2, count)

	// Query for actor 20
	results = buf.Query(20, dayStart, dayStart, dayStart.Add(time.Hour))
	entries := []codec.LogEntry{}
	for entry := range results {
		entries = append(entries, entry)
	}
	assert.Len(t, entries, 1)
	assert.Equal(t, entry1.ID(), entries[0].ID())

	// Query for actor 30
	results = buf.Query(30, dayStart, dayStart, dayStart.Add(time.Hour))
	entries = entries[:0]
	for entry := range results {
		entries = append(entries, entry)
	}
	assert.Len(t, entries, 1)
	assert.Equal(t, entry2.ID(), entries[0].ID())

	// Query for non-existent actor
	results = buf.Query(99, dayStart, dayStart, dayStart.Add(time.Hour))
	count = 0
	for range results {
		count++
	}
	assert.Equal(t, 0, count)
}

func TestBuffer_QueryInclusiveTo(t *testing.T) {
	c, _ := codec.NewCodec()
	buf := New(10, c)
	dayStart := time.Now().UTC().Truncate(24 * time.Hour)
	sg := seq.NewSequence(dayStart)

	ts := dayStart.Add(10*time.Minute + 30*time.Second)
	id := sg.Next(ts)
	entry, _ := codec.NewLogEntry(id, "test", []uint32{1})
	buf.Add(entry)

	results := buf.Query(1, dayStart, dayStart, ts)
	count := 0
	for range results {
		count++
	}
	assert.Equal(t, 1, count)
}

func TestBuffer_QueryActors(t *testing.T) {
	c, _ := codec.NewCodec()
	buf := New(10, c)
	dayStart := time.Now().UTC().Truncate(24 * time.Hour)

	// Create entries with different actor combinations
	entry1, _ := codec.NewLogEntry(1, "test1", []uint32{10, 20})     // actors 10, 20
	entry2, _ := codec.NewLogEntry(2, "test2", []uint32{10, 30})     // actors 10, 30
	entry3, _ := codec.NewLogEntry(3, "test3", []uint32{10, 20, 30}) // actors 10, 20, 30
	entry4, _ := codec.NewLogEntry(4, "test4", []uint32{40})         // actor 40
	buf.Add(entry1)
	buf.Add(entry2)
	buf.Add(entry3)
	buf.Add(entry4)

	// Query for single actor (should work like old Query)
	results := buf.QueryActors(dayStart, dayStart, dayStart.Add(time.Hour), []uint32{10})
	count := 0
	for range results {
		count++
	}
	assert.Equal(t, 3, count) // entries 1, 2, 3 have actor 10

	// Query for intersection of two actors
	results = buf.QueryActors(dayStart, dayStart, dayStart.Add(time.Hour), []uint32{10, 20})
	entries := []codec.LogEntry{}
	for entry := range results {
		entries = append(entries, entry)
	}
	assert.Len(t, entries, 2) // entries 1 and 3 have both actors 10 and 20
	assert.Equal(t, entry1.ID(), entries[0].ID())
	assert.Equal(t, entry3.ID(), entries[1].ID())

	// Query for intersection of three actors
	results = buf.QueryActors(dayStart, dayStart, dayStart.Add(time.Hour), []uint32{10, 20, 30})
	entries = entries[:0]
	for entry := range results {
		entries = append(entries, entry)
	}
	assert.Len(t, entries, 1) // only entry 3 has all three actors
	assert.Equal(t, entry3.ID(), entries[0].ID())

	// Query for non-intersecting actors
	results = buf.QueryActors(dayStart, dayStart, dayStart.Add(time.Hour), []uint32{20, 40})
	count = 0
	for range results {
		count++
	}
	assert.Equal(t, 0, count) // no entry has both actors 20 and 40

	// Query with empty actors list
	results = buf.QueryActors(dayStart, dayStart, dayStart.Add(time.Hour), []uint32{})
	count = 0
	for range results {
		count++
	}
	assert.Equal(t, 0, count) // empty actors should return no results
}
