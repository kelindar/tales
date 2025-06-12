package buffer

import (
	"sync"
	"testing"
	"time"

	"github.com/kelindar/threads/internal/codec"
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
	assert.Len(t, results, 2)

	// Query for actor 20
	results = buf.Query(20, dayStart, dayStart, dayStart.Add(time.Hour))
	assert.Len(t, results, 1)
	assert.Equal(t, entry1.ID(), results[0].ID())

	// Query for actor 30
	results = buf.Query(30, dayStart, dayStart, dayStart.Add(time.Hour))
	assert.Len(t, results, 1)
	assert.Equal(t, entry2.ID(), results[0].ID())

	// Query for non-existent actor
	results = buf.Query(99, dayStart, dayStart, dayStart.Add(time.Hour))
	assert.Empty(t, results)
}

func TestBuffer_Concurrency(t *testing.T) {
	c, _ := codec.NewCodec()
	buf := New(1000, c)
	var wg sync.WaitGroup

	// Concurrent adds
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			entry, _ := codec.NewLogEntry(uint32(i), "test", []uint32{uint32(i)})
			buf.Add(entry)
		}(i)
	}

	wg.Wait()
	assert.Equal(t, 100, buf.length)

	// Concurrent flush and adds
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = buf.Flush()
	}()
	go func() {
		defer wg.Done()
		entry, _ := codec.NewLogEntry(101, "test", []uint32{101})
		buf.Add(entry)
	}()

	wg.Wait()
}
