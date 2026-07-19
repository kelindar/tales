package buffer

import (
	"bytes"
	"testing"
	"time"

	"github.com/kelindar/roaring"
	"github.com/kelindar/tales/internal/codec"
	"github.com/stretchr/testify/require"
)

func TestBatchSnapshot(t *testing.T) {
	c, err := codec.NewCodec()
	require.NoError(t, err)
	defer c.Close()
	buf := New(2, c)
	day := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	first, _ := codec.NewLogEntry(10, "a", []uint32{9, 1})
	second, _ := codec.NewLogEntry(10, "b", []uint32{1})
	require.NoError(t, buf.Add(day, first))
	require.Equal(t, day, buf.Day())
	require.Positive(t, buf.Bytes())
	require.NoError(t, buf.Add(day, second))
	require.Error(t, buf.Add(day, first))

	_, snapshot, count := buf.Snapshot()
	snapshot[0] = 0
	require.NotEqual(t, snapshot[0], buf.data[0])
	require.Equal(t, uint32(2), count)

	batch, err := buf.Take()
	require.NoError(t, err)
	require.Equal(t, uint32(2), batch.Entries)
	require.Equal(t, [2]uint32{10, 10}, batch.Time)
	require.Equal(t, []uint32{1, 9}, []uint32{batch.Indexes[0].Actor, batch.Indexes[1].Actor})
	bitmap, err := roaring.ReadFrom(bytes.NewReader(batch.Indexes[0].Data))
	require.NoError(t, err)
	require.True(t, bitmap.Contains(0))
	require.True(t, bitmap.Contains(1))
	require.Zero(t, buf.Size())
	require.Zero(t, buf.Bytes())
	require.True(t, buf.Day().IsZero())

	raw, err := c.Decompress(batch.Data)
	require.NoError(t, err)
	_, err = codec.ValidateEntries(raw, 2)
	require.NoError(t, err)
}

func TestBufferDayGuard(t *testing.T) {
	c, _ := codec.NewCodec()
	defer c.Close()
	buf := New(2, c)
	entry, _ := codec.NewLogEntry(0, "x", []uint32{1})
	day := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	require.NoError(t, buf.Add(day, entry))
	require.Error(t, buf.Add(day.AddDate(0, 0, 1), entry))
}
