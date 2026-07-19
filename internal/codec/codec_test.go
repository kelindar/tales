package codec

import (
	"encoding/binary"
	"math"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestEventFrame(t *testing.T) {
	day := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	entry, err := NewLogEntry(12_345, `{"ok":true}`, []uint32{7, 9})
	require.NoError(t, err)
	validated, size, err := ValidateEntry(entry)
	require.NoError(t, err)
	require.Equal(t, len(entry), size)

	event := NewEvent(day, validated)
	require.Equal(t, day.Add(12_345*time.Millisecond), event.Time())
	require.Equal(t, `{"ok":true}`, event.Text())
	require.Equal(t, []byte(`{"ok":true}`), []byte(event.JSON()))
	require.Equal(t, []uint32{7, 9}, collectActors(event))
	require.Equal(t, unsafe.SliceData(event.Bytes()), unsafe.SliceData(event.JSON()))

	clone := event.Clone()
	event.Bytes()[0] = 'x'
	require.Equal(t, byte('{'), clone.Bytes()[0])

	count := 0
	for range event.Actors() {
		count++
		break
	}
	require.Equal(t, 1, count)
}

func TestFrameValidation(t *testing.T) {
	valid, err := NewLogEntry(MaxMillis, "x", []uint32{1})
	require.NoError(t, err)
	maximum, err := NewLogEntry(0, string(make([]byte, math.MaxUint16-headerSize-4)), []uint32{1})
	require.NoError(t, err)
	require.Len(t, maximum, math.MaxUint16)
	_, err = NewLogEntry(0, string(make([]byte, math.MaxUint16-headerSize-3)), []uint32{1})
	require.Error(t, err)
	_, err = NewLogEntry(MaxMillis+1, "x", []uint32{1})
	require.Error(t, err)
	_, err = NewLogEntry(0, string(make([]byte, 1<<16)), nil)
	require.Error(t, err)

	tests := map[string][]byte{
		"short header":          {1, 2, 3},
		"size beyond data":      makeFrame(7, 100, 8),
		"unaligned actors":      makeFrame(7, 8, 7),
		"actors beyond payload": makeFrame(7, 8, 12),
		"millis beyond day":     makeFrame(MaxMillis+1, 8, 8),
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, _, err := ValidateEntry(test)
			require.Error(t, err)
		})
	}
	_, err = ValidateEntries(append(valid, 1), 1)
	require.Error(t, err)
	_, err = ValidateEntries(valid, 2)
	require.Error(t, err)
}

func makeFrame(millis uint32, size, actorsEnd uint16) []byte {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint32(data, millis)
	binary.LittleEndian.PutUint16(data[4:], size)
	binary.LittleEndian.PutUint16(data[6:], actorsEnd)
	return data
}

func collectActors(event Event) []uint32 {
	var actors []uint32
	for actor := range event.Actors() {
		actors = append(actors, actor)
	}
	return actors
}
