package tales

import (
	"strings"
	"testing"
	"time"

	"github.com/kelindar/tales/internal/codec"
	"github.com/stretchr/testify/require"
)

func TestCursor(t *testing.T) {
	entry, err := codec.NewLogEntry(1, "event", []uint32{1})
	require.NoError(t, err)
	ref := eventRef{
		event:    codec.NewEvent(time.Unix(0, 0).UTC(), entry),
		millis:   1,
		writer:   0x0123456789abcdef,
		position: 42,
	}
	cursor, err := encodeCursor(ref)
	require.NoError(t, err)
	require.Len(t, cursor.String(), 27)
	require.Equal(t, cursor, Cursor(cursor.String()))
	position, err := decodeCursor(cursor)
	require.NoError(t, err)
	require.Equal(t, &cursorPosition{millis: 1, writer: ref.writer, position: ref.position}, position)

	require.Empty(t, Zero.String())
	position, err = decodeCursor(Zero)
	require.NoError(t, err)
	require.Nil(t, position)

	for _, value := range []string{"!", "AA", strings.Repeat("A", 27), strings.Repeat("A", 100)} {
		_, err := decodeCursor(Cursor(value))
		require.Error(t, err)
	}

	_, err = encodeCursor(eventRef{position: ^uint64(0)})
	require.Error(t, err)
}
