package machine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestID(t *testing.T) {
	first := ID()
	require.Equal(t, first, ID())
	require.NotZero(t, first)
	require.Equal(t, uint32(pid), uint32(first))
}
