package tales

import (
	"context"
	"testing"
	"time"

	s3mock "github.com/kelindar/s3/mock"
	"github.com/stretchr/testify/require"
)

func TestCompactKeys(t *testing.T) {
	require.Equal(t, "2026-07-19/compact/index.bin", keyOfCompactIndex("2026-07-19"))
	require.Equal(t, "2026-07-19/compact/data.log", keyOfCompactData("2026-07-19"))
	require.Equal(t, "2026-07-19/compact/metadata.json", keyOfCompactMeta("2026-07-19"))
}

func TestCompactEdges(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	service := testService(t, server, "compact-edges", "compactor", func(c *config) { c.now = func() time.Time { return now } })
	defer service.Close()
	day := now.AddDate(0, 0, -2)

	require.Error(t, service.Compact(nil, day))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, service.Compact(ctx, day), context.Canceled)
	require.NoError(t, service.Compact(context.Background(), day))
	require.False(t, server.ObjectExists("compact-edges/"+keyOfCompactMeta(dayKey(day))))
}
