package tales

import (
	"context"
	"testing"
	"time"

	s3mock "github.com/kelindar/s3/mock"
	"github.com/stretchr/testify/require"
)

func TestChunkKey(t *testing.T) {
	require.Equal(t, "2026-07-19/writers/0123456789abcdef/manifest.json", keyOfManifest("2026-07-19", "0123456789abcdef"))
	require.Equal(t, "2026-07-19/writers/0123456789abcdef/00000000000000000005.log", keyOfChunk("2026-07-19", "0123456789abcdef", 5))
}

func TestPendingCutoff(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	first := testService(t, server, "cutoff", "writer", func(c *config) { c.now = func() time.Time { return now } })
	require.NoError(t, first.Log("committed", 1))
	require.NoError(t, first.Sync(context.Background()))
	require.NoError(t, first.Close())

	second := testService(t, server, "cutoff", "writer", func(c *config) { c.now = func() time.Time { return now } })
	client := &failingClient{Client: second.s3Client, failManifest: true}
	second.s3Client = client
	defer second.Close()
	require.NoError(t, second.Log("pending", 1))
	events := collectEvents(t, second.Query(context.Background(), now, now, 1))
	require.Equal(t, []string{"committed", "pending"}, eventTexts(events))
	require.Error(t, second.Sync(context.Background()))
	events = collectEvents(t, second.Query(context.Background(), now, now, 1))
	require.Equal(t, []string{"committed", "pending"}, eventTexts(events))
	client.failManifest = false
}

func TestSyncCancellation(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	service := testService(t, server, "cancel-sync", "writer", func(c *config) { c.now = func() time.Time { return now } })
	defer service.Close()
	require.NoError(t, service.Log("one", 1))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, service.Sync(ctx), context.Canceled)
	require.NoError(t, service.Sync(context.Background()))
	require.Equal(t, []string{"one"}, eventTexts(collectEvents(t, service.Query(context.Background(), now, now, 1))))
}
