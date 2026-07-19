package tales

import (
	"context"
	"crypto/rand"
	"fmt"
	"iter"
	"strings"
	"sync"
	"testing"
	"time"

	s3lib "github.com/kelindar/s3"
	s3mock "github.com/kelindar/s3/mock"
	internals3 "github.com/kelindar/tales/internal/s3"
	"github.com/stretchr/testify/require"
)

func TestDistributedWriters(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	now := time.Date(2026, 7, 19, 12, 0, 0, 123_000_000, time.UTC)
	a := testService(t, server, "shared", "writer-a", func(c *config) { c.now = func() time.Time { return now } })
	b := testService(t, server, "shared", "writer-b", func(c *config) { c.now = func() time.Time { return now } })
	defer a.Close()
	defer b.Close()

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, writer := range []*Service{a, b} {
		writer := writer
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := writer.Log(writer.config.WriterID+"-0", 1); err != nil {
				errs <- err
				return
			}
			if err := writer.Log(writer.config.WriterID+"-1", 1); err != nil {
				errs <- err
				return
			}
			errs <- writer.Sync(context.Background())
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	reader := testService(t, server, "shared", "reader", func(c *config) { c.now = func() time.Time { return now } })
	defer reader.Close()
	events := collectEvents(t, reader.Query(context.Background(), now.Add(-time.Second), now.Add(time.Second), 1))
	require.Len(t, events, 4)
	writers := []*Service{a, b}
	if writers[1].config.WriterID < writers[0].config.WriterID {
		writers[0], writers[1] = writers[1], writers[0]
	}
	require.Equal(t, []string{
		writers[0].config.WriterID + "-0",
		writers[0].config.WriterID + "-1",
		writers[1].config.WriterID + "-0",
		writers[1].config.WriterID + "-1",
	}, eventTexts(events))
	for _, event := range events {
		require.Equal(t, now, event.Time())
	}
}

func TestWarmQuery(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	now := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	service := testService(t, server, "warm", "writer", func(c *config) { c.now = func() time.Time { return now } })
	defer service.Close()

	require.NoError(t, service.Log(`{"value":1}`, 1, 2))
	events := collectEvents(t, service.Query(context.Background(), now, now, 1, 2))
	require.Len(t, events, 1)
	require.Equal(t, events[0].Bytes(), []byte(events[0].JSON()))
	clone := events[0].Clone()
	events[0].Bytes()[0] = 'x'
	require.Equal(t, byte('{'), clone.Bytes()[0])
	require.NoError(t, service.Sync(context.Background()))
	require.True(t, server.ObjectExists("warm/"+keyOfManifest(dayKey(now), service.config.WriterID)))
}

func TestCloseRetry(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	service := testService(t, server, "retry", "writer")
	client := &failingClient{Client: service.s3Client, failManifest: true}
	service.s3Client = client
	require.NoError(t, service.Log("one", 1))
	require.Error(t, service.Close())
	require.NoError(t, service.Log("two", 1))
	client.failManifest = false
	require.NoError(t, service.Sync(context.Background()))

	now := time.Now()
	events := collectEvents(t, service.Query(context.Background(), now.Add(-time.Hour), now.Add(time.Hour), 1))
	require.Equal(t, []string{"one", "two"}, eventTexts(events))
	require.NoError(t, service.Close())
}

func TestAmbiguousCommit(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	service := testService(t, server, "ambiguous", "writer")
	client := &ambiguousClient{Client: service.s3Client, failManifest: true}
	service.s3Client = client
	defer service.Close()

	require.NoError(t, service.Log("once", 1))
	require.Error(t, service.Sync(context.Background()))
	require.NoError(t, service.Sync(context.Background()))
	manifest, err := service.downloadManifest(context.Background(), dayKey(time.Now()), service.config.WriterID)
	require.NoError(t, err)
	require.Len(t, manifest.Chunks, 1)
	events := collectEvents(t, service.Query(context.Background(), time.Now().Add(-time.Hour), time.Now().Add(time.Hour), 1))
	require.Equal(t, []string{"once"}, eventTexts(events))
}

func TestWarmSnapshot(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	service := testService(t, server, "snapshot", "writer")
	defer service.Close()
	require.NoError(t, service.Log("once", 1))

	client := &blockingListClient{Client: service.s3Client, started: make(chan struct{}), release: make(chan struct{})}
	service.s3Client = client
	type queryResult struct {
		texts []string
		err   error
	}
	done := make(chan queryResult, 1)
	go func() {
		var result queryResult
		for event, err := range service.Query(context.Background(), time.Now().Add(-time.Hour), time.Now().Add(time.Hour), 1) {
			if err != nil {
				result.err = err
				break
			}
			result.texts = append(result.texts, event.Text())
		}
		done <- result
	}()
	<-client.started
	require.NoError(t, service.Sync(context.Background()))
	close(client.release)
	got := <-done
	require.NoError(t, got.err)
	require.Equal(t, []string{"once"}, got.texts)
}

func TestDiscoveryCache(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	service := testService(t, server, "cache", "writer")
	require.NoError(t, service.Log("one", 1))
	require.NoError(t, service.Sync(context.Background()))
	from, to := time.Now().Add(-time.Hour), time.Now().Add(time.Hour)
	collectEvents(t, service.Query(context.Background(), from, to, 1))
	lists := countMethod(server, "GET", "writers")
	collectEvents(t, service.Query(context.Background(), from, to, 1))
	require.Equal(t, lists, countMethod(server, "GET", "writers"))
	require.NoError(t, service.Close())
}

func TestCompactionEquivalence(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	old := time.Date(2026, 7, 15, 10, 0, 0, 0, time.UTC)
	writer := testService(t, server, "compact", "writer", func(c *config) { c.now = func() time.Time { return old } })
	require.NoError(t, writer.Log("one", 1))
	require.NoError(t, writer.Log("two", 1, 2))
	require.NoError(t, writer.Sync(context.Background()))
	require.NoError(t, writer.Close())

	future := old.Add(72 * time.Hour)
	service := testService(t, server, "compact", "compactor", func(c *config) { c.now = func() time.Time { return future } })
	defer service.Close()
	from, to := old.Add(-time.Hour), old.Add(time.Hour)
	before := eventTexts(collectEvents(t, service.Query(context.Background(), from, to, 1)))
	require.NoError(t, service.Compact(context.Background(), old))
	after := eventTexts(collectEvents(t, service.Query(context.Background(), from, to, 1)))
	require.Equal(t, before, after)
	require.True(t, server.ObjectExists("compact/"+keyOfCompactMeta(dayKey(old))))
	require.NoError(t, service.Compact(context.Background(), old))
	require.Error(t, service.Compact(context.Background(), future.Add(-24*time.Hour)))

	late := testService(t, server, "compact", "late", func(c *config) { c.now = func() time.Time { return old } })
	require.NoError(t, late.Log("late", 1))
	require.Error(t, late.Sync(context.Background()))
	require.NoError(t, late.s3Client.Delete(context.Background(), keyOfCompactMeta(dayKey(old))))
	require.NoError(t, late.Close())
}

func TestCompactionCommit(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	old := time.Date(2026, 7, 10, 10, 0, 0, 0, time.UTC)
	writer := testService(t, server, "commit", "writer", func(c *config) { c.now = func() time.Time { return old } })
	require.NoError(t, writer.Log("one", 1))
	require.NoError(t, writer.Sync(context.Background()))
	require.NoError(t, writer.Close())

	now := old.Add(72 * time.Hour)
	service := testService(t, server, "commit", "compactor", func(c *config) { c.now = func() time.Time { return now } })
	client := &suffixFailClient{Client: service.s3Client, suffix: "/compact/metadata.json", fail: true}
	service.s3Client = client
	defer service.Close()
	require.Error(t, service.Compact(context.Background(), old))
	require.False(t, server.ObjectExists("commit/"+keyOfCompactMeta(dayKey(old))))
	require.Equal(t, []string{"one"}, eventTexts(collectEvents(t, service.Query(context.Background(), old.Add(-time.Hour), old.Add(time.Hour), 1))))
	client.fail = false
	require.NoError(t, service.Compact(context.Background(), old))
}

func TestServerCopy(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	old := time.Date(2026, 7, 10, 10, 0, 0, 0, time.UTC)
	writer := testService(t, server, "copy", "writer", func(c *config) {
		c.now = func() time.Time { return old }
		c.BufferSize = 90
	})
	payload := make([]byte, 60_000)
	for range 180 {
		_, err := rand.Read(payload)
		require.NoError(t, err)
		require.NoError(t, writer.Log(string(payload), 1))
	}
	require.NoError(t, writer.Sync(context.Background()))
	manifest, err := writer.downloadManifest(context.Background(), dayKey(old), writer.config.WriterID)
	require.NoError(t, err)
	require.Len(t, manifest.Chunks, 2)
	for _, chunk := range manifest.Chunks {
		require.GreaterOrEqual(t, chunk.Data.Size, int64(s3lib.MinPartSize))
	}
	firstSource := "copy/" + keyOfChunk(dayKey(old), writer.config.WriterID, 0)
	secondSource := "copy/" + keyOfChunk(dayKey(old), writer.config.WriterID, 1)
	require.NoError(t, writer.Close())

	clock := old.Add(72 * time.Hour)
	service := testService(t, server, "copy", "compactor", func(c *config) {
		c.now = func() time.Time { return clock }
		c.composeParts = 1
	})
	defer service.Close()
	firstBefore := countPath(server, "GET", firstSource)
	secondBefore := countPath(server, "GET", secondSource)
	require.NoError(t, service.Compact(context.Background(), old))
	require.Equal(t, 1, countPath(server, "GET", firstSource)-firstBefore, "compactor should GET only the actor bitmap")
	require.Equal(t, 1, countPath(server, "GET", secondSource)-secondBefore, "compactor should GET only the actor bitmap")
	meta, ok, err := service.compactMetadata(context.Background(), dayKey(old))
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, meta.Sources[0].Copied)
	require.False(t, meta.Sources[1].Copied)

	clock = clock.Add(25 * time.Hour)
	cleanup := &deleteFailClient{Client: service.s3Client, fail: true}
	service.s3Client = cleanup
	require.Error(t, service.Compact(context.Background(), old))
	require.Len(t, collectEvents(t, service.Query(context.Background(), old.Add(-time.Hour), old.Add(time.Hour), 1)), 180)
	cleanup.fail = false
	require.NoError(t, service.Compact(context.Background(), old))
	require.False(t, server.ObjectExists(firstSource))
	require.True(t, server.ObjectExists(secondSource))
	require.NoError(t, service.Compact(context.Background(), old))
	events := collectEvents(t, service.Query(context.Background(), old.Add(-time.Hour), old.Add(time.Hour), 1))
	require.Len(t, events, 180)
}

func TestQueryErrors(t *testing.T) {
	t.Run("yields cancellation once", func(t *testing.T) {
		server := s3mock.New("events", "us-east-1")
		defer server.Close()
		service := testService(t, server, "errors", "writer")
		defer service.Close()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		count := 0
		for _, err := range service.Query(ctx, time.Now().Add(-time.Hour), time.Now(), 1) {
			require.ErrorIs(t, err, context.Canceled)
			count++
		}
		require.Equal(t, 1, count)
	})

	t.Run("yields invalid arguments once", func(t *testing.T) {
		server := s3mock.New("events", "us-east-1")
		defer server.Close()
		service := testService(t, server, "arguments", "writer")
		defer service.Close()
		count := 0
		for _, err := range service.Query(context.Background(), time.Now(), time.Now().Add(-time.Hour), 1) {
			require.Error(t, err)
			count++
		}
		require.Equal(t, 1, count)
	})

	t.Run("yields malformed manifest once", func(t *testing.T) {
		server := s3mock.New("events", "us-east-1")
		defer server.Close()
		now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
		writer := testService(t, server, "malformed", "writer", func(c *config) { c.now = func() time.Time { return now } })
		require.NoError(t, writer.Log("one", 1))
		require.NoError(t, writer.Sync(context.Background()))
		_, err := writer.s3Client.Upload(context.Background(), keyOfManifest(dayKey(now), writer.config.WriterID), []byte("not json"))
		require.NoError(t, err)
		require.NoError(t, writer.Close())

		reader := testService(t, server, "malformed", "reader", func(c *config) { c.now = func() time.Time { return now } })
		defer reader.Close()
		count := 0
		for _, err := range reader.Query(context.Background(), now.Add(-time.Hour), now.Add(time.Hour), 1) {
			require.Error(t, err)
			count++
		}
		require.Equal(t, 1, count)
	})
}

func TestWriterID(t *testing.T) {
	var a, b config
	WithWriterID("same")(&a)
	WithWriterID("same")(&b)
	require.Equal(t, a.WriterID, b.WriterID)
	require.Len(t, a.WriterID, 16)
	WithWriterID("hello")(&a)
	require.Equal(t, "a430d84680aabd0b", a.WriterID)
}

func TestLifecycle(t *testing.T) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	service := testService(t, server, "lifecycle", "writer")
	require.NoError(t, service.Log("seed", 1))

	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, operation := range []func(){
		func() {
			for range 100 {
				_ = service.Log("event", 1)
			}
		},
		func() {
			for range 20 {
				_ = service.Sync(context.Background())
			}
		},
		func() {
			for range 20 {
				for range service.Query(context.Background(), time.Now().Add(-time.Hour), time.Now().Add(time.Hour), 1) {
				}
			}
		},
	} {
		operation := operation
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			operation()
		}()
	}
	close(start)
	require.NoError(t, service.Close())
	wg.Wait()
	require.Error(t, service.Log("late", 1))
}

func BenchmarkLog(b *testing.B) {
	server := s3mock.New("events", "us-east-1")
	defer server.Close()
	service := testService(b, server, "bench", "writer")
	defer service.Close()
	b.ResetTimer()
	for range b.N {
		_ = service.Log("hello", 1)
	}
}

func testService(t testing.TB, server *s3mock.Server, prefix, writer string, extra ...Option) *Service {
	options := []Option{
		WithPrefix(prefix), WithWriterID(writer), WithInterval(time.Minute),
		WithClient(func(cfg internals3.Config) (internals3.Client, error) { return internals3.NewMockClient(server, cfg) }),
	}
	options = append(options, extra...)
	service, err := New("events", "us-east-1", options...)
	require.NoError(t, err)
	return service
}

func collectEvents(t testing.TB, sequence func(func(Event, error) bool)) []Event {
	t.Helper()
	var events []Event
	sequence(func(event Event, err error) bool {
		require.NoError(t, err)
		events = append(events, event)
		return true
	})
	return events
}

func eventTexts(events []Event) []string {
	texts := make([]string, len(events))
	for i := range events {
		texts[i] = events[i].Text()
	}
	return texts
}

func countMethod(server *s3mock.Server, method, contains string) int {
	count := 0
	for _, request := range server.GetRequestsWithMethod(method) {
		if strings.Contains(request.Query, "list-type=2") && strings.Contains(request.Query, contains) {
			count++
		}
	}
	return count
}

func countPath(server *s3mock.Server, method, contains string) int {
	count := 0
	for _, request := range server.GetRequestsWithMethod(method) {
		if strings.Contains(request.Path, contains) {
			count++
		}
	}
	return count
}

type failingClient struct {
	internals3.Client
	failManifest bool
}

func (c *failingClient) Upload(ctx context.Context, key string, data []byte) (string, error) {
	if c.failManifest && strings.HasSuffix(key, "/manifest.json") {
		return "", fmt.Errorf("injected manifest failure")
	}
	return c.Client.Upload(ctx, key, data)
}

type ambiguousClient struct {
	internals3.Client
	failManifest bool
}

type suffixFailClient struct {
	internals3.Client
	suffix string
	fail   bool
}

type deleteFailClient struct {
	internals3.Client
	fail bool
}

func (c *deleteFailClient) Delete(ctx context.Context, key string) error {
	if c.fail {
		c.fail = false
		return fmt.Errorf("injected delete failure")
	}
	return c.Client.Delete(ctx, key)
}

func (c *suffixFailClient) Upload(ctx context.Context, key string, data []byte) (string, error) {
	if c.fail && strings.HasSuffix(key, c.suffix) {
		return "", fmt.Errorf("injected upload failure")
	}
	return c.Client.Upload(ctx, key, data)
}

func (c *ambiguousClient) Upload(ctx context.Context, key string, data []byte) (string, error) {
	etag, err := c.Client.Upload(ctx, key, data)
	if err == nil && c.failManifest && strings.HasSuffix(key, "/manifest.json") {
		c.failManifest = false
		return "", fmt.Errorf("ambiguous manifest response")
	}
	return etag, err
}

type blockingListClient struct {
	internals3.Client
	started chan struct{}
	release chan struct{}
}

func (c *blockingListClient) List(ctx context.Context, prefix string) iter.Seq2[internals3.Object, error] {
	return func(yield func(internals3.Object, error) bool) {
		close(c.started)
		select {
		case <-c.release:
		case <-ctx.Done():
			yield(internals3.Object{}, ctx.Err())
			return
		}
		c.Client.List(ctx, prefix)(yield)
	}
}
