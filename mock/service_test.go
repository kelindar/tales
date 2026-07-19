package mock

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestService(t *testing.T) {
	tests := map[string]func(*testing.T){
		"log query":     testLogQuery,
		"intersection":  testQueryIntersection,
		"close resets":  testCloseResets,
		"query filters": testQueryFilters,
		"edges":         testServiceEdges,
	}
	for name, fn := range tests {
		t.Run(name, fn)
	}
}

func testLogQuery(t *testing.T) {
	svc := NewService(2)
	now := time.Now()

	svc.Log("first", 1)
	svc.Log("second", 2)
	svc.Log("third", 1)

	from := now.Add(-time.Minute)
	to := now.Add(time.Minute)

	var results []string
	for event, err := range svc.Query(context.Background(), from, to, 1) {
		assert.NoError(t, err)
		results = append(results, event.Text())
	}
	assert.Equal(t, []string{"third"}, results)

	results = results[:0]
	for event, err := range svc.Query(context.Background(), from, to, 2) {
		assert.NoError(t, err)
		results = append(results, event.Text())
	}
	assert.Equal(t, []string{"second"}, results)
}

func testQueryIntersection(t *testing.T) {
	svc := NewService(3)
	now := time.Now()
	svc.Log("a", 1)
	svc.Log("b", 1, 2)
	svc.Log("c", 2)

	from := now.Add(-time.Minute)
	to := now.Add(time.Minute)

	var res []string
	for event, err := range svc.Query(context.Background(), from, to, 1, 2) {
		assert.NoError(t, err)
		res = append(res, event.Text())
	}
	assert.Equal(t, []string{"b"}, res)
}

func testCloseResets(t *testing.T) {
	svc := NewService(2)
	svc.Log("pending", 1)

	err := svc.Close()
	assert.NoError(t, err)
	assert.Equal(t, 2, svc.capacity)
	assert.Nil(t, svc.buf)
	assert.Zero(t, svc.size)
	assert.Zero(t, svc.next)
	assert.True(t, svc.closed)
	assert.Error(t, svc.Log("late", 1))
	assert.Error(t, svc.Sync(context.Background()))
	assert.Error(t, svc.Close())
}

func testQueryFilters(t *testing.T) {
	svc := NewService(4)
	now := time.Now()
	svc.Log("keep", 1)
	svc.Log("skip-actors", 2)

	from := now.Add(-time.Minute)
	to := now.Add(time.Minute)

	var texts []string
	for event, err := range svc.Query(context.Background(), from, to, 1) {
		assert.NoError(t, err)
		texts = append(texts, event.Text())
	}
	assert.Equal(t, []string{"keep"}, texts)

	for range svc.Query(context.Background(), now.Add(time.Hour), now.Add(2*time.Hour), 1) {
		t.Fatal("expected no results outside time range")
	}

	count := 0
	svc.Log("second", 1)
	for range svc.Query(context.Background(), from, to, 1) {
		count++
		break
	}
	assert.Equal(t, 1, count)
}

func testServiceEdges(t *testing.T) {
	svc := NewService(0)
	require.Equal(t, 1, svc.capacity)
	require.Error(t, svc.Log("", 1))
	require.Error(t, svc.Log("x"))
	require.Error(t, svc.Sync(nil))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, svc.Sync(ctx), context.Canceled)

	require.NoError(t, svc.Log("ok", 1))
	require.NoError(t, svc.Sync(context.Background()))

	for _, err := range svc.Query(nil, time.Now().Add(-time.Minute), time.Now(), 1) {
		require.Error(t, err)
		break
	}
	for _, err := range svc.Query(context.Background(), time.Now(), time.Now().Add(-time.Minute), 1) {
		require.Error(t, err)
		break
	}
	for _, err := range svc.Query(context.Background(), time.Now().Add(-time.Minute), time.Now()) {
		require.Error(t, err)
		break
	}

	require.NoError(t, svc.Close())
	for _, err := range svc.Query(context.Background(), time.Now().Add(-time.Minute), time.Now(), 1) {
		require.Error(t, err)
		break
	}
}
