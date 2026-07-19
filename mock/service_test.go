package mock

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestServiceLogQuery(t *testing.T) {
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

func TestServiceQueryIntersection(t *testing.T) {
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

func TestServiceCloseResets(t *testing.T) {
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

func TestServiceQueryFilters(t *testing.T) {
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

	// Outside the time window
	for range svc.Query(context.Background(), now.Add(time.Hour), now.Add(2*time.Hour), 1) {
		t.Fatal("expected no results outside time range")
	}

	// Stop iteration early
	count := 0
	svc.Log("second", 1)
	for range svc.Query(context.Background(), from, to, 1) {
		count++
		break
	}
	assert.Equal(t, 1, count)
}
