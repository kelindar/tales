package mock

import (
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
	for _, text := range svc.Query(from, to, 1) {
		results = append(results, text)
	}
	assert.Equal(t, []string{"third"}, results)

	results = results[:0]
	for _, text := range svc.Query(from, to, 2) {
		results = append(results, text)
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
	for _, text := range svc.Query(from, to, 1, 2) {
		res = append(res, text)
	}
	assert.Equal(t, []string{"b"}, res)
}

func TestServiceCloseResets(t *testing.T) {
	svc := NewService(2)
	svc.Log("pending", 1)

	err := svc.Close()
	assert.NoError(t, err)
	assert.Equal(t, 0, svc.capacity)
	assert.Nil(t, svc.buf)
	assert.Zero(t, svc.size)
	assert.Zero(t, svc.next)
}
