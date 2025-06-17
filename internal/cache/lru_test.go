// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLRUEviction(t *testing.T) {
	l := NewLRU[string, int](2)

	l.Add("a", 1)
	time.Sleep(time.Microsecond)
	l.Add("b", 2)
	time.Sleep(time.Microsecond)
	l.Get("a")    // mark a as recently used
	l.Add("a", 2) // should evict "b"
	time.Sleep(time.Microsecond)
	l.Add("c", 3) // should evict "b"

	_, okA := l.Get("a")
	_, okB := l.Get("b")
	_, okC := l.Get("c")

	assert.True(t, okA, "entry a should remain")
	assert.False(t, okB, "entry b should be evicted")
	assert.True(t, okC, "entry c should remain")
}

func TestLRUCapacityOne(t *testing.T) {
	l := NewLRU[string, int](1)
	l.Add("x", 10)
	l.Add("y", 20)
	_, okX := l.Get("x")
	_, okY := l.Get("y")
	assert.False(t, okX)
	assert.True(t, okY)
}
