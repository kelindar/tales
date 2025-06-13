package seq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSequenceGenerator(t *testing.T) {
	t.Run("NewSequenceGenerator", func(t *testing.T) {
		now := time.Date(2024, 1, 15, 14, 30, 0, 0, time.UTC)
		sg := NewSequence(now)

		expectedDayStart := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		assert.Equal(t, expectedDayStart, sg.Day())
	})

	t.Run("Generate", func(t *testing.T) {
		dayStart := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		sg := NewSequence(dayStart)

		// Generate at 14:30
		now := dayStart.Add(14*time.Hour + 30*time.Minute)
		id := sg.Next(now)

		// Should be able to reconstruct the timestamp
		reconstructed := TimeOf(id, sg.Day())
		expected := dayStart.Add(14*time.Hour + 30*time.Minute)
		assert.Equal(t, expected, reconstructed)
	})

	t.Run("SequentialCounters", func(t *testing.T) {
		sg := NewSequence(time.Now())
		now := time.Now()

		id1 := sg.Next(now)
		id2 := sg.Next(now)
		id3 := sg.Next(now)

		// IDs should be sequential
		assert.Greater(t, id2, id1)
		assert.Greater(t, id3, id2)
	})

	t.Run("DifferentMinutes", func(t *testing.T) {
		dayStart := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		sg := NewSequence(dayStart)

		time1 := dayStart.Add(10 * time.Minute)
		time2 := dayStart.Add(20 * time.Minute)

		id1 := sg.Next(time1)
		id2 := sg.Next(time2)

		// IDs from different minutes should be different
		assert.NotEqual(t, id1, id2)
	})

	t.Run("AutoReset", func(t *testing.T) {
		sg := NewSequence(time.Now())

		// Generate ID for next day (should auto-reset)
		tomorrow := time.Now().Add(24 * time.Hour)
		id := sg.Next(tomorrow)

		// Day start should be updated
		assert.Equal(t, DayOf(tomorrow), sg.Day())
		assert.NotZero(t, id)
	})

	t.Run("ReconstructTimestamp", func(t *testing.T) {
		dayStart := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		sg := NewSequence(dayStart)

		// Generate ID at 14:30
		originalTime := dayStart.Add(14*time.Hour + 30*time.Minute)
		id := sg.Next(originalTime)

		// Reconstruct timestamp
		reconstructed := TimeOf(id, sg.Day())

		// Should be at 14:30:00 (minute precision)
		expected := dayStart.Add(14*time.Hour + 30*time.Minute)
		assert.Equal(t, expected, reconstructed)
	})
}

func TestUtilityFunctions(t *testing.T) {
	t.Run("ReconstructTimestamp", func(t *testing.T) {
		dayStart := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)

		// Create sequence ID for 14:30 (870 minutes)
		sequenceID := uint32(870 << counterBits)

		reconstructed := TimeOf(sequenceID, dayStart)
		expected := dayStart.Add(14*time.Hour + 30*time.Minute)

		assert.Equal(t, expected, reconstructed)
	})

	t.Run("GetDayStart", func(t *testing.T) {
		// Test with various times
		testTime := time.Date(2024, 1, 15, 14, 30, 45, 123456789, time.UTC)
		expected := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)

		assert.Equal(t, expected, DayOf(testTime))
	})

	t.Run("GetDateString", func(t *testing.T) {
		testTime := time.Date(2024, 1, 15, 14, 30, 45, 0, time.UTC)
		expected := "2024-01-15"

		assert.Equal(t, expected, FormatDate(testTime))
	})
}
