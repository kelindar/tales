package threads

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSequenceGenerator(t *testing.T) {
	t.Run("NewSequenceGenerator", func(t *testing.T) {
		now := time.Date(2024, 1, 15, 14, 30, 0, 0, time.UTC)
		sg := NewSequenceGenerator(now)

		expectedDayStart := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		assert.Equal(t, expectedDayStart, sg.DayStart())
	})

	t.Run("Generate", func(t *testing.T) {
		dayStart := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		sg := NewSequenceGenerator(dayStart)

		// Generate at 14:30
		now := dayStart.Add(14*time.Hour + 30*time.Minute)
		id := sg.Generate(now)

		// Should be able to reconstruct the timestamp
		reconstructed := ReconstructTimestamp(id, sg.DayStart())
		expected := dayStart.Add(14*time.Hour + 30*time.Minute)
		assert.Equal(t, expected, reconstructed)
	})

	t.Run("SequentialCounters", func(t *testing.T) {
		sg := NewSequenceGenerator(time.Now())
		now := time.Now()

		id1 := sg.Generate(now)
		id2 := sg.Generate(now)
		id3 := sg.Generate(now)

		// IDs should be sequential
		assert.Greater(t, id2, id1)
		assert.Greater(t, id3, id2)
	})

	t.Run("DifferentMinutes", func(t *testing.T) {
		dayStart := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		sg := NewSequenceGenerator(dayStart)

		time1 := dayStart.Add(10 * time.Minute)
		time2 := dayStart.Add(20 * time.Minute)

		id1 := sg.Generate(time1)
		id2 := sg.Generate(time2)

		// IDs from different minutes should be different
		assert.NotEqual(t, id1, id2)
	})

	t.Run("AutoReset", func(t *testing.T) {
		sg := NewSequenceGenerator(time.Now())

		// Generate ID for next day (should auto-reset)
		tomorrow := time.Now().Add(24 * time.Hour)
		id := sg.Generate(tomorrow)

		// Day start should be updated
		assert.Equal(t, getDayStart(tomorrow), sg.DayStart())
		assert.NotZero(t, id)
	})

	t.Run("ReconstructTimestamp", func(t *testing.T) {
		dayStart := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		sg := NewSequenceGenerator(dayStart)

		// Generate ID at 14:30
		originalTime := dayStart.Add(14*time.Hour + 30*time.Minute)
		id := sg.Generate(originalTime)

		// Reconstruct timestamp
		reconstructed := ReconstructTimestamp(id, sg.DayStart())

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

		reconstructed := ReconstructTimestamp(sequenceID, dayStart)
		expected := dayStart.Add(14*time.Hour + 30*time.Minute)

		assert.Equal(t, expected, reconstructed)
	})

	t.Run("GetDayStart", func(t *testing.T) {
		// Test with various times
		testTime := time.Date(2024, 1, 15, 14, 30, 45, 123456789, time.UTC)
		expected := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)

		assert.Equal(t, expected, getDayStart(testTime))
	})

	t.Run("GetDateString", func(t *testing.T) {
		testTime := time.Date(2024, 1, 15, 14, 30, 45, 0, time.UTC)
		expected := "2024-01-15"

		assert.Equal(t, expected, getDateString(testTime))
	})
}
