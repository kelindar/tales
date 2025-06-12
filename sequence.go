package threads

import (
	"sync/atomic"
	"time"
)

const (
	// Sequence ID format constants
	minuteBits  = 12                     // High 12 bits for minutes (0-1439)
	counterBits = 20                     // Low 20 bits for counter (0-1048575)
	counterMask = (1 << counterBits) - 1 // 0xFFFFF
	maxMinutes  = 1439                   // Maximum minutes in a day (24*60-1)
	maxCounter  = (1 << counterBits) - 1 // Maximum counter value
)

// SequenceGenerator generates unique sequence IDs for a specific day.
// Sequence ID Format: (minutes_from_day_start << 20) | atomic_counter
type SequenceGenerator struct {
	dayStart time.Time     // Start of the current day (UTC)
	counter  atomic.Uint32 // Atomic counter for uniqueness
}

// NewSequenceGenerator creates a new sequence generator for the given day.
func NewSequenceGenerator(dayStart time.Time) *SequenceGenerator {
	return &SequenceGenerator{
		dayStart: getDayStart(dayStart),
	}
}

// Generate creates a unique sequence ID for the given timestamp.
func (sg *SequenceGenerator) Generate(now time.Time) uint32 {
	// Check if we need to reset for a new day
	if getDayStart(now) != sg.dayStart {
		sg.dayStart = getDayStart(now)
		sg.counter.Store(0)
	}

	minutesFromDayStart := uint32(now.Sub(sg.dayStart).Minutes())
	if minutesFromDayStart > maxMinutes {
		minutesFromDayStart = maxMinutes
	}

	counter := sg.counter.Add(1) & counterMask
	return (minutesFromDayStart << counterBits) | counter
}

// DayStart returns the current day start time.
func (sg *SequenceGenerator) DayStart() time.Time {
	return sg.dayStart
}

// ReconstructTimestamp reconstructs a timestamp from a sequence ID and day start.
func ReconstructTimestamp(sequenceID uint32, dayStart time.Time) time.Time {
	minutes := sequenceID >> counterBits
	return dayStart.Add(time.Duration(minutes) * time.Minute)
}

// getDayStart returns the start of the day (00:00:00) for the given time in UTC.
func getDayStart(t time.Time) time.Time {
	year, month, day := t.UTC().Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

// getDateString returns the date string in YYYY-MM-DD format for S3 key prefixes.
func getDateString(t time.Time) string {
	return t.UTC().Format("2006-01-02")
}
