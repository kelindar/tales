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

// sequence generates unique sequence IDs for a specific day.
// Sequence ID Format: (minutes_from_day_start << 20) | atomic_counter
type sequence struct {
	day time.Time     // Start of the current day (UTC)
	cnt atomic.Uint32 // Atomic counter for uniqueness
}

// newSequence creates a new sequence generator for the given day.
func newSequence(dayStart time.Time) *sequence {
	return &sequence{
		day: dayOf(dayStart),
	}
}

// Next creates a unique sequence ID for the given timestamp.
func (sg *sequence) Next(now time.Time) uint32 {
	// Check if we need to reset for a new day
	if dayOf(now) != sg.day {
		sg.day = dayOf(now)
		sg.cnt.Store(0)
	}

	minutesFromDayStart := uint32(now.Sub(sg.day).Minutes())
	if minutesFromDayStart > maxMinutes {
		minutesFromDayStart = maxMinutes
	}

	counter := sg.cnt.Add(1) & counterMask
	return (minutesFromDayStart << counterBits) | counter
}

// Day returns the current day start time.
func (sg *sequence) Day() time.Time {
	return sg.day
}

// timeOf reconstructs a timestamp from a sequence ID and day start.
func timeOf(sequenceID uint32, dayStart time.Time) time.Time {
	return dayStart.Add(time.Duration(sequenceID>>counterBits) * time.Minute)
}

// dayOf returns the start of the day (00:00:00) for the given time in UTC.
func dayOf(t time.Time) time.Time {
	year, month, day := t.UTC().Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

// formatDate returns the date string in YYYY-MM-DD format for S3 key prefixes.
func formatDate(t time.Time) string {
	return t.UTC().Format("2006-01-02")
}
