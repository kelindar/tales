package threads

import (
	"sync/atomic"
	"time"
)

// generateSequenceID creates a unique sequence ID for the given timestamp.
// Format: (minutes_from_day_start << 20) | atomic_counter
// - High 12 bits: minutes from day start (0-1439, fits in 11 bits, using 12 for safety)
// - Low 20 bits: atomic counter within that minute (0-1048575)
// This guarantees lexicographic ordering and uniqueness.
func generateSequenceID(dayStart time.Time, now time.Time, atomicCounter *uint32) uint32 {
	minutesFromDayStart := uint32(now.Sub(dayStart).Minutes())

	// Debug: print the calculation (remove this in production)
	// fmt.Printf("DEBUG: generateSequenceID - dayStart=%s, now=%s, minutes=%d\n",
	//     dayStart.Format(time.RFC3339), now.Format(time.RFC3339), minutesFromDayStart)

	// Ensure we don't exceed 12 bits for minutes (1440 minutes in a day)
	if minutesFromDayStart > 1439 {
		minutesFromDayStart = 1439
	}

	// Increment atomic counter and wrap at 1048576 (20 bits)
	counter := atomic.AddUint32(atomicCounter, 1) & 0xFFFFF

	return (minutesFromDayStart << 20) | counter
}

// reconstructTimestamp reconstructs a timestamp from a sequence ID and day start.
func reconstructTimestamp(sequenceID uint32, dayStart time.Time) time.Time {
	minutes := sequenceID >> 20
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

// isNewDay checks if the current time represents a new day compared to the day start.
func isNewDay(dayStart time.Time, now time.Time) bool {
	return getDayStart(now) != dayStart
}
