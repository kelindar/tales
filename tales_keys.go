package tales

import "fmt"

// buildDailyKeys builds S3 object keys for the provided date (YYYY-MM-DD).
func buildDailyKeys(date string) (threadsLog, threadsIdx, actorsLog, actorsIdx string) {
	threadsLog = fmt.Sprintf("%s/threads.log", date)
	threadsIdx = fmt.Sprintf("%s/threads.idx", date)
	actorsLog = fmt.Sprintf("%s/actors.log", date)
	actorsIdx = fmt.Sprintf("%s/actors.idx", date)
	return
}

// buildChunkKey builds an S3 key for a specific chunk file inside the date folder.
func buildChunkKey(date string, chunk uint64) string {
	return fmt.Sprintf("%s/%d.log", date, chunk)
}
