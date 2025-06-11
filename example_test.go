package threads

import (
	"fmt"
	"log"
	"time"

	"github.com/kelindar/threads/internal/s3"
)

// Example demonstrates basic usage of the threads library
func Example() {
	// Configure the logger
	config := Config{
		S3Config: s3.Config{
			Bucket: "my-game-logs",
			Region: "us-east-1",
			Prefix: "game-events",
		},
		ChunkInterval: 5 * time.Minute,
		BufferSize:    1000,
	}

	// Create logger
	logger, err := New(config)
	if err != nil {
		log.Fatal(err)
	}
	defer logger.Close()

	// Log some game events
	logger.Log("Player joined the game", []uint32{12345})
	logger.Log("Player moved to position (100, 200)", []uint32{12345})
	logger.Log("Player attacked monster", []uint32{12345, 67890}) // Player and monster
	logger.Log("Monster died", []uint32{67890})
	logger.Log("Player gained 100 XP", []uint32{12345})

	// Query events for a specific player
	from := time.Now().Add(-1 * time.Hour)
	to := time.Now().Add(1 * time.Hour)

	fmt.Println("Events for player 12345:")
	var count int
	for _, text := range logger.Query(12345, from, to) {
		fmt.Printf("- %s\n", text)
		count++
	}
	fmt.Printf("Total events: %d\n", count)

	// Output:
	// Events for player 12345:
	// - Player joined the game
	// - Player moved to position (100, 200)
	// - Player attacked monster
	// - Player gained 100 XP
	// Total events: 4
}

// ExampleQuery demonstrates querying historical data
func ExampleLogger_Query() {
	// Assume logger is already created and has historical data
	logger := &Logger{} // This would be a real logger instance

	// Query events for the last 24 hours
	from := time.Now().Add(-24 * time.Hour)
	to := time.Now()

	var eventCount int
	for timestamp, text := range logger.Query(12345, from, to) {
		fmt.Printf("%s: %s\n", timestamp.Format("2006-01-02 15:04"), text)
		eventCount++
		if eventCount >= 3 { // Limit output for example
			break
		}
	}

	// Output would show historical events like:
	// 2025-06-11 21:30: Player joined the game
	// 2025-06-11 21:30: Player moved to position (100, 200)
	// 2025-06-11 21:30: Player attacked monster
}

// ExampleConfig demonstrates different configuration options
func ExampleConfig() {
	// Basic configuration
	basicConfig := Config{
		S3Config: s3.Config{
			Bucket: "my-logs",
			Region: "us-west-2",
		},
	}

	// Advanced configuration
	advancedConfig := Config{
		S3Config: s3.Config{
			Bucket:        "my-logs",
			Region:        "us-west-2",
			Prefix:        "production/game-events",
			MaxConcurrent: 20,
			RetryAttempts: 5,
		},
		ChunkInterval: 10 * time.Minute, // Flush every 10 minutes
		BufferSize:    2000,             // Larger buffer
	}

	fmt.Printf("Basic config bucket: %s\n", basicConfig.S3Config.Bucket)
	fmt.Printf("Advanced config prefix: %s\n", advancedConfig.S3Config.Prefix)

	// Output:
	// Basic config bucket: my-logs
	// Advanced config prefix: production/game-events
}
