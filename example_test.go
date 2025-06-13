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
	logger.Log("Player joined the game", 12345)
	logger.Log("Player moved to position (100, 200)", 12345)
	logger.Log("Player attacked monster", 12345, 67890) // Player and monster
	logger.Log("Monster died", 67890)
	logger.Log("Player gained 100 XP", 12345)

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
