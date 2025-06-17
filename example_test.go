// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root\

package tales

import (
	"fmt"
	"log"
	"time"

	s3mock "github.com/kelindar/s3/mock"
	"github.com/kelindar/tales/internal/s3"
)

// Example demonstrates basic usage of the tales library
func Example() {
	// Use a mock S3 server for the example
	mockServer := s3mock.New("example-bucket", "us-east-1")
	defer mockServer.Close()

	// Create logger
	logger, err := New(
		"example-bucket",
		"us-east-1",
		WithPrefix("events"),
		WithInterval(5*time.Minute),
		WithBuffer(1000),
		WithClient(func(cfg s3.Config) (s3.Client, error) {
			return s3.NewMockClient(mockServer, cfg)
		}),
	)
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
	for _, text := range logger.Query(from, to, 12345) {
		fmt.Printf("- %s\n", text)
		count++
	}
	fmt.Printf("Total events: %d\n", count)

	// Query events involving both player and monster
	fmt.Println("\nEvents involving both player 12345 and monster 67890:")
	count = 0
	for _, text := range logger.Query(from, to, 12345, 67890) {
		fmt.Printf("- %s\n", text)
		count++
	}
	fmt.Printf("Total intersection events: %d\n", count)

	// Output:
	// Events for player 12345:
	// - Player joined the game
	// - Player moved to position (100, 200)
	// - Player attacked monster
	// - Player gained 100 XP
	// Total events: 4
	//
	// Events involving both player 12345 and monster 67890:
	// - Player attacked monster
	// Total intersection events: 1
}
