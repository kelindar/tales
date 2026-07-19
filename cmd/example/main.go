package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/kelindar/tales"
)

var actors = []struct {
	id   uint32
	name string
}{
	{1001, "Sir Galahad"},
	{1002, "Lady Guinevere"},
	{1003, "Merlin the Wise"},
	{1004, "Sir Lancelot"},
	{1005, "Dame Morgana"},
}

func loadTrainingTexts() []string {
	file, err := os.Open("train.txt")
	if err != nil {
		log.Fatal("Failed to open train.txt:", err)
	}
	defer file.Close()

	var texts []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if line := scanner.Text(); line != "" {
			texts = append(texts, line)
		}
	}
	return texts
}

func simulateConversation(logger *tales.Service, texts []string) {
	speaker := actors[rand.Intn(len(actors))]
	numParticipants := 1 + rand.Intn(3)
	participants := []uint32{speaker.id}

	// Add random other participants
	for i := 0; i < numParticipants && len(participants) < len(actors); i++ {
		for {
			actor := actors[rand.Intn(len(actors))]
			found := false
			for _, p := range participants {
				if p == actor.id {
					found = true
					break
				}
			}
			if !found {
				participants = append(participants, actor.id)
				break
			}
		}
	}

	// Pick random text and log it
	message := fmt.Sprintf("%s: %s", speaker.name, texts[rand.Intn(len(texts))])
	if err := logger.Log(message, participants...); err != nil {
		log.Printf("Error: %v", err)
	} else {
		log.Printf("Logged: %s", message)
	}
}

func runSimulation(logger *tales.Service, duration time.Duration) {
	log.Printf("Starting simulation for %v...", duration)
	texts := loadTrainingTexts()

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	// Generate conversations every 8-30 seconds
	ticker := time.NewTicker(time.Duration(1+rand.Intn(2)) * time.Second)
	defer ticker.Stop()

	count := 0
	for {
		select {
		case <-ctx.Done():
			log.Printf("Simulation complete. Total conversations: %d", count)
			return
		case <-ticker.C:
			simulateConversation(logger, texts)
			count++
			ticker.Reset(time.Duration(1+rand.Intn(2)) * time.Second)
		}
	}
}

func demoQueries(logger *tales.Service) {
	log.Println("\n=== Query Demo ===")

	now := time.Now()
	from := now.Add(-12 * time.Hour)

	// Show conversations for first actor
	actor := actors[0]
	log.Printf("\nConversations for %s:", actor.name)
	count := 0
	for event, err := range logger.Query(context.Background(), from, now, actor.id) {
		if err != nil {
			log.Printf("  Query failed: %v", err)
			return
		}
		log.Printf("  [%s] %s", event.Time().Format("15:04:05"), event.Text())
		count++
	}
	if count == 0 {
		log.Printf("  No conversations found")
	}
}

func main() {
	log.Println("=== Tales Medieval Chat Simulation ===")
	logger, err := tales.New("storyline", "eu-central-003", tales.WithBackblaze())
	if err != nil {
		log.Fatal("Failed to create logger:", err)
	}

	// Run simulation
	//runSimulation(logger, 60*time.Minute)
	if err := logger.Sync(context.Background()); err != nil {
		log.Fatal("Failed to sync logger:", err)
	}
	demoQueries(logger)
	if err := logger.Close(); err != nil {
		log.Fatal("Failed to close logger:", err)
	}
	log.Println("\n=== Complete ===")
}
