package main

import (
	"analytics-service/analytics"
	"analytics-service/migrations"
	"analytics-service/repository"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

const databaseURL = "postgres://analytics:analytics@localhost:5432/analytics?sslmode=disable"

func main() {
	db, err := repository.NewPostgres()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	fmt.Println("Connected to PostgreSQL")
	repo := repository.NewPostgresRepository(db)

	err = migrations.Run(databaseURL)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Migrations completed")

	const secs = 3
	ticker := time.NewTicker(secs * time.Second)
	defer ticker.Stop()

	service := analytics.NewService()

	fmt.Println("Analytics service started")

	eventsChan := make(chan analytics.Event)
	go ReadFromKafka(eventsChan)

	for {
		select {
		case event := <-eventsChan:
			service.Process(event)
		case <-ticker.C:
			results := service.Flush(secs)
			err := repo.Save(results)
			if err != nil {
				fmt.Println("Failed to save statistics:", err)
				continue
			}

			fmt.Println("Statistics saved:", len(results))
		}
	}
}

func ReadFromKafka(eventsChan chan analytics.Event) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		Topic:       "user-events",
		GroupID:     "analytics-service",
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()
	defer close(eventsChan)

	for {
		message, err := reader.ReadMessage(context.Background())
		if err != nil {
			fmt.Println("Kafka error", err)
			return
		}

		var event analytics.Event
		err = json.Unmarshal(message.Value, &event)
		if err != nil {
			fmt.Println("Invalid event:", err)
			continue
		}

		eventsChan <- event
	}
}
