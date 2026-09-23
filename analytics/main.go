package main

import (
	"analytics-service/analytics"
	"analytics-service/migrations"
	"analytics-service/repository"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const databaseURL = "postgres://analytics:analytics@localhost:5432/analytics?sslmode=disable"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := repository.NewPostgresConnection()
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
	eventRepo := repository.NewEventRepository(db)
	processor := analytics.NewProcessor(db, eventRepo, service)

	fmt.Println("Analytics service started")

	eventsChan := make(chan analytics.Event)
	go ReadFromKafka(ctx, eventsChan)

	running := true
	for running || eventsChan != nil {
		select {
		case <-ctx.Done():
			running = false
		case event, ok := <-eventsChan:
			if !ok {
				eventsChan = nil
				continue
			}
			err := processor.Process(event)
			if err != nil {
				fmt.Println("Failed to process event:", err)
				continue
			}
		case <-ticker.C:
			windowEnd := time.Now()
			results := service.Snapshot()

			err := repo.Save(results)
			if err != nil {
				fmt.Println("Failed to save statistics:", err)
				continue
			}

			service.Reset(windowEnd)

			fmt.Println("Statistics saved:", len(results))
		}
	}

	//fmt.Println("Shutting down analytics service...")
	fmt.Println("Analytics service stopped")
}

func ReadFromKafka(ctx context.Context, eventsChan chan analytics.Event) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		Topic:       "user-events",
		GroupID:     "analytics-service",
		StartOffset: kafka.FirstOffset,
	})
	defer fmt.Println("Kafka reading is shutdown")
	defer reader.Close()
	defer close(eventsChan)

	for {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}

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
