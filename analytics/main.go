package main

import (
	"analytics-service/analytics"
	"analytics-service/cache"
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

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func main() {
	ctxSIGTERM, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	databaseURL := os.Getenv("DATABASE_URL")
	db, err := repository.NewPostgresConnection(databaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	fmt.Println("Connected to PostgreSQL")
	repo := repository.NewStatisticsRepository(db)

	err = migrations.Run(databaseURL)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Migrations completed")

	rdb := cache.NewRedisCache("redis:6379", 0)
	defer rdb.Close()

	ctxTimeout, cancel := context.WithTimeout(ctxSIGTERM, 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctxTimeout); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to Redis!")

	err = rdb.Set(ctxSIGTERM, "analytics:clicks", 100, 30*time.Second)
	if err != nil {
		log.Fatal(err)
	}

	value, err := rdb.Get(ctxSIGTERM, "analytics:clicks")
	if err == redis.Nil {
		fmt.Println("No such key...")
	} else if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Clicks:", value)

	const secs = 30
	ticker := time.NewTicker(secs * time.Second)
	defer ticker.Stop()

	service := analytics.NewService()
	eventRepo := repository.NewEventRepository(db)
	processor := analytics.NewProcessor(db, eventRepo, service)

	fmt.Println("Analytics service started")

	broker := os.Getenv("KAFKA_BROKER")
	fmt.Println("broker =", broker)
	eventsChan := make(chan analytics.Event)
	go ReadFromKafka(ctxSIGTERM, broker, eventsChan)

	running := true
	for running || eventsChan != nil {
		select {
		case <-ctxSIGTERM.Done():
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

			size := len(results)
			if size > 0 {
				fmt.Println("Statistics saved:", len(results))
			}
		}
	}

	fmt.Println("Analytics service stopped")
}

func ReadFromKafka(ctxSIGTERM context.Context, broker string, eventsChan chan analytics.Event) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       "user-events",
		GroupID:     "analytics-service",
		StartOffset: kafka.FirstOffset,
	})
	defer fmt.Println("Kafka reading is shutdown")
	defer reader.Close()
	defer close(eventsChan)

	var reTry byte = 20
	for {
		message, err := reader.ReadMessage(ctxSIGTERM)
		if err != nil {
			if ctxSIGTERM.Err() != nil {
				return
			}

			if reTry == 0 {
				return
			}

			reTry--
			log.Println("Kafka error", err)
			log.Println("Retrying in 1 second...")

			timer := time.NewTimer(time.Second)
			select {
			case <-ctxSIGTERM.Done():
				timer.Stop()
				return
			case <-timer.C:
				continue
			}
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
