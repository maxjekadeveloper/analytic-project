package main

import (
	"analytics-service/analytics"
	"analytics-service/cache"
	"analytics-service/migrations"
	"analytics-service/repository"
	"analytics-service/statistics_handler"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	statisticsRepo := repository.NewStatisticsRepository(db)

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

	cacheService := cache.NewCacheService(statisticsRepo, rdb)
	statsHandler := statistics_handler.NewStatisticsHandler(ctxSIGTERM, cacheService)
	mux := http.NewServeMux()
	mux.Handle("/statistics", statsHandler)
	server := &http.Server{Addr: ":3111", Handler: mux}
	go func() {
		fmt.Println("Statistics server started on :3111")
		err := server.ListenAndServe()

		if err != nil && err != http.ErrServerClosed {
			fmt.Println("Server error", err)
		}

		fmt.Println("Server stopped")
	}()

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

			err := statisticsRepo.Save(results)
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
