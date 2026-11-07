package main

import (
	"analytic_project/cache"
	"analytic_project/handler"
	"analytic_project/kafka"
	"analytic_project/service"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctxSIGTERM, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	broker := os.Getenv("KAFKA_BROKER")
	producer := kafka.NewProducer(broker, "user-events")
	eventService := service.NewEventService(producer)
	eventsHandler := handler.NewEventHandler(eventService)

	mux := http.NewServeMux()
	mux.Handle("/events", eventsHandler)
	mux.HandleFunc("/", handler.FrontendHandler)
	server := &http.Server{Addr: ":3000", Handler: mux}

	go func() {
		fmt.Println("Server started on :3000")
		err := server.ListenAndServe()

		if err != nil && err != http.ErrServerClosed {
			fmt.Println("Server error", err)
		}

		fmt.Println("Server stopped")
	}()

	rdb := cache.NewRedisCache("redis:6379", 0)
	defer rdb.Close()

	ctxTimeout, cancel := context.WithTimeout(ctxSIGTERM, 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctxTimeout); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to Redis!")

	err := rdb.Set(ctxSIGTERM, "analytics:clicks", 100, 30*time.Second)
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

	<-ctxSIGTERM.Done()

	fmt.Println("Shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		fmt.Println("HTTP shutdown server error", err)
	}
}
