package main

import (
	"analytic_project/handler"
	"analytic_project/kafka"
	"analytic_project/service"
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
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
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	<-ctx.Done()

	fmt.Println("Shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		fmt.Println("HTTP shutdown server error", err)
	}

	fmt.Println("Server stopped")
}
