package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"primary_server/client"
	"primary_server/handler"
	"primary_server/kafka"
	"primary_server/service"
	"syscall"
	"time"
)

func main() {
	ctxSIGTERM, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	broker := os.Getenv("KAFKA_BROKER")
	producer := kafka.NewProducer(broker, "user-events")
	eventService := service.NewEventService(producer)
	eventsHandler := handler.NewEventHandler(eventService)

	analyticsClient := client.NewAnalyticsClient("http://analytics-server:3111")
	statisticsHandler := handler.NewStatisticsHandler(analyticsClient)

	mux := http.NewServeMux()
	mux.HandleFunc("/", handler.FrontendHandler)
	mux.HandleFunc("/events", eventsHandler.PublishToMQ)
	mux.HandleFunc("/statistics", statisticsHandler.GetStatistics)
	server := &http.Server{Addr: ":3000", Handler: mux}

	go func() {
		fmt.Println("Server started on :3000")
		err := server.ListenAndServe()

		if err != nil && err != http.ErrServerClosed {
			fmt.Println("Server error", err)
		}

		fmt.Println("Server stopped")
	}()

	<-ctxSIGTERM.Done()

	fmt.Println("Shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		fmt.Println("HTTP shutdown server error", err)
	}
}
