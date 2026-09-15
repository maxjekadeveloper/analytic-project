package main

import (
	"analytic_project/handler"
	"analytic_project/kafka"
	"analytic_project/service"
	"fmt"
	"net/http"
)

func main() {
	producer := kafka.NewProducer("localhost:9092", "user-events")
	eventService := service.NewEventService(producer)
	eventsHandler := handler.NewEventHandler(eventService)

	http.Handle("/events", eventsHandler)
	http.HandleFunc("/", handler.FrontendHandler)
	fmt.Println("Server started on :3000")
	err := http.ListenAndServe(":3000", nil)

	if err != nil {
		fmt.Println("Server error", err)
	}

}
