package handler

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"primary_server/model"
	"primary_server/service"

	"github.com/google/uuid"
)

type EventsHandler struct {
	service *service.EventService
}

func NewEventHandler(service *service.EventService) *EventsHandler {
	return &EventsHandler{
		service: service,
	}
}

func (h *EventsHandler) PublishToMQ(w http.ResponseWriter, r *http.Request) {
	if err := validateHTTPHeader(w, r); err != nil {
		log.Println("Header error:", err)
		return
	}

	requestID := uuid.NewString()
	log.Printf("[%s] Request received\n", requestID)
	log.Printf("[%s] Method: %s\n", requestID, r.Method)
	log.Printf("[%s] Path: %s\n", requestID, r.URL.Path)

	event, err := validateHTTPBody(w, r)
	if err != nil {
		log.Printf("[%s] Body error: %s\n", requestID, err)
		return
	}

	err = h.service.PushToBroker(event)
	if err != nil {
		log.Printf("[%s] Failed to publish Kafka: %s\n", requestID, err)
		http.Error(w, "Failed to publish Kafka", http.StatusInternalServerError)
		return
	}

	log.Printf("[%s] Event: %+v\n", requestID, event)
	sendStatusOK(w)
}

func validateHTTPHeader(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		log.Println("Method:", r.Method)
		log.Println("Path:", r.URL.Path)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return fmt.Errorf("Method is not allowed")
	}

	contentType := r.Header.Get("Content-Type")
	if contentType != "application/json" {
		http.Error(w, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
		return fmt.Errorf("Content-Type is not supported")
	}

	return nil
}

func validateHTTPBody(w http.ResponseWriter, r *http.Request) (model.Event, error) {
	var event model.Event
	err := json.NewDecoder(r.Body).Decode(&event)

	if err != nil {
		log.Println("JSON decode error:", err)
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
	}

	if err = event.Validate(); err != nil {
		log.Println("Validation error:", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	return event, err
}

func sendStatusOK(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)

	response := map[string]string{"status": "accepted"}
	err := json.NewEncoder(w).Encode(response)
	if err != nil {
		log.Printf("failed to answer frontend: %v", err)
	}
}
