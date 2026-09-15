package handler

import (
	"analytic_project/model"
	"analytic_project/service"
	"encoding/json"
	"fmt"
	"net/http"
)

type EventsHandler struct {
	service *service.EventService
}

func NewEventHandler(service *service.EventService) *EventsHandler {
	return &EventsHandler{
		service: service,
	}
}

func (h *EventsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := validateHTTPHeader(w, r); err != nil {
		fmt.Println("Header error:", err)
		return
	}

	fmt.Println("Request received")
	fmt.Println("Method:", r.Method)
	fmt.Println("Path:", r.URL.Path)

	event, err := validateHTTPBody(w, r)
	if err != nil {
		fmt.Println("Body error:", err)
		return
	}

	err = h.service.Process(event)
	if err != nil {
		http.Error(w, "Failed to process event", http.StatusInternalServerError)
		return
	}

	fmt.Printf("Event: %+v\n", event)
	fmt.Fprintln(w, "Event received")

	sendStatusOK(w)
}

func validateHTTPHeader(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		fmt.Println("Method:", r.Method)
		fmt.Println("Path:", r.URL.Path)
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
		fmt.Println("JSON decode error:", err)
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
	}

	if err = event.Validate(); err != nil {
		fmt.Println("Validation error:", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	return event, err
}

func sendStatusOK(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)

	response := map[string]string{"status": "accepted"}
	json.NewEncoder(w).Encode(response)
}
