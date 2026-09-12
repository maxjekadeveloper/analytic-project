package main

import (
	"analytic_project/event"
	"encoding/json"
	"fmt"
	"net/http"
)

func main() {
	http.HandleFunc("/events", eventsHandler)
	http.HandleFunc("/", frontendHandler)
	fmt.Println("Server started on :3000")
	err := http.ListenAndServe(":3000", nil)

	if err != nil {
		fmt.Println("Server error", err)
	}
}

func frontendHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Request received")
	fmt.Println("Method:", r.Method)
	fmt.Println("Path:", r.URL.Path)
	http.FileServer(http.Dir("../frontend")).ServeHTTP(w, r)
	//http.ServeFile(w, r, "../frontend/index.html")
}

func eventsHandler(w http.ResponseWriter, r *http.Request) {
	//responseForOptions(w, r)
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

	fmt.Printf("Event: %+v\n", event)
	fmt.Fprintln(w, "Event received")

	sendStatusOK(w)
}

// func responseForOptions(w http.ResponseWriter, r *http.Request) {
// 	w.Header().Set("Access-Control-Allow-Origin", "*")
// 	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
// 	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

// 	if r.Method == http.MethodOptions {
// 		w.WriteHeader(http.StatusOK)
// 	}
// }

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

func validateHTTPBody(w http.ResponseWriter, r *http.Request) (event.Event, error) {
	var event event.Event
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
