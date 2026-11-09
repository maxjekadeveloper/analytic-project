package handler

import (
	"log"
	"net/http"
	"primary_server/client"
)

type StatisticsHandler struct {
	analyticsClient *client.AnalyticsClient
}

func NewStatisticsHandler(analyticsClient *client.AnalyticsClient) *StatisticsHandler {
	return &StatisticsHandler{analyticsClient: analyticsClient}
}

func (s *StatisticsHandler) GetStatistics(w http.ResponseWriter, r *http.Request) {
	data, err := s.analyticsClient.GetStatistics(r.Context())
	if err != nil {
		log.Printf("error of getting statistics: %v\n", err)
		http.Error(w, "failed to get statistics", http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(data)
	if err != nil {
		log.Printf("write statistics response: %v\n", err)
	}
}
