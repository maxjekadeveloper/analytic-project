package statistics_handler

import (
	"analytics-service/cache"
	"context"
	"encoding/json"
	"log"
	"net/http"
)

type StatisticsHandler struct {
	ctx          context.Context
	cacheService *cache.CacheService
}

func NewStatisticsHandler(ctx context.Context, cacheService *cache.CacheService) *StatisticsHandler {
	return &StatisticsHandler{ctx: ctx, cacheService: cacheService}
}

func (s *StatisticsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}

	stats, err := s.cacheService.GetStats(s.ctx)
	if err != nil {
		http.Error(w, "error getting statistics", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	err = json.NewEncoder(w).Encode(stats)
	if err != nil {
		log.Printf("failed to encode statistics: %v\n", err)
	}
}
