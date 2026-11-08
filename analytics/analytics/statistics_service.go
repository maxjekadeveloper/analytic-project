package analytics

import (
	"time"
)

type Result struct {
	WindowStart time.Time
	WindowEnd   time.Time
	EventType   string
	Count       int
}

type Event struct {
	EventID   string    `json:"event_id"`
	EventType string    `json:"event_type"`
	ElementID string    `json:"element_id"`
	Value     any       `json:"value"`
	Timestamp time.Time `json:"timestamp"`
}

type StatisticsService struct {
	counts      map[string]int
	windowStart time.Time
}

func NewService() *StatisticsService {
	return &StatisticsService{counts: make(map[string]int), windowStart: time.Now()}
}

func (s *StatisticsService) Process(event Event) {
	s.counts[event.EventType]++
}

func (s *StatisticsService) Snapshot() []Result {
	windowEnd := time.Now()
	results := make([]Result, 0, len(s.counts))

	for eventType, count := range s.counts {
		results = append(results, Result{
			WindowStart: s.windowStart,
			WindowEnd:   windowEnd,
			EventType:   eventType,
			Count:       count,
		})
	}

	return results
}

func (s *StatisticsService) Reset(windowEnd time.Time) {
	s.counts = make(map[string]int)
	s.windowStart = windowEnd
}
