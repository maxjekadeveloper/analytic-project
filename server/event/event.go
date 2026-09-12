package event

import (
	"fmt"
	"time"
)

type Event struct {
	EventType string    `json:"event_type"`
	ElementID string    `json:"element_id"`
	Value     any       `json:"value"`
	Timestamp time.Time `json:"timestamp"`
}

func (e Event) Validate() error {
	err := error(nil)

	if e.EventType == "" {
		err = fmt.Errorf("event_type is required")
	}

	if e.ElementID == "" {
		err = fmt.Errorf("element_id is required")
	}

	if e.Timestamp.IsZero() {
		err = fmt.Errorf("timestamp is required")
	}

	return err
}
