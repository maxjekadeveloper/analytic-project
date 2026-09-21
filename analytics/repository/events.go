package repository

import (
	"analytics-service/analytics"
	"encoding/json"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type EventRepository struct {
	db *sqlx.DB
}

func NewEventRepository(db *sqlx.DB) *EventRepository {
	return &EventRepository{db: db}
}

func (r *EventRepository) Save(tx *sqlx.Tx, event analytics.Event) (bool, error) {
	value, err := json.Marshal(event.Value)
	if err != nil {
		return false, fmt.Errorf("marshal event value: %w", err)
	}

	result, err := tx.Exec(`
				INSERT INTO events (event_id, event_type, element_id, value, event_timestamp)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (event_id)
				DO NOTHING
				`, event.EventID, event.EventType, event.ElementID, value, event.Timestamp)

	if err != nil {
		return false, fmt.Errorf("save event: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("get affected rows: %w", err)
	}

	return rows == 1, nil
}
