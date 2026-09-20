package repository

import (
	"fmt"

	"github.com/jmoiron/sqlx"
)

type ProcessedEventRepository struct {
	db *sqlx.DB
}

func NewProcessedEventRepository(db *sqlx.DB) *ProcessedEventRepository {
	return &ProcessedEventRepository{db: db}
}

func (r *ProcessedEventRepository) MarkProcessed(tx *sqlx.Tx, eventID string) (bool, error) {
	result, err := tx.Exec(`
		INSERT INTO processed_events (event_id)
		VALUES ($1)
		ON CONFLICT (event_id)
		DO NOTHING
		`, eventID)

	if err != nil {
		return false, fmt.Errorf("mark event as processed: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("get rows affected: %w", err)
	}

	return rows == 1, err
}
