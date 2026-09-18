package repository

import (
	"analytics-service/analytics"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type PostresRepository struct {
	db *sqlx.DB
}

func NewPostgresRepository(db *sqlx.DB) *PostresRepository {
	return &PostresRepository{db: db}
}

func (r *PostresRepository) Save(results []analytics.Result) error {
	tx, err := r.db.Beginx()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	for _, result := range results {
		_, err := tx.Exec(`
			INSERT INTO event_statistics (
				window_start,
				window_end,
				event_type,
				count
			)
			VALUES ($1, $2, $3, $4)
		`,
			result.WindowStart,
			result.WindowEnd,
			result.EventType,
			result.Count,
		)

		if err != nil {
			tx.Rollback()
			return fmt.Errorf("save event statistic: %w", err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}
