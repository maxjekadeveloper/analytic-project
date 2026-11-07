package repository

import (
	"analytics-service/analytics"
	"analytics-service/statistics"
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type StatisticsRepository struct {
	db *sqlx.DB
}

func NewStatisticsRepository(db *sqlx.DB) *StatisticsRepository {
	return &StatisticsRepository{db: db}
}

func (r *StatisticsRepository) Save(results []analytics.Result) error {
	tx, err := r.db.Beginx()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	for _, result := range results {
		_, err := tx.Exec(`
			INSERT INTO event_statistics (
				window_start,
				window_end,
				event_type,
				count
			)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (window_start, window_end, event_type)
			DO NOTHING
		`,
			result.WindowStart,
			result.WindowEnd,
			result.EventType,
			result.Count,
		)

		if err != nil {
			return fmt.Errorf("save event statistic: %w", err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (r *StatisticsRepository) GetStats(ctx context.Context) ([]statistics.Statistics, error) {
	var stats []statistics.Statistics
	//var stats statistics.Statistics

	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return stats, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	query := `SELECT event_type, SUM(count) AS count 
						FROM event_statistics 
						GROUP BY event_type`

	err = tx.SelectContext(ctx, &stats, query)
	if err != nil {
		return stats, fmt.Errorf("select event statistics: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return stats, fmt.Errorf("commit transaction: %w", err)
	}

	return stats, nil
}

//SELECT event_type, SUM(count) FROM event_statistics GROUP BY event_type
