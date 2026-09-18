package repository

import (
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

func NewPostgres() (*sqlx.DB, error) {
	db, err := sqlx.Connect("pgx", "postgres://analytics:analytics@localhost:5432/analytics")

	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}

	return db, nil
}
