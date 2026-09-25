package repository

import (
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

func NewPostgresConnection(databaseURL string) (*sqlx.DB, error) {
	db, err := sqlx.Connect("pgx", databaseURL)

	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}

	return db, nil
}
