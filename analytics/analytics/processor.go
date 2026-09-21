package analytics

import (
	"fmt"

	"github.com/jmoiron/sqlx"
)

type EventRepository interface {
	Save(tx *sqlx.Tx, event Event) (bool, error)
}

type Processor struct {
	db         *sqlx.DB
	repository EventRepository
	service    *Service
}

func NewProcessor(db *sqlx.DB, repository EventRepository, service *Service) *Processor {
	return &Processor{db: db, repository: repository, service: service}
}

func (p *Processor) Process(event Event) error {
	tx, err := p.db.Beginx()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	created, err := p.repository.Save(tx, event)
	if !created {
		return fmt.Errorf("saving to event table fault: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	p.service.Process(event)

	return nil
}
