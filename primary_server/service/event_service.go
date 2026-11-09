package service

import "primary_server/model"

type EventProducer interface {
	Publish(event model.Event) error
}

type EventService struct {
	producer EventProducer
}

func NewEventService(producer EventProducer) *EventService {
	return &EventService{producer: producer}
}

func (s *EventService) PushToBroker(event model.Event) error {
	return s.producer.Publish(event)
}
