package kafka

import (
	"analytic_project/model"
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokerAddress string, topic string) *Producer {
	writer := &kafka.Writer{
		Addr:  kafka.TCP(brokerAddress),
		Topic: topic}

	return &Producer{writer: writer}
}

func (p *Producer) Publish(event model.Event) error {
	data, err := json.Marshal(event)

	if err != nil {
		return err
	}

	return p.writer.WriteMessages(context.Background(), kafka.Message{Value: data})
}
