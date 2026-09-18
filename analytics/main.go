package main

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		Topic:       "user-events",
		GroupID:     "analytics-service",
		StartOffset: kafka.FirstOffset,
	})

	defer reader.Close()

	fmt.Println("Analytics service started")

	for {
		message, err := reader.ReadMessage(context.Background())
		if err != nil {
			fmt.Println("Kafka error", err)
			return
		}
		fmt.Println("Received message:")
		fmt.Println(string(message.Value))
	}
}
