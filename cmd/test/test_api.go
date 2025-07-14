package main

import (
	"Distributed-system/pkg/api"
	"fmt"
	"log"
	"testing"
	"time"
)

func TestProducerAPI(t *testing.T) {
	producer := api.NewProducer(nil)
	defer producer.Close()

	// Test single message production
	t.Run("SingleMessage", func(t *testing.T) {
		msg := &api.ProduceMessage{
			Topic:   "test-topic",
			Value:   "Test single message",
			Headers: map[string]string{"test": "header"},
		}

		resp, err := producer.Send(msg)
		if err != nil {
			t.Fatalf("Failed to send message: %v", err)
		}

		if resp.Topic != msg.Topic {
			t.Errorf("Expected topic %s, got %s", msg.Topic, resp.Topic)
		}
	})

	// Test batch message production
	t.Run("BatchMessages", func(t *testing.T) {
		var messages []*api.ProduceMessage
		for i := 0; i < 3; i++ {
			messages = append(messages, &api.ProduceMessage{
				Topic:   "test-topic",
				Value:   fmt.Sprintf("Test batch message %d", i),
				Headers: map[string]string{"test": "header"},
			})
		}

		responses, err := producer.SendBatch(messages)
		if err != nil {
			t.Fatalf("Failed to send batch: %v", err)
		}

		if len(responses) != len(messages) {
			t.Errorf("Expected %d responses, got %d", len(messages), len(responses))
		}
	})

	// Test async message production
	t.Run("AsyncMessage", func(t *testing.T) {
		done := make(chan struct{})
		msg := &api.ProduceMessage{
			Topic:   "test-topic",
			Value:   "Test async message",
			Headers: map[string]string{"test": "header"},
		}

		producer.SendAsync(msg, func(resp *api.ProduceResponse, err error) {
			if err != nil {
				t.Errorf("Failed to send async message: %v", err)
			}
			close(done)
		})

		select {
		case <-done:
			// Success
		case <-time.After(3 * time.Second):
			t.Fatal("Async message send timed out")
		}
	})
}

func TestConsumerAPI(t *testing.T) {
	consumer := api.NewConsumer(nil)
	defer consumer.Close()

	// Test subscription
	t.Run("Subscribe", func(t *testing.T) {
		err := consumer.Subscribe([]string{"test-topic"})
		if err != nil {
			t.Fatalf("Failed to subscribe: %v", err)
		}
	})

	// Test message consumption
	t.Run("Consume", func(t *testing.T) {
		messages, err := consumer.Poll(time.Second)
		if err != nil {
			t.Fatalf("Failed to poll messages: %v", err)
		}

		for _, msg := range messages {
			if msg.Topic != "test-topic" {
				t.Errorf("Expected topic test-topic, got %s", msg.Topic)
			}
		}
	})

	// Test offset commit
	t.Run("Commit", func(t *testing.T) {
		err := consumer.Commit()
		if err != nil {
			t.Fatalf("Failed to commit offsets: %v", err)
		}
	})

	// Test background consumption
	t.Run("BackgroundConsumption", func(t *testing.T) {
		done := make(chan struct{})
		err := consumer.StartConsuming(func(messages []*api.ConsumerMessage) error {
			for _, msg := range messages {
				log.Printf("Received message: %s", msg.Value)
			}
			close(done)
			return nil
		})
		if err != nil {
			t.Fatalf("Failed to start background consumption: %v", err)
		}

		select {
		case <-done:
			// Success
		case <-time.After(3 * time.Second):
			t.Fatal("Background consumption timed out")
		}

		err = consumer.StopConsuming()
		if err != nil {
			t.Fatalf("Failed to stop consumption: %v", err)
		}
	})
}

func TestProducerConsumerIntegration(t *testing.T) {
	producer := api.NewProducer(nil)
	consumer := api.NewConsumer(nil)
	defer producer.Close()
	defer consumer.Close()

	// Subscribe consumer
	err := consumer.Subscribe([]string{"test-topic"})
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	// Produce a message
	msg := &api.ProduceMessage{
		Topic:   "test-topic",
		Value:   "Integration test message",
		Headers: map[string]string{"test": "header"},
	}

	resp, err := producer.Send(msg)
	if err != nil {
		t.Fatalf("Failed to send message: %v", err)
	}

	// Consume the message
	messages, err := consumer.Poll(time.Second)
	if err != nil {
		t.Fatalf("Failed to poll messages: %v", err)
	}

	found := false
	for _, consumedMsg := range messages {
		if consumedMsg.Value == msg.Value {
			found = true
			if consumedMsg.Topic != resp.Topic {
				t.Errorf("Expected topic %s, got %s", resp.Topic, consumedMsg.Topic)
			}
			if consumedMsg.Partition != resp.Partition {
				t.Errorf("Expected partition %d, got %d", resp.Partition, consumedMsg.Partition)
			}
			if consumedMsg.Offset != resp.Offset {
				t.Errorf("Expected offset %d, got %d", resp.Offset, consumedMsg.Offset)
			}
			break
		}
	}

	if !found {
		t.Error("Failed to find produced message in consumed messages")
	}
} 