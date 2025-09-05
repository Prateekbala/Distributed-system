// internal/message/message.go
package message

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Message represents a single message in the broker
type Message struct {
	ID        string            `json:"id"`
	Key       string            `json:"key,omitempty"`
	Value     []byte            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
	Offset    int64             `json:"offset"`
	Partition int               `json:"partition"`
	Topic     string            `json:"topic"`
}

func NewMessage(topic string, partition int, offset int64, key string, value []byte, headers map[string]string) *Message {
	return &Message{
		ID:        uuid.New().String(),
		Key:       key,
		Value:     value,
		Headers:   headers,
		Timestamp: time.Now(),
		Topic:     topic,
		Partition: partition,
		Offset:    offset, // Assign the provided offset
	}
}

// Serialize converts message to JSON bytes for storage.
func (m *Message) Serialize() ([]byte, error) {
	return json.Marshal(m)
}

// Deserialize creates a message from JSON bytes.
func Deserialize(data []byte) (*Message, error) {
	var msg Message
	err := json.Unmarshal(data, &msg)
	return &msg, err
}

// Size returns the serialized size of the message.
func (m *Message) Size() int {
	data, _ := m.Serialize()
	return len(data)
}
