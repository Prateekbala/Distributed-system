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

// NewMessage creates a new message with generated ID and current timestamp
func NewMessage(topic string, partition int, key string, value []byte, headers map[string]string) *Message {
	return &Message{
		ID:        uuid.New().String(),
		Key:       key,
		Value:     value,
		Headers:   headers,
		Timestamp: time.Now(),
		Topic:     topic,
		Partition: partition,
		
	}
}

// ToJSON converts message to JSON bytes
func (m *Message) ToJSON() ([]byte, error) {
	return json.Marshal(m)
}

// FromJSON creates a message from JSON bytes
func FromJSON(data []byte) (*Message, error) {
	var msg Message
	err := json.Unmarshal(data, &msg)
	return &msg, err
}

// Size returns the approximate size of the message in bytes
func (m *Message) SizeAprox() int64 {
	size := len(m.ID) + len(m.Key) + len(m.Value) + len(m.Topic)
	for k, v := range m.Headers {
		size += len(k) + len(v)
	}
	return int64(size + 64) // Add some overhead for other fields
}
func (m *Message) Serialize() ([]byte, error) {
    return json.Marshal(m)
}

// Deserialize message from disk
func Deserialize(data []byte) (*Message, error) {
    var msg Message
    err := json.Unmarshal(data, &msg)
    return &msg, err
}

// Size returns the serialized size of the message
func (m *Message) Size() int {
    data, _ := m.Serialize()
    return len(data)
}