package storage

import (
	"fmt"
	"sync"

	"Distributed-system/internal/message"
)

// PartitionKey represents a unique identifier for a partition
type PartitionKey struct {
	Topic     string
	Partition int
}

// String returns string representation of partition key
func (pk PartitionKey) String() string {
	return fmt.Sprintf("%s-%d", pk.Topic, pk.Partition)
}

// MemoryStorage provides in-memory message storage
type MemoryStorage struct {
	// messages stores messages by partition key and offset
	messages map[PartitionKey]map[int64]*message.Message
	// indexes stores messages by key for efficient key-based lookups
	indexes map[PartitionKey]map[string][]*message.Message
	mu      sync.RWMutex
}

// NewMemoryStorage creates a new in-memory storage instance
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		messages: make(map[PartitionKey]map[int64]*message.Message),
		indexes:  make(map[PartitionKey]map[string][]*message.Message),
	}
}

// Store stores a message in the given topic and partition
func (ms *MemoryStorage) Store(msg *message.Message) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	partitionKey := PartitionKey{
		Topic:     msg.Topic,
		Partition: msg.Partition,
	}

	// Initialize partition storage if it doesn't exist
	if ms.messages[partitionKey] == nil {
		ms.messages[partitionKey] = make(map[int64]*message.Message)
		ms.indexes[partitionKey] = make(map[string][]*message.Message)
	}

	// Store message by offset
	ms.messages[partitionKey][msg.Offset] = msg

	// Index by key if key exists
	if msg.Key != "" {
		ms.indexes[partitionKey][msg.Key] = append(ms.indexes[partitionKey][msg.Key], msg)
	}

	return nil
}

// GetMessage retrieves a message by topic, partition, and offset
func (ms *MemoryStorage) GetMessage(topic string, partition int, offset int64) (*message.Message, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	partitionKey := PartitionKey{
		Topic:     topic,
		Partition: partition,
	}

	partitionMessages, exists := ms.messages[partitionKey]
	if !exists {
		return nil, fmt.Errorf("partition %s does not exist", partitionKey.String())
	}

	msg, exists := partitionMessages[offset]
	if !exists {
		return nil, fmt.Errorf("message at offset %d does not exist in partition %s", offset, partitionKey.String())
	}

	return msg, nil
}

// GetMessages retrieves messages from a given offset with a limit
func (ms *MemoryStorage) GetMessages(topic string, partition int, startOffset int64, limit int) ([]*message.Message, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	partitionKey := PartitionKey{
		Topic:     topic,
		Partition: partition,
	}

	partitionMessages, exists := ms.messages[partitionKey]
	if !exists {
		return []*message.Message{}, nil // Return empty slice if partition doesn't exist
	}

	var messages []*message.Message
	currentOffset := startOffset
	count := 0

	for count < limit {
		if msg, exists := partitionMessages[currentOffset]; exists {
			messages = append(messages, msg)
			count++
		}
		currentOffset++

		// Break if we've gone beyond reasonable range
		if currentOffset > startOffset+int64(limit*2) {
			break
		}
	}

	return messages, nil
}

// GetMessagesByKey retrieves all messages with a specific key from a partition
func (ms *MemoryStorage) GetMessagesByKey(topic string, partition int, key string) ([]*message.Message, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	partitionKey := PartitionKey{
		Topic:     topic,
		Partition: partition,
	}

	partitionIndexes, exists := ms.indexes[partitionKey]
	if !exists {
		return []*message.Message{}, nil
	}

	messages, exists := partitionIndexes[key]
	if !exists {
		return []*message.Message{}, nil
	}

	// Return a copy to avoid external modifications
	result := make([]*message.Message, len(messages))
	copy(result, messages)
	return result, nil
}

// GetPartitionSize returns the number of messages in a partition
func (ms *MemoryStorage) GetPartitionSize(topic string, partition int) int64 {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	partitionKey := PartitionKey{
		Topic:     topic,
		Partition: partition,
	}

	partitionMessages, exists := ms.messages[partitionKey]
	if !exists {
		return 0
	}

	return int64(len(partitionMessages))
}

// GetHighWaterMark returns the highest offset for a partition
func (ms *MemoryStorage) GetHighWaterMark(topic string, partition int) int64 {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	partitionKey := PartitionKey{
		Topic:     topic,
		Partition: partition,
	}

	partitionMessages, exists := ms.messages[partitionKey]
	if !exists {
		return 0
	}

	var highWaterMark int64 = -1
	for offset := range partitionMessages {
		if offset > highWaterMark {
			highWaterMark = offset
		}
	}

	if highWaterMark == -1 {
		return 0
	}
	return highWaterMark + 1 // Next available offset
}

// Clear removes all messages from storage (useful for testing)
func (ms *MemoryStorage) Clear() {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.messages = make(map[PartitionKey]map[int64]*message.Message)
	ms.indexes = make(map[PartitionKey]map[string][]*message.Message)
}

// Add EnsureTopicDirs and Close methods to MemoryStorage to satisfy the Storage interface
func (ms *MemoryStorage) EnsureTopicDirs(meta interface{}) error {
	// No-op for in-memory storage
	return nil
}

func (ms *MemoryStorage) Close() error {
	// No-op for in-memory storage
	return nil
}