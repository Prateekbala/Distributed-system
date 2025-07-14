package topic

import (
	"fmt"
	"sync"
	"time"
)

// Partition represents a single partition within a topic
type Partition struct {
	ID           int       `json:"id"`
	NextOffset   int64     `json:"next_offset"`
	MessageCount int64     `json:"message_count"`
	CreatedAt    time.Time `json:"created_at"`
}

// Topic represents a topic with its partitions
type Topic struct {
	Name         string               `json:"name"`
	Partitions   map[int]*Partition   `json:"partitions"`
	Config       *TopicConfig         `json:"config"`
	CreatedAt    time.Time           `json:"created_at"`
	mu           sync.RWMutex
}

// TopicConfig holds configuration for a topic
type TopicConfig struct {
	NumPartitions     int           `json:"num_partitions"`
	ReplicationFactor int           `json:"replication_factor"`
	Retention         time.Duration `json:"retention"`
	MaxMessageSize    int64         `json:"max_message_size"`
}

// DefaultTopicConfig returns default configuration for topics
func DefaultTopicConfig() *TopicConfig {
	return &TopicConfig{
		NumPartitions:     1,
		ReplicationFactor: 1,
		Retention:         24 * time.Hour, // 24 hours
		MaxMessageSize:    1024 * 1024,    // 1MB
	}
}

// NewTopic creates a new topic with given name and config
func NewTopic(name string, config *TopicConfig) *Topic {
	if config == nil {
		config = DefaultTopicConfig()
	}

	topic := &Topic{
		Name:       name,
		Partitions: make(map[int]*Partition),
		Config:     config,
		CreatedAt:  time.Now(),
	}

	// Initialize partitions
	for i := 0; i < config.NumPartitions; i++ {
		topic.Partitions[i] = &Partition{
			ID:           i,
			NextOffset:   0,
			MessageCount: 0,
			CreatedAt:    time.Now(),
		}
	}

	return topic
}

// GetPartition returns a partition by ID
func (t *Topic) GetPartition(partitionID int) (*Partition, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	partition, exists := t.Partitions[partitionID]
	if !exists {
		return nil, fmt.Errorf("partition %d does not exist in topic %s", partitionID, t.Name)
	}
	return partition, nil
}

// GetNextOffset returns the next offset for a partition and increments it
func (t *Topic) GetNextOffset(partitionID int) (int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	partition, exists := t.Partitions[partitionID]
	if !exists {
		return 0, fmt.Errorf("partition %d does not exist in topic %s", partitionID, t.Name)
	}

	offset := partition.NextOffset
	partition.NextOffset++
	partition.MessageCount++
	return offset, nil
}

// GetPartitionCount returns the number of partitions
func (t *Topic) GetPartitionCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.Partitions)
}

// TopicManager manages all topics in the broker
type TopicManager struct {
	topics map[string]*Topic
	mu     sync.RWMutex
}

// NewTopicManager creates a new topic manager
func NewTopicManager() *TopicManager {
	return &TopicManager{
		topics: make(map[string]*Topic),
	}
}

// CreateTopic creates a new topic
// NOTE: In cluster mode, topic creation/deletion should be coordinated via Raft leader.
// The TopicManager methods are used by the Raft leader to apply changes locally.
func (tm *TopicManager) CreateTopic(name string, config *TopicConfig) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.topics[name]; exists {
		return fmt.Errorf("topic %s already exists", name)
	}

	tm.topics[name] = NewTopic(name, config)
	return nil
}

// DeleteTopic removes a topic
func (tm *TopicManager) DeleteTopic(name string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.topics[name]; !exists {
		return fmt.Errorf("topic %s does not exist", name)
	}

	delete(tm.topics, name)
	return nil
}

// GetTopic returns a topic by name
func (tm *TopicManager) GetTopic(name string) (*Topic, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	topic, exists := tm.topics[name]
	if !exists {
		return nil, fmt.Errorf("topic %s does not exist", name)
	}
	return topic, nil
}

// ListTopics returns all topic names
func (tm *TopicManager) ListTopics() []string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	names := make([]string, 0, len(tm.topics))
	for name := range tm.topics {
		names = append(names, name)
	}
	return names
}

// TopicExists checks if a topic exists
func (tm *TopicManager) TopicExists(name string) bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	_, exists := tm.topics[name]
	return exists
}