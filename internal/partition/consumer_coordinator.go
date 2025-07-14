package partition

import (
	"Distributed-system/internal/metrics"
	"sync"
	"time"
)

// ConsumerCoordinator manages consumer group coordination and load balancing
type ConsumerCoordinator struct {
	groups     map[string]*ConsumerGroup
	mu         sync.RWMutex
	heartbeatInterval time.Duration
	rebalanceTimeout  time.Duration
}

// NewConsumerCoordinator creates a new consumer coordinator
func NewConsumerCoordinator() *ConsumerCoordinator {
	return &ConsumerCoordinator{
		groups:            make(map[string]*ConsumerGroup),
		heartbeatInterval: time.Second * 3,
		rebalanceTimeout:  time.Second * 10,
	}
}
func (c *ConsumerCoordinator) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Clear all consumer groups
	c.groups = make(map[string]*ConsumerGroup)

	return nil
} 
// RegisterConsumer registers a consumer with the coordinator
func (cc *ConsumerCoordinator) RegisterConsumer(groupID, consumerID string, topics []string) error {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	group, exists := cc.groups[groupID]
	if !exists {
		group = &ConsumerGroup{
			ID:                   groupID,
			Consumers:           make(map[string]*Consumer),
			Topics:              topics,
			PartitionAssignments: make(map[string]map[int]string),
		}
		cc.groups[groupID] = group
	}

	consumer := &Consumer{
		ID:                 consumerID,
		GroupID:           groupID,
		Topics:            topics,
		AssignedPartitions: make(map[string][]int),
		LastHeartbeat:     time.Now().UnixNano(),
	}

	group.Consumers[consumerID] = consumer
	cc.rebalanceGroup(group)

	return nil
}

// UnregisterConsumer removes a consumer from the coordinator
func (cc *ConsumerCoordinator) UnregisterConsumer(groupID, consumerID string) error {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	group, exists := cc.groups[groupID]
	if !exists {
		return nil
	}

	delete(group.Consumers, consumerID)
	cc.rebalanceGroup(group)

	return nil
}

// UpdateHeartbeat updates the heartbeat for a consumer
func (cc *ConsumerCoordinator) UpdateHeartbeat(groupID, consumerID string) error {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	group, exists := cc.groups[groupID]
	if !exists {
		return nil
	}

	consumer, exists := group.Consumers[consumerID]
	if !exists {
		return nil
	}

	now := time.Now().Unix()
	consumer.LastHeartbeat = now
	// Update Prometheus metric for last seen
	metrics.ConsumerLastSeen.WithLabelValues(groupID, consumerID).Set(float64(now))
	return nil
}

// rebalanceGroup redistributes partitions among consumers in a group
func (cc *ConsumerCoordinator) rebalanceGroup(group *ConsumerGroup) {
	// Get list of active consumers
	activeConsumers := make([]string, 0)
	now := time.Now().UnixNano()
	for id, consumer := range group.Consumers {
		if now-consumer.LastHeartbeat < cc.rebalanceTimeout.Nanoseconds() {
			activeConsumers = append(activeConsumers, id)
		}
	}

	if len(activeConsumers) == 0 {
		return
	}

	// Clear existing assignments
	for _, consumer := range group.Consumers {
		consumer.AssignedPartitions = make(map[string][]int)
	}

	// For each topic, distribute partitions among active consumers
	for _, topic := range group.Topics {
		// Get partition count for topic (this should come from topic manager)
		partitionCount := 3 // Default to 3 partitions

		// Ensure the map is initialized before assignment
		if group.PartitionAssignments[topic] == nil {
			group.PartitionAssignments[topic] = make(map[int]string)
		}

		// Calculate partitions per consumer
		partitionsPerConsumer := partitionCount / len(activeConsumers)
		extraPartitions := partitionCount % len(activeConsumers)

		// Assign partitions to consumers
		partitionIndex := 0
		for i, consumerID := range activeConsumers {
			consumer := group.Consumers[consumerID]
			consumer.AssignedPartitions[topic] = make([]int, 0)

			// Calculate number of partitions for this consumer
			numPartitions := partitionsPerConsumer
			if i < extraPartitions {
				numPartitions++
			}

			// Assign partitions
			for j := 0; j < numPartitions; j++ {
				partition := partitionIndex + j
				consumer.AssignedPartitions[topic] = append(consumer.AssignedPartitions[topic], partition)
				group.PartitionAssignments[topic][partition] = consumerID
			}

			partitionIndex += numPartitions
		}
	}
}

// GetConsumerAssignments returns the partition assignments for a consumer
func (cc *ConsumerCoordinator) GetConsumerAssignments(groupID, consumerID string) (map[string][]int, error) {
	cc.mu.RLock()
	defer cc.mu.RUnlock()

	group, exists := cc.groups[groupID]
	if !exists {
		return nil, nil
	}

	consumer, exists := group.Consumers[consumerID]
	if !exists {
		return nil, nil
	}

	// Return a copy of the assignments
	assignments := make(map[string][]int)
	for topic, partitions := range consumer.AssignedPartitions {
		assignments[topic] = make([]int, len(partitions))
		copy(assignments[topic], partitions)
	}

	return assignments, nil
}

// GetGroupMetadata returns metadata about a consumer group
func (cc *ConsumerCoordinator) GetGroupMetadata(groupID string) (*ConsumerGroupMetadata, error) {
	cc.mu.RLock()
	defer cc.mu.RUnlock()

	group, exists := cc.groups[groupID]
	if !exists {
		return nil, nil
	}

	metadata := &ConsumerGroupMetadata{
		ID:     groupID,
		Topics: group.Topics,
		Consumers: make([]ConsumerMetadata, 0, len(group.Consumers)),
	}

	for _, consumer := range group.Consumers {
		consumerMeta := ConsumerMetadata{
			ID:                 consumer.ID,
			AssignedPartitions: make(map[string][]int),
			LastHeartbeat:      consumer.LastHeartbeat,
		}

		for topic, partitions := range consumer.AssignedPartitions {
			consumerMeta.AssignedPartitions[topic] = make([]int, len(partitions))
			copy(consumerMeta.AssignedPartitions[topic], partitions)
		}

		metadata.Consumers = append(metadata.Consumers, consumerMeta)
	}

	return metadata, nil
}

// GroupMetrics holds metrics for a consumer group (for rebalancing)
type GroupMetrics struct {
	ConsumerLag      map[string]map[int]int64 // topic -> partition -> lag
	ConsumerThroughput map[string]map[int]float64 // topic -> partition -> msgs/sec
	ConsumerHealth   map[string]int64 // consumerID -> last seen unix
}

// GetGroupMetrics returns lag, throughput, and health for a group
func (cc *ConsumerCoordinator) GetGroupMetrics(groupID string) *GroupMetrics {
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	group, exists := cc.groups[groupID]
	if !exists {
		return nil
	}
	gm := &GroupMetrics{
		ConsumerLag:      make(map[string]map[int]int64),
		ConsumerThroughput: make(map[string]map[int]float64),
		ConsumerHealth:   make(map[string]int64),
	}
	for cid, consumer := range group.Consumers {
		gm.ConsumerHealth[cid] = consumer.LastHeartbeat
	}
	// TODO: Fill lag and throughput from metrics or internal state
	return gm
}

// RebalanceGroupByID safely triggers a rebalance for a group by ID
func (cc *ConsumerCoordinator) RebalanceGroupByID(groupID string) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	group, exists := cc.groups[groupID]
	if exists {
		cc.rebalanceGroup(group)
	}
} 