// internal/partition/partition.go
package partition

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"sync"
)

// PartitionStrategy defines how messages are assigned to partitions
type PartitionStrategy string

const (
	StrategyHash       PartitionStrategy = "hash"       // Hash-based on key
	StrategyRoundRobin PartitionStrategy = "round_robin" // Round-robin distribution
	StrategyRandom     PartitionStrategy = "random"     // Random assignment
)

// PartitionAssigner handles partition assignment for messages
type PartitionAssigner struct {
	strategy    PartitionStrategy
	roundRobin  map[string]int // topic -> next partition
	mu          sync.RWMutex
}

// NewPartitionAssigner creates a new partition assigner
func NewPartitionAssigner(strategy PartitionStrategy) *PartitionAssigner {
	return &PartitionAssigner{
		strategy:   strategy,
		roundRobin: make(map[string]int),
	}
}

// AssignPartition assigns a partition for a message
func (pa *PartitionAssigner) AssignPartition(topic, key string, numPartitions int) int {
	if numPartitions <= 0 {
		return 0
	}

	switch pa.strategy {
	case StrategyHash:
		return pa.hashPartition(key, numPartitions)
	case StrategyRoundRobin:
		return pa.roundRobinPartition(topic, numPartitions)
	case StrategyRandom:
		return pa.randomPartition(numPartitions)
	default:
		return pa.hashPartition(key, numPartitions)
	}
}

// hashPartition assigns partition based on key hash
func (pa *PartitionAssigner) hashPartition(key string, numPartitions int) int {
	if key == "" {
		// If no key, use random assignment
		return rand.Intn(numPartitions)
	}

	hasher := fnv.New32a()
	hasher.Write([]byte(key))
	return int(hasher.Sum32()) % numPartitions
}

// roundRobinPartition assigns partition in round-robin fashion
func (pa *PartitionAssigner) roundRobinPartition(topic string, numPartitions int) int {
	pa.mu.Lock()
	defer pa.mu.Unlock()

	current := pa.roundRobin[topic]
	next := (current + 1) % numPartitions
	pa.roundRobin[topic] = next

	return current
}

// randomPartition assigns partition randomly
func (pa *PartitionAssigner) randomPartition(numPartitions int) int {
	return rand.Intn(numPartitions)
}

// ConsumerGroupManager manages consumer groups and partition assignments
type ConsumerGroupManager struct {
	groups map[string]*ConsumerGroup
	mu     sync.RWMutex
}

// NewConsumerGroupManager creates a new consumer group manager
func NewConsumerGroupManager() *ConsumerGroupManager {
	return &ConsumerGroupManager{
		groups: make(map[string]*ConsumerGroup),
	}
}

// ConsumerGroup represents a group of consumers
type ConsumerGroup struct {
	ID              string                           `json:"id"`
	Consumers       map[string]*Consumer            `json:"consumers"`
	Assignments     map[string][]int                `json:"assignments"` // consumer_id -> partitions
	Topics          []string                        `json:"topics"`
	PartitionAssignments map[string]map[int]string  `json:"partition_assignments"` // topic -> partition -> consumer_id
	mu              sync.RWMutex
}

// Consumer represents a single consumer
type Consumer struct {
	ID               string   `json:"id"`
	GroupID          string   `json:"group_id"`
	Topics           []string `json:"topics"`
	AssignedPartitions map[string][]int `json:"assigned_partitions"` // topic -> partitions
	LastHeartbeat    int64    `json:"last_heartbeat"`
}

// CreateConsumerGroup creates a new consumer group
func (cgm *ConsumerGroupManager) CreateConsumerGroup(groupID string, topics []string) *ConsumerGroup {
	cgm.mu.Lock()
	defer cgm.mu.Unlock()

	if group, exists := cgm.groups[groupID]; exists {
		return group
	}

	group := &ConsumerGroup{
		ID:                   groupID,
		Consumers:           make(map[string]*Consumer),
		Assignments:         make(map[string][]int),
		Topics:              topics,
		PartitionAssignments: make(map[string]map[int]string),
	}

	// Initialize partition assignments for each topic
	for _, topic := range topics {
		group.PartitionAssignments[topic] = make(map[int]string)
	}

	cgm.groups[groupID] = group
	return group
}

// GetConsumerGroup returns a consumer group by ID
func (cgm *ConsumerGroupManager) GetConsumerGroup(groupID string) (*ConsumerGroup, error) {
	cgm.mu.RLock()
	defer cgm.mu.RUnlock()

	group, exists := cgm.groups[groupID]
	if !exists {
		return nil, fmt.Errorf("consumer group %s does not exist", groupID)
	}

	return group, nil
}

// AddConsumer adds a consumer to a group
func (cgm *ConsumerGroupManager) AddConsumer(groupID, consumerID string, topics []string) error {
	cgm.mu.Lock()
	defer cgm.mu.Unlock()

	group, exists := cgm.groups[groupID]
	if !exists {
		group = cgm.CreateConsumerGroup(groupID, topics)
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	consumer := &Consumer{
		ID:                 consumerID,
		GroupID:           groupID,
		Topics:            topics,
		AssignedPartitions: make(map[string][]int),
		LastHeartbeat:     0,
	}

	group.Consumers[consumerID] = consumer

	// Trigger rebalancing after adding consumer
	cgm.rebalanceGroup(group, topics)

	return nil
}

// RemoveConsumer removes a consumer from a group
func (cgm *ConsumerGroupManager) RemoveConsumer(groupID, consumerID string) error {
	cgm.mu.Lock()
	defer cgm.mu.Unlock()

	group, exists := cgm.groups[groupID]
	if !exists {
		return fmt.Errorf("consumer group %s does not exist", groupID)
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	consumer, exists := group.Consumers[consumerID]
	if !exists {
		return fmt.Errorf("consumer %s does not exist in group %s", consumerID, groupID)
	}

	delete(group.Consumers, consumerID)

	// Trigger rebalancing after removing consumer
	cgm.rebalanceGroup(group, consumer.Topics)

	return nil
}

// rebalanceGroup redistributes partitions among consumers in a group
func (cgm *ConsumerGroupManager) rebalanceGroup(group *ConsumerGroup, topics []string) {
	// Get partition counts for each topic (this should come from topic manager)
	// For now, we'll assume a fixed number of partitions per topic
	topicPartitions := make(map[string]int)
	for _, topic := range topics {
		topicPartitions[topic] = 3 // Default to 3 partitions per topic
	}

	// Clear existing assignments
	group.Assignments = make(map[string][]int)
	for topic := range group.PartitionAssignments {
		group.PartitionAssignments[topic] = make(map[int]string)
	}

	// If no consumers, nothing to assign
	if len(group.Consumers) == 0 {
		return
	}

	// Get list of consumer IDs
	consumerIDs := make([]string, 0, len(group.Consumers))
	for consumerID := range group.Consumers {
		consumerIDs = append(consumerIDs, consumerID)
	}

	// Assign partitions for each topic
	for _, topic := range topics {
		partitionCount := topicPartitions[topic]
		
		// Initialize consumer assignments for this topic
		for _, consumerID := range consumerIDs {
			if group.Consumers[consumerID].AssignedPartitions[topic] == nil {
				group.Consumers[consumerID].AssignedPartitions[topic] = make([]int, 0)
			}
		}

		// Distribute partitions among consumers
		for partition := 0; partition < partitionCount; partition++ {
			consumerIndex := partition % len(consumerIDs)
			consumerID := consumerIDs[consumerIndex]

			// Assign partition to consumer
			group.Consumers[consumerID].AssignedPartitions[topic] = 
				append(group.Consumers[consumerID].AssignedPartitions[topic], partition)
			
			// Update group's partition assignments
			group.PartitionAssignments[topic][partition] = consumerID

			// Update group assignments
			if group.Assignments[consumerID] == nil {
				group.Assignments[consumerID] = make([]int, 0)
			}
			group.Assignments[consumerID] = append(group.Assignments[consumerID], partition)
		}
	}
}

// GetConsumerAssignments returns partition assignments for a consumer
func (cgm *ConsumerGroupManager) GetConsumerAssignments(groupID, consumerID string) (map[string][]int, error) {
	group, err := cgm.GetConsumerGroup(groupID)
	if err != nil {
		return nil, err
	}

	group.mu.RLock()
	defer group.mu.RUnlock()

	consumer, exists := group.Consumers[consumerID]
	if !exists {
		return nil, fmt.Errorf("consumer %s does not exist in group %s", consumerID, groupID)
	}

	return consumer.AssignedPartitions, nil
}

// GetPartitionConsumer returns which consumer is assigned to a specific partition
func (cgm *ConsumerGroupManager) GetPartitionConsumer(groupID, topic string, partition int) (string, error) {
	group, err := cgm.GetConsumerGroup(groupID)
	if err != nil {
		return "", err
	}

	group.mu.RLock()
	defer group.mu.RUnlock()

	if assignments, exists := group.PartitionAssignments[topic]; exists {
		if consumerID, exists := assignments[partition]; exists {
			return consumerID, nil
		}
	}

	return "", fmt.Errorf("no consumer assigned to partition %d of topic %s in group %s", 
		partition, topic, groupID)
}

// ListConsumerGroups returns all consumer group IDs
func (cgm *ConsumerGroupManager) ListConsumerGroups() []string {
	cgm.mu.RLock()
	defer cgm.mu.RUnlock()

	groups := make([]string, 0, len(cgm.groups))
	for groupID := range cgm.groups {
		groups = append(groups, groupID)
	}

	return groups
}

// GetGroupMetadata returns metadata about a consumer group
func (cgm *ConsumerGroupManager) GetGroupMetadata(groupID string) (*ConsumerGroupMetadata, error) {
	group, err := cgm.GetConsumerGroup(groupID)
	if err != nil {
		return nil, err
	}

	group.mu.RLock()
	defer group.mu.RUnlock()

	metadata := &ConsumerGroupMetadata{
		ID:        groupID,
		Topics:    group.Topics,
		Consumers: make([]ConsumerMetadata, 0),
	}

	for _, consumer := range group.Consumers {
		consumerMeta := ConsumerMetadata{
			ID:                 consumer.ID,
			AssignedPartitions: consumer.AssignedPartitions,
			LastHeartbeat:     consumer.LastHeartbeat,
		}
		metadata.Consumers = append(metadata.Consumers, consumerMeta)
	}

	return metadata, nil
}

// ConsumerGroupMetadata represents metadata about a consumer group
type ConsumerGroupMetadata struct {
	ID        string             `json:"id"`
	Topics    []string           `json:"topics"`
	Consumers []ConsumerMetadata `json:"consumers"`
}

// ConsumerMetadata represents metadata about a consumer
type ConsumerMetadata struct {
	ID                 string           `json:"id"`
	AssignedPartitions map[string][]int `json:"assigned_partitions"`
	LastHeartbeat      int64            `json:"last_heartbeat"`
}