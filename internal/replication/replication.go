package replication

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"Distributed-system/internal/cluster"
	"Distributed-system/internal/message"
	"Distributed-system/internal/storage"
	"Distributed-system/internal/topic"
)

// ReplicationFactor defines how many replicas each partition should have
const DefaultReplicationFactor = 3

// PartitionReplica represents a replica of a partition
type PartitionReplica struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	NodeID    string `json:"node_id"`
	IsLeader  bool   `json:"is_leader"`
	IsSynced  bool   `json:"is_synced"`
	LastOffset int64 `json:"last_offset"`
}

// PartitionAssignment represents a partition assignment
type PartitionAssignment struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	LeaderID  string `json:"leader_id"`
	Replicas  []string `json:"replicas"`
}

// ReplicationManager handles message replication between brokers
type ReplicationManager struct {
	*ReplicaManager
	brokerID    string
	leaderPeers map[string]string // topic-partition -> leader broker ID
	followers   map[string][]string // topic-partition -> follower broker IDs
	isr         map[string][]string // topic-partition -> in-sync replica broker IDs
	storage     storage.Storage
	wal         *storage.WAL
	cluster     *cluster.Cluster
	mu          sync.RWMutex
}

// NewReplicationManager creates a new replication manager
func NewReplicationManager(brokerID string, storage storage.Storage, wal *storage.WAL, cluster *cluster.Cluster) *ReplicationManager {
	return &ReplicationManager{
		ReplicaManager: NewReplicaManager(),
		brokerID:    brokerID,
		leaderPeers: make(map[string]string),
		followers:   make(map[string][]string),
		isr:         make(map[string][]string),
		storage:     storage,
		wal:         wal,
		cluster:     cluster,
	}
}

// SetLeader sets the leader for a topic-partition
func (rm *ReplicationManager) SetLeader(topic string, partition int, leaderID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	key := fmt.Sprintf("%s-%d", topic, partition)
	rm.leaderPeers[key] = leaderID
}

// GetLeader gets the leader for a topic-partition
func (rm *ReplicationManager) GetLeader(topic string, partition int) string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	key := fmt.Sprintf("%s-%d", topic, partition)
	return rm.leaderPeers[key]
}

// AddFollower adds a follower for a topic-partition
func (rm *ReplicationManager) AddFollower(topic string, partition int, followerID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	key := fmt.Sprintf("%s-%d", topic, partition)
	rm.followers[key] = append(rm.followers[key], followerID)
}

// RemoveFollower removes a follower from a topic-partition
func (rm *ReplicationManager) RemoveFollower(topic string, partition int, followerID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	key := fmt.Sprintf("%s-%d", topic, partition)
	
	followers := rm.followers[key]
	newFollowers := make([]string, 0, len(followers))
	for _, f := range followers {
		if f != followerID {
			newFollowers = append(newFollowers, f)
		}
	}
	rm.followers[key] = newFollowers
}

// IsLeader checks if this broker is the leader for a topic-partition
func (rm *ReplicationManager) IsLeader(topic string, partition int) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	key := fmt.Sprintf("%s-%d", topic, partition)
	return rm.leaderPeers[key] == rm.brokerID
}

// GetFollowers returns all followers for a topic-partition
func (rm *ReplicationManager) GetFollowers(topic string, partition int) []string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	key := fmt.Sprintf("%s-%d", topic, partition)
	return rm.followers[key]
}

// GetAllAssignments returns all partition assignments
func (rm *ReplicationManager) GetAllAssignments() []*PartitionAssignment {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	assignments := make([]*PartitionAssignment, 0)
	seen := make(map[string]bool)

	for key, leaderID := range rm.leaderPeers {
		if seen[key] {
			continue
		}
		seen[key] = true

		// Parse topic and partition from key
		var topic string
		var partition int
		fmt.Sscanf(key, "%s-%d", &topic, &partition)

		assignment := &PartitionAssignment{
			Topic:     topic,
			Partition: partition,
			LeaderID:  leaderID,
			Replicas:  append([]string{leaderID}, rm.followers[key]...),
		}
		assignments = append(assignments, assignment)
	}

	return assignments
}

// ReassignPartition reassigns a partition to new replicas
func (rm *ReplicationManager) ReassignPartition(topic string, partition int, newLeaderID string, newFollowers []string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topic, partition)
	
	// Update leader
	rm.leaderPeers[key] = newLeaderID
	
	// Update followers
	rm.followers[key] = newFollowers

	log.Printf("Reassigned partition %s-%d: leader=%s, followers=%v", 
		topic, partition, newLeaderID, newFollowers)
}

// ReplicateMessage replicates a message to all followers
func (rm *ReplicationManager) ReplicateMessage(msg *message.Message) error {
	if !rm.IsLeader(msg.Topic, msg.Partition) {
		return fmt.Errorf("not leader for topic %s partition %d", msg.Topic, msg.Partition)
	}

	followers := rm.GetFollowers(msg.Topic, msg.Partition)
	for _, followerID := range followers {
		go func(followerID string) {
			if err := rm.sendToFollower(followerID, msg); err != nil {
				log.Printf("Failed to replicate message to follower %s: %v", followerID, err)
			}
		}(followerID)
	}
	return nil
}

// sendToFollower sends a message to a follower broker
func (rm *ReplicationManager) sendToFollower(followerID string, msg *message.Message) error {
	// Get the follower's address from the cluster
	followerAddr := rm.getFollowerAddress(followerID)
	if followerAddr == "" {
		return fmt.Errorf("follower %s address not found", followerID)
	}

	// Serialize the message
	data, err := msg.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize message: %v", err)
	}

	// Send the message to the follower
	url := fmt.Sprintf("http://%s/replication", followerAddr)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("failed to send message to follower: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("follower returned error status: %d", resp.StatusCode)
	}

	log.Printf("Successfully replicated message to follower %s: topic=%s, partition=%d, offset=%d",
		followerID, msg.Topic, msg.Partition, msg.Offset)
	return nil
}

// HandleReplicationRequest handles incoming replication requests from the leader
func (rm *ReplicationManager) HandleReplicationRequest(w http.ResponseWriter, r *http.Request) {
	var msg message.Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// Store the replicated message
	if err := rm.storeReplicatedMessage(&msg); err != nil {
		log.Printf("Failed to store replicated message: %v", err)
		http.Error(w, "Failed to store message", http.StatusInternalServerError)
		return
	}

	log.Printf("Successfully stored replicated message: topic=%s, partition=%d, offset=%d",
		msg.Topic, msg.Partition, msg.Offset)
	w.WriteHeader(http.StatusOK)
}

// storeReplicatedMessage stores a replicated message
func (rm *ReplicationManager) storeReplicatedMessage(msg *message.Message) error {
	if rm.storage == nil {
		return fmt.Errorf("storage not available")
	}

	// Log the storage type
	switch rm.storage.(type) {
	case *storage.DiskStorage:
		log.Printf("Storing replicated message using disk storage: topic=%s, partition=%d, offset=%d",
			msg.Topic, msg.Partition, msg.Offset)
	case *storage.MemoryStorage:
		log.Printf("Storing replicated message using memory storage: topic=%s, partition=%d, offset=%d",
			msg.Topic, msg.Partition, msg.Offset)
	default:
		log.Printf("Storing replicated message using unknown storage type: topic=%s, partition=%d, offset=%d",
			msg.Topic, msg.Partition, msg.Offset)
	}

	// Store the message
	if err := rm.storage.Store(msg); err != nil {
		return fmt.Errorf("failed to store replicated message: %v", err)
	}

	// Write to WAL if available
	if rm.wal != nil {
		_ = rm.wal.WriteMessage(msg.Topic, int32(msg.Partition), msg)
	}

	return nil
}

// getFollowerAddress gets the address of a follower broker
func (rm *ReplicationManager) getFollowerAddress(followerID string) string {
	if rm.cluster == nil {
		return ""
	}
	return rm.cluster.GetBrokerAddress(followerID)
}

// getStorage gets the storage interface from the broker
func (rm *ReplicationManager) getStorage() storage.Storage {
	return rm.storage
}

// HandleReassignmentRequest handles partition reassignment requests
func (rm *ReplicationManager) HandleReassignmentRequest(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Topic     string   `json:"topic"`
		Partition int      `json:"partition"`
		LeaderID  string   `json:"leader_id"`
		Replicas  []string `json:"replicas"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// Update assignment
	rm.ReassignPartition(req.Topic, req.Partition, req.LeaderID, req.Replicas)

	w.WriteHeader(http.StatusOK)
}

// ReplicationStatus represents the current replication status
type ReplicationStatus struct {
	NodeID             string              `json:"node_id"`
	LeaderPartitions   map[string][]int32  `json:"leader_partitions"`
	FollowerPartitions map[string][]int32  `json:"follower_partitions"`
	ReplicationFactor  int                 `json:"replication_factor"`
	QueueSize          int                 `json:"queue_size"`
}

// Stop stops the replication manager and cleans up resources
func (rm *ReplicationManager) Stop() error {
	// Close any open replication connections
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Clear any pending replication tasks
	rm.leaderPeers = make(map[string]string)
	rm.followers = make(map[string][]string)

	return nil
}

// ReplicateTopicCreation replicates topic creation across the cluster
func (rm *ReplicationManager) ReplicateTopicCreation(topic string, config *topic.TopicConfig) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Get all brokers in the cluster
	brokers := rm.getClusterBrokers()
	if len(brokers) == 0 {
		return fmt.Errorf("no brokers available for replication")
	}

	// Create topic on all brokers
	for _, brokerID := range brokers {
		if brokerID == rm.brokerID {
			continue // Skip self
		}

		// Get broker address
		brokerAddr := rm.getFollowerAddress(brokerID)
		if brokerAddr == "" {
			log.Printf("Warning: Could not find address for broker %s", brokerID)
			continue
		}

		// Send topic creation request
		url := fmt.Sprintf("http://%s/topics/%s", brokerAddr, topic)
		data, err := json.Marshal(map[string]interface{}{
			"num_partitions":     config.NumPartitions,
			"replication_factor": config.ReplicationFactor,
		})
		if err != nil {
			log.Printf("Warning: Failed to marshal topic config for broker %s: %v", brokerID, err)
			continue
		}

		resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
		if err != nil {
			log.Printf("Warning: Failed to create topic on broker %s: %v", brokerID, err)
			continue
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			log.Printf("Warning: Broker %s returned error status %d for topic creation", 
				brokerID, resp.StatusCode)
			continue
		}

		log.Printf("Successfully created topic %s on broker %s", topic, brokerID)
	}

	return nil
}

// getClusterBrokers returns all broker IDs in the cluster
func (rm *ReplicationManager) getClusterBrokers() []string {
	// In a real implementation, this would get the list from the cluster manager
	// For now, return a hardcoded list
	return []string{"broker-1", "broker-2", "broker-3"}
}

// SetFollowers sets the followers for a topic-partition and initializes ISR
func (rm *ReplicationManager) SetFollowers(topic string, partition int, followers []string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	key := fmt.Sprintf("%s-%d", topic, partition)
	rm.followers[key] = followers
	// Initialize ISR to all replicas (including leader)
	isr := make([]string, 0, len(followers)+1)
	isr = append(isr, rm.leaderPeers[key])
	isr = append(isr, followers...)
	rm.isr[key] = isr
}

// MarkReplicaInSync marks a replica as in-sync for a topic-partition
func (rm *ReplicationManager) MarkReplicaInSync(topic string, partition int, brokerID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	key := fmt.Sprintf("%s-%d", topic, partition)
	for _, id := range rm.isr[key] {
		if id == brokerID {
			return // already in ISR
		}
	}
	rm.isr[key] = append(rm.isr[key], brokerID)
}

// RemoveReplicaFromISR removes a replica from ISR
func (rm *ReplicationManager) RemoveReplicaFromISR(topic string, partition int, brokerID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	key := fmt.Sprintf("%s-%d", topic, partition)
	newISR := make([]string, 0, len(rm.isr[key]))
	for _, id := range rm.isr[key] {
		if id != brokerID {
			newISR = append(newISR, id)
		}
	}
	rm.isr[key] = newISR
}

// GetISR returns the list of in-sync replicas for a topic-partition
func (rm *ReplicationManager) GetISR(topic string, partition int) []string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	key := fmt.Sprintf("%s-%d", topic, partition)
	return append([]string{}, rm.isr[key]...)
}

// StartFollowerReplicationLoop starts a background loop to fetch messages from leaders for all followed partitions
func (rm *ReplicationManager) StartFollowerReplicationLoop(fetchFunc func(topic string, partition int, offset int64, leaderID string) ([]*message.Message, error)) {
	go func() {
		for {
			time.Sleep(2 * time.Second)
			rm.mu.RLock()
			for key, followers := range rm.followers {
				// key is topic-partition string
				var topic string
				var partition int
				fmt.Sscanf(key, "%s-%d", &topic, &partition)
				for _, followerID := range followers {
					if followerID == rm.brokerID {
						leaderID := rm.leaderPeers[key]
						if leaderID == "" || leaderID == rm.brokerID {
							continue // No leader or self-leader
						}
						// Get last offset for this partition
						lastOffset := rm.storage.GetHighWaterMark(topic, partition)
						msgs, err := fetchFunc(topic, partition, lastOffset, leaderID)
						if err != nil {
							log.Printf("Follower failed to fetch from leader: %v", err)
							continue
						}
						for _, msg := range msgs {
							if err := rm.storeReplicatedMessage(msg); err == nil {
								rm.MarkReplicaInSync(topic, partition, rm.brokerID)
							}
						}
					}
				}
			}
			rm.mu.RUnlock()
		}
	}()
}