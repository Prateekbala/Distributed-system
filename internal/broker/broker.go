// internal/broker/broker.go
package broker

import (
	"Distributed-system/internal/cluster"
	"Distributed-system/internal/message"
	"Distributed-system/internal/offset"
	"Distributed-system/internal/partition"
	"Distributed-system/internal/replication"
	"Distributed-system/internal/storage"
	"fmt"
	"io"
	"log"
	"sync"
	"time"
)

// Broker is the central component that handles API requests, coordinates
// with the cluster, and manages data storage and replication.
type Broker struct {
	mu             sync.RWMutex
	brokerID       string
	apiAddress     string
	apiPort        string
	cluster        *cluster.Cluster
	storage        storage.Storage
	wal            *storage.WAL
	offsetManager  *offset.Manager
	replicationMgr *replication.ReplicationManager
	partitioner    *partition.PartitionAssigner

	stopChan  chan struct{}
	isStopped bool
}

// NewBroker creates a new broker instance with all its dependencies.
// REFACTOR: Simplified constructor. No longer takes local managers.
func NewBroker(brokerID string, cl *cluster.Cluster, st storage.Storage, wal *storage.WAL, om *offset.Manager) *Broker {
	b := &Broker{
		brokerID:      brokerID,
		apiAddress:    cl.APIAddress(),
		apiPort:       cl.APIPort(),
		cluster:       cl,
		storage:       st,
		wal:           wal,
		offsetManager: om,
		partitioner:   partition.NewPartitionAssigner(partition.StrategyHash),
		stopChan:      make(chan struct{}),
	}

	b.replicationMgr = replication.NewReplicationManager(brokerID, cl, st)

	if wal != nil {
		if err := b.performRecovery(); err != nil {
			log.Printf("[WARN] Recovery from WAL failed: %v", err)
		}
	}

	return b
}

// Start begins all background processes for the broker.
func (b *Broker) Start() {
	b.replicationMgr.Start()
	// REFACTOR: Start a loop to ensure disk storage directories are created for topics known by the FSM.
	go b.syncStorageDirs()
	log.Printf("[INFO] Broker %s started successfully.", b.brokerID)
}

// Stop gracefully shuts down the broker.
func (b *Broker) Stop() {
	b.mu.Lock()
	if b.isStopped {
		b.mu.Unlock()
		return
	}
	b.isStopped = true
	b.mu.Unlock()

	log.Println("[INFO] Stopping broker...")
	close(b.stopChan)
	b.replicationMgr.Stop()

	// Use a type assertion to safely close the storage.
	if closer, ok := b.storage.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			log.Printf("[ERROR] Failed to close storage: %v", err)
		}
	}

	if err := b.offsetManager.Close(); err != nil {
		log.Printf("[ERROR] Failed to close offset manager: %v", err)
	}

	log.Println("[INFO] Broker stopped.")
}

// syncStorageDirs periodically checks the FSM for topics and ensures
// that corresponding directories exist on disk.
func (b *Broker) syncStorageDirs() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			topics := b.cluster.RaftNode.FSM.GetTopics()
			if ds, ok := b.storage.(*storage.DiskStorage); ok {
				for _, topicMeta := range topics {
					if err := ds.EnsureTopicDirs(topicMeta); err != nil {
						log.Printf("[ERROR] Failed to ensure directories for topic %s: %v", topicMeta.Name, err)
					}
				}
			}
		case <-b.stopChan:
			return
		}
	}
}

// performRecovery replays the Write-Ahead Log to restore state.
func (b *Broker) performRecovery() error {
	log.Println("[INFO] Starting recovery from WAL...")
	entries, err := b.wal.Recovery()
	if err != nil {
		return fmt.Errorf("failed to read WAL for recovery: %w", err)
	}

	log.Printf("[INFO] Found %d WAL entries to replay.", len(entries))
	for _, entry := range entries {
		if entry.Type == "message" && entry.Message != nil {
			// REFACTOR: Ensure topic directories exist before storing recovered messages.
			if ds, ok := b.storage.(*storage.DiskStorage); ok {
				topics := b.cluster.RaftNode.FSM.GetTopics()
				if topicMeta, exists := topics[entry.Message.Topic]; exists {
					ds.EnsureTopicDirs(topicMeta)
				}
			}
			if err := b.storage.Store(entry.Message); err != nil {
				log.Printf("[WARN] Failed to replay message during recovery: %v", err)
			}
		}
	}
	log.Println("[INFO] Recovery from WAL complete.")
	return nil
}

// CreateTopic proposes the creation of a new topic to the cluster.
func (b *Broker) CreateTopic(name string, numPartitions, replicationFactor int) error {
	// REFACTOR: This is now just a proxy to the cluster method.
	return b.cluster.ProposeTopicCreation(name, numPartitions, replicationFactor)
}

// Produce handles a new message, storing it locally and ensuring it gets replicated.
func (b *Broker) Produce(topicName string, key string, value []byte, headers map[string]string) (*message.Message, error) {
	// REFACTOR: Get topic metadata from the authoritative FSM.
	topics := b.cluster.RaftNode.FSM.GetTopics()
	topicMeta, ok := topics[topicName]
	if !ok {
		return nil, fmt.Errorf("topic '%s' not found", topicName)
	}

	partitionID := b.partitioner.AssignPartition(topicName, key, topicMeta.NumPartitions)

	if !b.replicationMgr.IsLeader(topicName, partitionID) {
		leaderID := b.replicationMgr.GetLeaderBrokerID(topicName, partitionID)
		return nil, fmt.Errorf("not leader for partition %d. current leader is %s", partitionID, leaderID)
	}

	// This broker is the leader, so it assigns the offset.
	offset := b.storage.GetHighWaterMark(topicName, partitionID)
	msg := message.NewMessage(topicName, partitionID, offset, key, value, headers)

	if b.wal != nil {
		if err := b.wal.WriteMessage(topicName, int32(partitionID), msg); err != nil {
			return nil, fmt.Errorf("failed to write message to WAL: %w", err)
		}
	}

	if err := b.storage.Store(msg); err != nil {
		return nil, fmt.Errorf("failed to store message: %w", err)
	}

	log.Printf("[INFO] Leader %s: Stored message for %s-%d at offset %d", b.brokerID, topicName, partitionID, msg.Offset)
	return msg, nil
}

// Consume retrieves messages from a specific partition.
func (b *Broker) Consume(topicName string, partition int, offset int64, limit int) ([]*message.Message, error) {
	return b.storage.GetMessages(topicName, partition, offset, limit)
}

// CommitOffset persists a consumer group's offset.
func (b *Broker) CommitOffset(groupID, topic string, partition int, offset int64) error {
	return b.offsetManager.CommitOffset(groupID, topic, int32(partition), offset)
}

// GetOffset retrieves a consumer group's offset.
func (b *Broker) GetOffset(groupID, topic string, partition int) int64 {
	return b.offsetManager.GetOffset(groupID, topic, int32(partition))
}

// GetTopicMetadata returns metadata about all topics and their partitions.
func (b *Broker) GetTopicMetadata() map[string]map[int]*cluster.PartitionAssignment {
	// REFACTOR: Read directly from the FSM via the cluster.
	return b.cluster.RaftNode.FSM.GetAssignments()
}

// --- New Consumer Group Methods ---

// RegisterConsumer proxies the request to the cluster manager.
func (b *Broker) RegisterConsumer(groupID, consumerID string, topics []string) error {
	return b.cluster.RegisterConsumer(groupID, consumerID, topics)
}

// ConsumerHeartbeat proxies the request to the cluster manager.
func (b *Broker) ConsumerHeartbeat(groupID, consumerID string) error {
	return b.cluster.ConsumerHeartbeat(groupID, consumerID)
}

// GetConsumerGroup reads group info from the authoritative FSM.
func (b *Broker) GetConsumerGroup(groupID string) *cluster.ConsumerGroup {
	groups := b.cluster.RaftNode.FSM.GetConsumerGroups()
	return groups[groupID]
}

// --- Broker Info Getters ---

func (b *Broker) GetBrokerID() string {
	return b.brokerID
}

func (b *Broker) APIAddress() string {
	return b.apiAddress
}

func (b *Broker) APIPort() string {
	return b.apiPort
}
