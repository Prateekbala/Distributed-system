package broker

import (
	"Distributed-system/internal/cluster"
	"Distributed-system/internal/message"
	"Distributed-system/internal/metrics"
	"Distributed-system/internal/offset"
	"Distributed-system/internal/partition"
	"Distributed-system/internal/replication"
	"Distributed-system/internal/storage"
	"Distributed-system/internal/topic"
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"net/http"
	"sync"
	"time"
)

// Broker represents a Kafka broker
type Broker struct {
	topicManager   *topic.TopicManager
	storage        storage.Storage
	consumers      map[string]*ConsumerGroup // consumer group ID -> consumer group
	wal            *storage.WAL
	offsetManager  *offset.Manager
	coordinator    *partition.ConsumerCoordinator
	cluster        *cluster.Cluster
	replicationMgr *replication.ReplicationManager
	mu             sync.RWMutex
	stopChan       chan struct{}
	isStopped      bool
}

// ConsumerGroup represents a group of consumers
type ConsumerGroup struct {
	ID        string
	Consumers map[string]*Consumer // consumer ID -> consumer
	Offsets   map[string]int64     // partition key -> offset
	mu        sync.RWMutex
}

// Consumer represents a single consumer
type Consumer struct {
	ID      string
	GroupID string
	Topics  []string
}

// NewBroker creates a new broker instance
func NewBroker() *Broker {
	b := &Broker{
		topicManager: topic.NewTopicManager(),
		storage:      storage.NewMemoryStorage(),
		consumers:    make(map[string]*ConsumerGroup),
		coordinator:  partition.NewConsumerCoordinator(),
		stopChan:     make(chan struct{}),
	}
	cluster.RegisterBroker(b)
	return b
}

// NewBrokerWithStorage creates a new broker instance with persistent storage
func NewBrokerWithStorage(diskStorage *storage.DiskStorage, wal *storage.WAL, offsetManager *offset.Manager) *Broker {
	b := &Broker{
		topicManager:   topic.NewTopicManager(),
		storage:        diskStorage,
		consumers:      make(map[string]*ConsumerGroup),
		wal:            wal,
		offsetManager:  offsetManager,
		coordinator:    partition.NewConsumerCoordinator(),
		stopChan:       make(chan struct{}),
		isStopped:      false,
	}
	cluster.RegisterBroker(b)

	// Perform recovery if WAL exists
	if wal != nil {
		if err := b.performRecovery(); err != nil {
			log.Printf("Warning: Recovery failed: %v", err)
		}
	}

	// Start dynamic rebalance loop
	go b.rebalanceLoop()

	return b
}

// performRecovery recovers broker state from WAL
func (b *Broker) performRecovery() error {
	log.Println("Starting recovery from WAL...")
	entries, err := b.wal.Recovery()
	if err != nil {
		return fmt.Errorf("failed to read WAL for recovery: %v", err)
	}
	log.Printf("Found %d WAL entries to replay", len(entries))
	for _, entry := range entries {
		switch entry.Type {
		case "topic_create":
			if entry.Config == nil {
				entry.Config = &topic.TopicConfig{
					NumPartitions:     1,
					ReplicationFactor: 1,
				}
			}
			if err := b.CreateTopic(entry.Topic, entry.Config, false); err != nil {
				log.Printf("Warning: Failed to recover topic %s: %v", entry.Topic, err)
			} else {
				log.Printf("Recovered topic: %s with %d partitions", entry.Topic, entry.Config.NumPartitions)
			}
		case "message":
			if entry.Message != nil {
				if err := b.storage.Store(entry.Message); err != nil {
					log.Printf("Warning: Failed to replay message: %v", err)
				} else {
					log.Printf("Recovered message for topic %s, partition %d, offset %d", entry.Topic, entry.Partition, entry.Message.Offset)
				}
			}
		}
	}
	// Optionally, reload topics from disk if needed (for test/production parity)
	if diskStorage, ok := b.storage.(*storage.DiskStorage); ok {
		topics := diskStorage.ListTopics()
		for _, t := range topics {
			if !b.topicManager.TopicExists(t) {
				cfg, err := diskStorage.ReadTopicConfig(t)
				if err == nil {
					_ = b.CreateTopic(t, cfg, false)
				}
			}
		}
	}
	log.Println("Recovery completed successfully")
	return nil
}

// SetCluster sets the cluster manager for this broker
func (b *Broker) SetCluster(cluster *cluster.Cluster) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cluster = cluster
	b.replicationMgr = replication.NewReplicationManager(cluster.GetNodeID(), b.storage, b.wal, cluster)
	
	// Start background tasks for replication
	go b.reportLoadMetrics()
}

// CreateTopic creates a new topic
func (b *Broker) CreateTopic(name string, config *topic.TopicConfig, replicate bool) error {
	log.Printf("[DEBUG] CreateTopic called: name=%s, config=%+v, replicate=%v", name, config, replicate)
	if b.cluster != nil {
		if !b.cluster.IsLeader() {
			return fmt.Errorf("not the Raft leader; forward to leader")
		}
	}
	// Idempotency: Check if topic already exists
	if b.topicManager.TopicExists(name) {
		log.Printf("[DEBUG] Topic %s already exists", name)
		return fmt.Errorf("topic %s already exists", name)
	}

	// If in cluster mode, wait for cluster to be ready before proceeding
	if b.cluster != nil {
		if err := b.waitForClusterReady(); err != nil {
			return fmt.Errorf("cluster not ready: %v", err)
		}
	}

	// If in cluster mode and not controller, forward request to controller
	if b.cluster != nil && !b.cluster.IsController() {
		controllerAddr := b.cluster.GetBrokerAddress(b.cluster.GetLeader())
		if controllerAddr == "" {
			return fmt.Errorf("controller address not found")
		}
		url := fmt.Sprintf("http://%s/topics/%s", controllerAddr, name)
		data, err := json.Marshal(map[string]interface{}{
			"num_partitions":     config.NumPartitions,
			"replication_factor": config.ReplicationFactor,
		})
		if err != nil {
			return fmt.Errorf("failed to marshal topic config: %v", err)
		}
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
		if err != nil {
			return fmt.Errorf("failed to create HTTP request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Replication-Origin", "true")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to forward topic creation to controller: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			return fmt.Errorf("controller returned error status %d", resp.StatusCode)
		}
		return nil
	}

	// Only controller assigns partitions and stores metadata
	if b.cluster != nil && b.cluster.IsController() {
		// Get all brokers in the cluster (including self)
		peers := b.cluster.GetPeers()
		brokerIDs := make([]string, 0, len(peers)+1)
		brokerIDs = append(brokerIDs, b.cluster.GetNodeID())
		for id := range peers {
			if id != b.cluster.GetNodeID() {
				brokerIDs = append(brokerIDs, id)
			}
		}
		// Assign partitions round-robin across brokers
		for i := 0; i < config.NumPartitions; i++ {
			leaderIdx := i % len(brokerIDs)
			leader := brokerIDs[leaderIdx]
			replicas := make([]string, 0, config.ReplicationFactor)
			for r := 0; r < config.ReplicationFactor; r++ {
				replicaIdx := (leaderIdx + r) % len(brokerIDs)
				replicas = append(replicas, brokerIDs[replicaIdx])
			}
			assignment := &cluster.PartitionAssignment{
				Leader:   leader,
				Replicas: replicas,
			}
			b.cluster.StorePartitionAssignment(name, i, assignment)
		}
		meta := &cluster.TopicMetadata{
			Name:       name,
			Partitions: config.NumPartitions,
			Replicas:   config.ReplicationFactor,
		}
		b.cluster.StoreTopicMetadata(name, meta)
		// Sync assignments to all brokers
		b.syncPartitionAssignmentsToBrokers(name)
	}

	if err := b.topicManager.CreateTopic(name, config); err != nil {
		log.Printf("[DEBUG] Failed to create topic %s: %v", name, err)
		metrics.ErrorsTotal.WithLabelValues("create_topic_failed").Inc()
		return err
	}
	log.Printf("[DEBUG] Topic %s created successfully with config: %+v", name, config)

	// Write topic creation to WAL if available
	if b.wal != nil {
		err := b.wal.WriteTopicCreate(name, config)
		if err != nil {
			log.Printf("Warning: Failed to write topic creation to WAL: %v", err)
		}
	}

	// Update metrics
	metrics.TopicPartitions.WithLabelValues(name).Set(float64(config.NumPartitions))

	// If we're in a cluster, replicate topic creation to other brokers
	if b.cluster != nil && replicate && b.cluster.IsController() {
		peers := b.cluster.GetPeers()
		for peerID, peer := range peers {
			if peerID == b.cluster.GetNodeID() {
				continue // Skip self
			}
			brokerAddr := fmt.Sprintf("%s:%s", peer.Address, peer.Port)
			if brokerAddr == "" {
				log.Printf("Warning: Could not find address for broker %s", peerID)
				continue
			}
			url := fmt.Sprintf("http://%s/topics/%s", brokerAddr, name)
			data, err := json.Marshal(map[string]interface{}{
				"num_partitions":     config.NumPartitions,
				"replication_factor": config.ReplicationFactor,
			})
			if err != nil {
				log.Printf("Warning: Failed to marshal topic config for broker %s: %v", peerID, err)
				continue
			}
			// Add replication header
			req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
			if err != nil {
				log.Printf("Warning: Failed to create HTTP request for broker %s: %v", peerID, err)
				continue
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Replication-Origin", "true")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				log.Printf("Warning: Failed to create topic on broker %s: %v", peerID, err)
				continue
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusCreated {
				log.Printf("Warning: Broker %s returned error status %d for topic creation", peerID, resp.StatusCode)
				continue
			}
			log.Printf("Successfully created topic %s on broker %s", name, peerID)
		}
	}

	if ds, ok := b.storage.(*storage.DiskStorage); ok {
		ds.CreateTopicDirsAndConfig(name, config)
	}

	return nil
}

// waitForClusterReady waits for the cluster to be fully formed and ready
func (b *Broker) waitForClusterReady() error {
	if b.cluster == nil {
		return nil // Not in cluster mode
	}

	log.Printf("Waiting for cluster to be ready...")
	
	// Wait up to 30 seconds for cluster to be ready
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("timeout waiting for cluster to be ready")
		case <-ticker.C:
			// Check if we have a leader
			leader := b.cluster.GetLeader()
			if leader == "" {
				log.Printf("Waiting for leader election...")
				continue // No leader yet
			}

			// Check if we have peers
			peers := b.cluster.GetPeers()
			if len(peers) == 0 {
				log.Printf("Waiting for peer discovery... (leader=%s)", leader)
				continue // No peers yet
			}

			// Check if we're the controller and have enough peers for replication
			if b.cluster.IsController() {
				// For replication factor > 1, we need at least that many brokers
				// For now, just ensure we have at least 1 peer
				if len(peers) >= 1 {
					log.Printf("Cluster is ready: leader=%s, peers=%d", leader, len(peers))
					return nil
				} else {
					log.Printf("Waiting for more peers... (leader=%s, peers=%d)", leader, len(peers))
				}
			} else {
				// Non-controller nodes just need to know who the leader is
				log.Printf("Cluster is ready: leader=%s, peers=%d", leader, len(peers))
				return nil
			}
		}
	}
}

// syncPartitionAssignmentsToBrokers notifies all brokers of partition assignments for a topic
func (b *Broker) syncPartitionAssignmentsToBrokers(topic string) {
	if b.cluster == nil {
		return
	}
	assignmentsMap := b.cluster.GetPartitionAssignments()
	assignments := assignmentsMap[topic]
	if assignments == nil {
		return
	}
	peers := b.cluster.GetPeers()
	for peerID, peer := range peers {
		url := fmt.Sprintf("http://%s:%s/internal/assignments/%s", peer.Address, peer.Port, topic)
		data, err := json.Marshal(assignments)
		if err != nil {
			log.Printf("Failed to marshal assignments for %s: %v", peerID, err)
			continue
		}
		go func(url string, data []byte, peerID string) {
			resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
			if err != nil {
				log.Printf("Failed to send assignments to %s: %v", peerID, err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				log.Printf("Broker %s returned error status %d for assignment update", peerID, resp.StatusCode)
			}
		}(url, data, peerID)
	}
	// Also update self (controller) with assignments
	stringAssignments := make(map[string]*cluster.PartitionAssignment, len(assignments))
	for pid, assignment := range assignments {
		stringAssignments[fmt.Sprintf("%d", pid)] = assignment
	}
	b.HandleAssignmentUpdate(topic, stringAssignments)
}

// HandleAssignmentUpdate handles assignment updates from the controller
func (b *Broker) HandleAssignmentUpdate(topics string, assignments map[string]*cluster.PartitionAssignment) {
	if b.replicationMgr == nil {
		return
	}

	// Ensure the topic exists locally
	if !b.topicManager.TopicExists(topics) {
		// Try to fetch topic config from controller if in cluster
		if b.cluster != nil {
			controllerAddr := b.cluster.GetBrokerAddress(b.cluster.GetLeader())
			if controllerAddr != "" {
				url := fmt.Sprintf("http://%s/topics/%s/metadata", controllerAddr, topics)
				resp, err := http.Get(url)
				if err == nil && resp.StatusCode == http.StatusOK {
					defer resp.Body.Close()
					var respData map[string]interface{}
					if err := json.NewDecoder(resp.Body).Decode(&respData); err == nil {
						if configData, ok := respData["config"]; ok {
							configBytes, _ := json.Marshal(configData)
							var config topic.TopicConfig
							if err := json.Unmarshal(configBytes, &config); err == nil {
								_ = b.topicManager.CreateTopic(topics, &config)
								if ds, ok := b.storage.(*storage.DiskStorage); ok {
									ds.CreateTopicDirsAndConfig(topics, &config)
								}
								if b.wal != nil {
									_ = b.wal.WriteTopicCreate(topics, &config)
								}
							}
						}
					}
				}
			}
		}
	}

	for partitionStr, assignment := range assignments {
		var partition int
		fmt.Sscanf(partitionStr, "%d", &partition)
		b.replicationMgr.SetLeader(topics, partition, assignment.Leader)
		b.replicationMgr.SetFollowers(topics, partition, assignment.Replicas)
	}
}

// CreateTopicReplicated is used by HTTP handler to create a topic without further replication
func (b *Broker) CreateTopicReplicated(name string, config *topic.TopicConfig) error {
	log.Printf("[DEBUG] CreateTopicReplicated called: name=%s, config=%+v", name, config)
	return b.CreateTopic(name, config, false)
}

// DeleteTopic deletes a topic
func (b *Broker) DeleteTopic(name string) error {
	if b.cluster != nil {
		if !b.cluster.IsLeader() {
			return fmt.Errorf("not the Raft leader; forward to leader")
		}
		b.cluster.DeleteTopic(name)
	}
	return b.topicManager.DeleteTopic(name)
}

// ListTopics returns all topic names
func (b *Broker) ListTopics() []string {
	return b.topicManager.ListTopics()
}

// GetTopic returns a topic by name
func (b *Broker) GetTopic(name string) (*topic.Topic, error) {
	return b.topicManager.GetTopic(name)
}

// Produce sends a message to a topic
func (b *Broker) Produce(topicName string, key string, value []byte, headers map[string]string) (*message.Message, error) {
	start := time.Now()
	defer func() {
		metrics.ProduceLatency.WithLabelValues(topicName, fmt.Sprintf("%d", b.selectPartition(key, 1))).Observe(time.Since(start).Seconds())
	}()

	// Get or create topic
	topicObj, err := b.topicManager.GetTopic(topicName)
	if err != nil {
		metrics.ErrorsTotal.WithLabelValues("topic_not_found").Inc()
		return nil, fmt.Errorf("topic not found: %w", err)
	}

	// Determine partition (simple hash-based partitioning)
	partitionID := b.selectPartition(key, topicObj.GetPartitionCount())

	// Check if we're the leader for this partition
	if b.cluster != nil && !b.replicationMgr.IsLeader(topicName, partitionID) {
		leader := b.replicationMgr.GetLeader(topicName, partitionID)
		leaderAddr := ""
		if b.cluster != nil {
			leaderAddr = b.cluster.GetBrokerAddress(leader)
		}
		metrics.ErrorsTotal.WithLabelValues("not_leader").Inc()
		return nil, fmt.Errorf("not leader for partition %d; leader is %s at %s", partitionID, leader, leaderAddr)
	}

	// Get next offset for the partition
	offset, err := topicObj.GetNextOffset(partitionID)
	if err != nil {
		metrics.ErrorsTotal.WithLabelValues("get_offset_failed").Inc()
		return nil, fmt.Errorf("failed to get next offset: %w", err)
	}

	// Create message
	msg := message.NewMessage(topicName, partitionID, key, value, headers)
	msg.Offset = offset

	// Store message
	if err := b.storage.Store(msg); err != nil {
		metrics.ErrorsTotal.WithLabelValues("store_failed").Inc()
		return nil, fmt.Errorf("failed to store message: %w", err)
	}

	// Log to WAL if enabled
	if b.wal != nil {
		if err := b.wal.WriteMessage(topicName, int32(partitionID), msg); err != nil {
			metrics.ErrorsTotal.WithLabelValues("wal_write_failed").Inc()
			log.Printf("Warning: Failed to write message to WAL: %v", err)
		} else {
			metrics.WALOperations.WithLabelValues("write").Inc()
		}
	}

	// Replicate to followers if we're in a cluster
	if b.cluster != nil {
		if err := b.replicationMgr.ReplicateMessage(msg); err != nil {
			metrics.ErrorsTotal.WithLabelValues("replication_failed").Inc()
			log.Printf("Warning: Failed to replicate message: %v", err)
		}
	}

	// Update metrics
	metrics.MessagesProduced.WithLabelValues(topicName, fmt.Sprintf("%d", partitionID)).Inc()
	metrics.StorageSize.WithLabelValues(topicName, fmt.Sprintf("%d", partitionID)).Add(float64(len(value)))

	log.Printf("Produced message: topic=%s, partition=%d, offset=%d, key=%s", 
		topicName, partitionID, offset, key)

	return msg, nil
}

// ProduceToPartition sends a message to a specific partition in a topic
func (b *Broker) ProduceToPartition(topicName string, partition int, key string, value []byte, headers map[string]string) (*message.Message, error) {
	// Get or create topic
	topicObj, err := b.topicManager.GetTopic(topicName)
	if err != nil {
		return nil, fmt.Errorf("topic not found: %w", err)
	}

	// Verify partition exists
	if partition < 0 || partition >= topicObj.GetPartitionCount() {
		return nil, fmt.Errorf("invalid partition %d for topic %s", partition, topicName)
	}

	// Check if we're the leader for this partition
	if b.cluster != nil && !b.replicationMgr.IsLeader(topicName, partition) {
		leader := b.replicationMgr.GetLeader(topicName, partition)
		leaderAddr := ""
		if b.cluster != nil {
			leaderAddr = b.cluster.GetBrokerAddress(leader)
		}
		return nil, fmt.Errorf("not leader for partition %d; leader is %s at %s", partition, leader, leaderAddr)
	}

	// Get next offset for the partition
	offset, err := topicObj.GetNextOffset(partition)
	if err != nil {
		return nil, fmt.Errorf("failed to get next offset: %w", err)
	}

	// Create message
	msg := message.NewMessage(topicName, partition, key, value, headers)
	msg.Offset = offset

	// Store message
	if err := b.storage.Store(msg); err != nil {
		return nil, fmt.Errorf("failed to store message: %w", err)
	}

	// Log to WAL if enabled
	if b.wal != nil {
		if err := b.wal.WriteMessage(topicName, int32(partition), msg); err != nil {
			log.Printf("Warning: Failed to write message to WAL: %v", err)
		}
	}

	// Replicate to followers if we're in a cluster
	if b.cluster != nil {
		if err := b.replicationMgr.ReplicateMessage(msg); err != nil {
			log.Printf("Warning: Failed to replicate message: %v", err)
		}
	}

	log.Printf("Produced message to partition: topic=%s, partition=%d, offset=%d, key=%s", 
		topicName, partition, offset, key)

	return msg, nil
}

// Consume retrieves messages from a topic starting from a given offset
func (b *Broker) Consume(topicName string, partition int, offset int64, limit int) ([]*message.Message, error) {
	start := time.Now()
	defer func() {
		metrics.ConsumeLatency.WithLabelValues(topicName, fmt.Sprintf("%d", partition)).Observe(time.Since(start).Seconds())
	}()

	// Verify topic exists
	if !b.topicManager.TopicExists(topicName) {
		metrics.ErrorsTotal.WithLabelValues("topic_not_found").Inc()
		return nil, fmt.Errorf("topic %s does not exist", topicName)
	}

	// Get messages from storage
	messages, err := b.storage.GetMessages(topicName, partition, offset, limit)
	if err != nil {
		metrics.ErrorsTotal.WithLabelValues("retrieve_failed").Inc()
		return nil, fmt.Errorf("failed to retrieve messages: %w", err)
	}

	// Update metrics
	metrics.MessagesConsumed.WithLabelValues(topicName, fmt.Sprintf("%d", partition)).Add(float64(len(messages)))

	log.Printf("Consumed %d messages: topic=%s, partition=%d, start_offset=%d", 
		len(messages), topicName, partition, offset)

	return messages, nil
}

// GetMessage retrieves a specific message by topic, partition, and offset
func (b *Broker) GetMessage(topicName string, partition int, offset int64) (*message.Message, error) {
	return b.storage.GetMessage(topicName, partition, offset)
}

// GetTopicMetadata returns metadata about a topic
func (b *Broker) GetTopicMetadata(topicName string) (*TopicMetadata, error) {
	topicObj, err := b.topicManager.GetTopic(topicName)
	if err != nil {
		return nil, err
	}

	metadata := &TopicMetadata{
		Name:       topicName,
		Partitions: make([]PartitionMetadata, 0),
	}

	for partitionID := range topicObj.Partitions {
		partition, _ := topicObj.GetPartition(partitionID)
		hwm := b.storage.GetHighWaterMark(topicName, partitionID)
		
		// Get leader information if in cluster
		var leaderID string
		if b.cluster != nil {
			leaderID = b.replicationMgr.GetLeader(topicName, partitionID)
		}
		
		metadata.Partitions = append(metadata.Partitions, PartitionMetadata{
			ID:           partitionID,
			HighWaterMark: hwm,
			MessageCount: partition.MessageCount,
			LeaderID:     leaderID,
		})
	}

	return metadata, nil
}

// CreateConsumerGroup creates a new consumer group
func (b *Broker) CreateConsumerGroup(groupID string) *ConsumerGroup {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.consumers[groupID]; !exists {
		b.consumers[groupID] = &ConsumerGroup{
			ID:        groupID,
			Consumers: make(map[string]*Consumer),
			Offsets:   make(map[string]int64),
		}
	}

	return b.consumers[groupID]
}

// AddConsumerToGroup adds a consumer to a consumer group
func (b *Broker) AddConsumerToGroup(groupID, consumerID string, topics []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	group, exists := b.consumers[groupID]
	if !exists {
		group = b.CreateConsumerGroup(groupID)
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	group.Consumers[consumerID] = &Consumer{
		ID:      consumerID,
		GroupID: groupID,
		Topics:  topics,
	}

	log.Printf("Added consumer %s to group %s for topics %v", consumerID, groupID, topics)
	return nil
}

// CommitOffset commits an offset for a consumer group and partition
func (b *Broker) CommitOffset(groupID, topic string, partition int, offset int64) error {
	if b.offsetManager != nil {
		return b.offsetManager.CommitOffset(groupID, topic, int32(partition), offset)
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	group, exists := b.consumers[groupID]
	if !exists {
		// Auto-create the consumer group if it does not exist
		group = &ConsumerGroup{
			ID:        groupID,
			Consumers: make(map[string]*Consumer),
			Offsets:   make(map[string]int64),
		}
		b.consumers[groupID] = group
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	partitionKey := fmt.Sprintf("%s-%d", topic, partition)
	group.Offsets[partitionKey] = offset

	log.Printf("Committed offset: group=%s, topic=%s, partition=%d, offset=%d",
		groupID, topic, partition, offset)

	return nil
}

// GetConsumerGroupOffset gets the committed offset for a consumer group and partition
func (b *Broker) GetConsumerGroupOffset(groupID, topic string, partition int) int64 {
	if b.offsetManager != nil {
		return b.offsetManager.GetOffset(groupID, topic, int32(partition))
	}
	b.mu.RLock()
	defer b.mu.RUnlock()

	group, exists := b.consumers[groupID]
	if !exists {
		return 0 // Start from beginning if group doesn't exist
	}

	group.mu.RLock()
	defer group.mu.RUnlock()

	partitionKey := fmt.Sprintf("%s-%d", topic, partition)
	offset, exists := group.Offsets[partitionKey]
	if !exists {
		return 0 // Start from beginning if no offset committed
	}

	return offset
}

// selectPartition selects a partition for a message based on key
func (b *Broker) selectPartition(key string, numPartitions int) int {
	if key == "" {
		// If no key, distribute round-robin (simplified)
		return 0
	}

	// Hash-based partitioning
	hasher := fnv.New32a()
	hasher.Write([]byte(key))
	return int(hasher.Sum32()) % numPartitions
}

// TopicMetadata represents metadata about a topic
type TopicMetadata struct {
	Name       string              `json:"name"`
	Partitions []PartitionMetadata `json:"partitions"`
}

// PartitionMetadata represents metadata about a partition
type PartitionMetadata struct {
	ID            int    `json:"id"`
	HighWaterMark int64  `json:"high_water_mark"`
	MessageCount  int64  `json:"message_count"`
	LeaderID      string `json:"leader_id,omitempty"`
}

// StoreMessage stores a message directly (used during recovery)
func (b *Broker) StoreMessage(msg *message.Message) error {
	return b.storage.Store(msg)
}

// RegisterConsumer registers a consumer with a consumer group
func (b *Broker) RegisterConsumer(groupID, consumerID string, topics []string) error {
	err := b.coordinator.RegisterConsumer(groupID, consumerID, topics)
	if err == nil {
		go b.RebalanceConsumerGroup(groupID)
	}
	return err
}

// UnregisterConsumer removes a consumer from a consumer group
func (b *Broker) UnregisterConsumer(groupID, consumerID string) error {
	err := b.coordinator.UnregisterConsumer(groupID, consumerID)
	if err == nil {
		go b.RebalanceConsumerGroup(groupID)
	}
	return err
}

// GetConsumerAssignments returns the partition assignments for a consumer
func (b *Broker) GetConsumerAssignments(groupID, consumerID string) (map[string][]int, error) {
	// Get assignments from coordinator
	assignments, err := b.coordinator.GetConsumerAssignments(groupID, consumerID)
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer assignments: %w", err)
	}

	return assignments, nil
}

// GetGroupMetadata returns metadata about a consumer group
func (b *Broker) GetGroupMetadata(groupID string) (*ConsumerGroupMetadata, error) {
	// Get metadata from coordinator
	metadata, err := b.coordinator.GetGroupMetadata(groupID)
	if err != nil {
		return nil, fmt.Errorf("failed to get group metadata: %w", err)
	}

	// Convert coordinator metadata to broker metadata
	brokerMetadata := &ConsumerGroupMetadata{
		ID:        metadata.ID,
		Consumers: make([]ConsumerMetadata, len(metadata.Consumers)),
	}

	for i, consumer := range metadata.Consumers {
		// Get topics from the broker's consumer group
		var topics []string
		if group, exists := b.consumers[groupID]; exists {
			if c, exists := group.Consumers[consumer.ID]; exists {
				topics = c.Topics
			}
		}

		brokerMetadata.Consumers[i] = ConsumerMetadata{
			ID:      consumer.ID,
			GroupID: groupID,
			Topics:  topics,
		}
	}

	return brokerMetadata, nil
}

// ConsumerGroupMetadata represents metadata about a consumer group
type ConsumerGroupMetadata struct {
	ID        string             `json:"id"`
	Consumers []ConsumerMetadata `json:"consumers"`
}

// ConsumerMetadata represents metadata about a consumer
type ConsumerMetadata struct {
	ID      string   `json:"id"`
	GroupID string   `json:"group_id"`
	Topics  []string `json:"topics"`
}

// TopicExists checks if a topic exists
func (b *Broker) TopicExists(name string) bool {
	return b.topicManager.TopicExists(name)
}

// HandlePartitionReassignment handles a partition reassignment request
func (b *Broker) HandlePartitionReassignment(topic string, partition int, newLeader string, replicas []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	t, err := b.topicManager.GetTopic(topic)
	if err != nil {
		return fmt.Errorf("topic %s does not exist", topic)
	}

	_, err = t.GetPartition(partition)
	if err != nil {
		return fmt.Errorf("partition %d does not exist in topic %s", partition, topic)
	}

	// Update partition assignment
	if b.replicationMgr != nil {
		b.replicationMgr.ReassignPartition(topic, partition, newLeader, replicas)
	}

	// Optionally, store leader/replica info in a metadata map if needed
	// (Partition struct does not have leader/replicas fields)
	// For now, just log
	log.Printf("Partition reassignment completed for %s-%d: leader=%s, replicas=%v",
		topic, partition, newLeader, replicas)

	return nil
}

// GetBrokerLoad returns the current load metrics for this broker
func (b *Broker) GetBrokerLoad() *cluster.BrokerLoad {
	b.mu.RLock()
	defer b.mu.RUnlock()

	load := &cluster.BrokerLoad{
		NodeID: b.cluster.GetNodeID(),
	}

	// Count partitions and leaders
	topics := b.topicManager.ListTopics()
	for _, topicName := range topics {
		topicObj, err := b.topicManager.GetTopic(topicName)
		if err != nil {
			continue
		}
		for partitionID := range topicObj.Partitions {
			load.PartitionCount++
			// Check if we're the leader for this partition
			if b.cluster != nil && b.replicationMgr != nil {
				if b.replicationMgr.IsLeader(topicName, partitionID) {
					load.LeaderCount++
				} else {
					load.FollowerCount++
				}
			}
		}
	}

	// TODO: Add actual CPU and memory usage metrics
	// For now, use dummy values
	load.CPUUsage = 0.0
	load.MemoryUsage = 0.0

	return load
}

// reportLoadMetrics periodically reports load metrics to the cluster
func (b *Broker) reportLoadMetrics() {
	ticker := time.NewTicker(time.Minute) // Increased from 30s to 1 minute
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if b.cluster != nil {
				load := b.GetBrokerLoad()
				b.cluster.UpdateBrokerLoad(b.cluster.GetNodeID(), load)

				// Update Prometheus metrics
				metrics.CPUUsage.Set(load.CPUUsage)
				metrics.MemoryUsage.Set(float64(load.MemoryUsage))
			}
		case <-b.stopChan:
			return
		}
	}
}

// Start starts the broker
func (b *Broker) Start() error {
	// Start load metrics reporting if in cluster mode
	if b.cluster != nil {
		go b.reportLoadMetrics()
		// Start periodic assignment fetch if not controller
		if !b.cluster.IsController() {
			go b.periodicAssignmentFetch()
		}
	}
	// Start follower replication loop
	if b.replicationMgr != nil {
		b.replicationMgr.StartFollowerReplicationLoop(b.fetchFromLeader)
	}

	// Start any other background tasks here
	// For example, partition rebalancing, consumer group coordination, etc.

	return nil
}

// periodicAssignmentFetch periodically fetches partition assignments from the controller
func (b *Broker) periodicAssignmentFetch() {
	for {
		time.Sleep(60 * time.Second) // Increased from 30s to 60s
		if b.cluster == nil || b.cluster.IsController() {
			continue
		}
		controllerAddr := b.cluster.GetBrokerAddress(b.cluster.GetLeader())
		if controllerAddr == "" {
			continue
		}
		url := fmt.Sprintf("http://%s/internal/assignments/all", controllerAddr)
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("Failed to fetch assignments from controller: %v", err)
			continue
		}
		var allAssignments map[string]map[string]*cluster.PartitionAssignment
		err = json.NewDecoder(resp.Body).Decode(&allAssignments)
		resp.Body.Close()
		if err != nil {
			log.Printf("Failed to decode assignments from controller: %v", err)
			continue
		}
		for topic, assignments := range allAssignments {
			b.HandleAssignmentUpdate(topic, assignments)
		}
	}
}

// fetchFromLeader fetches messages from the leader for a partition
func (b *Broker) fetchFromLeader(topic string, partition int, offset int64, leaderID string) ([]*message.Message, error) {
	leaderAddr := b.cluster.GetBrokerAddress(leaderID)
	if leaderAddr == "" {
		return nil, fmt.Errorf("leader address not found")
	}
	url := fmt.Sprintf("http://%s/topics/%s/partitions/%d/consume?offset=%d&limit=100", leaderAddr, topic, partition, offset)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("leader returned status %d", resp.StatusCode)
	}
	var result struct {
		Messages []struct {
			ID        string            `json:"id"`
			Key       string            `json:"key"`
			Value     string            `json:"value"`
			Headers   map[string]string `json:"headers"`
			Timestamp string            `json:"timestamp"`
			Offset    int64             `json:"offset"`
			Partition int               `json:"partition"`
			Topic     string            `json:"topic"`
		} `json:"messages"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	msgs := make([]*message.Message, 0, len(result.Messages))
	for _, m := range result.Messages {
		msg := &message.Message{
			ID:        m.ID,
			Key:       m.Key,
			Value:     []byte(m.Value),
			Headers:   m.Headers,
			Timestamp: time.Now(), // Could parse m.Timestamp
			Offset:    m.Offset,
			Partition: m.Partition,
			Topic:     m.Topic,
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

// Stop stops the broker and cleans up resources
func (b *Broker) Stop() error {
	b.mu.Lock()
	if b.isStopped {
		b.mu.Unlock()
		return nil
	}
	b.isStopped = true
	b.mu.Unlock()

	log.Println("Stopping broker...")

	// Stop load metrics reporting
	select {
	case <-b.stopChan:
		// Channel already closed
	default:
		close(b.stopChan)
	}

	// Stop all consumer groups
	// Collect all group/consumer IDs first
	var toUnregister [][2]string
	b.mu.RLock()
	for groupID, group := range b.consumers {
		group.mu.RLock()
		for consumerID := range group.Consumers {
			toUnregister = append(toUnregister, [2]string{groupID, consumerID})
		}
		group.mu.RUnlock()
	}
	b.mu.RUnlock()

	// Now unregister without holding locks
	for _, pair := range toUnregister {
		groupID, consumerID := pair[0], pair[1]
		if err := b.UnregisterConsumer(groupID, consumerID); err != nil {
			log.Printf("Error unregistering consumer %s from group %s: %v", consumerID, groupID, err)
		}
	}

	// Stop coordinator
	if b.coordinator != nil {
		if err := b.coordinator.Stop(); err != nil {
			log.Printf("Error stopping coordinator: %v", err)
		}
	}

	// Stop replication manager if in cluster mode
	if b.replicationMgr != nil {
		if err := b.replicationMgr.Stop(); err != nil {
			log.Printf("Error stopping replication manager: %v", err)
		}
	}

	log.Println("Broker stopped successfully")
	return nil
}

// rebalanceLoop periodically checks for imbalance and triggers rebalance
func (b *Broker) rebalanceLoop() {
	ticker := time.NewTicker(30 * time.Second) // Increased from 10s to 30s
	defer ticker.Stop()
	for {
		select {
		case <-b.stopChan:
			return
		case <-ticker.C:
			b.mu.RLock()
			for groupID := range b.consumers {
				b.RebalanceConsumerGroup(groupID)
			}
			b.mu.RUnlock()
		}
	}
}

// RebalanceConsumerGroup triggers a rebalance for a consumer group based on metrics
func (b *Broker) RebalanceConsumerGroup(groupID string) {
	metrics := b.coordinator.GetGroupMetrics(groupID)
	if metrics == nil {
		return
	}

	// Heuristic: Only rebalance if lag/throughput is highly imbalanced or a consumer is dead
	const lagImbalanceThreshold = 2.0      // max lag is 2x min lag
	const throughputImbalanceThreshold = 2.0 // max throughput is 2x min throughput
	const deadConsumerTimeout = 10          // seconds

	shouldRebalance := false

	// Check for dead consumers
	now := time.Now().Unix()
	for _, lastSeen := range metrics.ConsumerHealth {
		if now-lastSeen > deadConsumerTimeout {
			shouldRebalance = true
			break
		}
	}

	// Check lag imbalance
	if !shouldRebalance {
		for _, partitionLag := range metrics.ConsumerLag {
			if len(partitionLag) < 2 {
				continue
			}
			var minLag, maxLag int64
			minLag = math.MaxInt64
			maxLag = math.MinInt64
			for _, lag := range partitionLag {
				if lag < minLag {
					minLag = lag
				}
				if lag > maxLag {
					maxLag = lag
				}
			}
			if minLag > 0 && float64(maxLag)/float64(minLag) > lagImbalanceThreshold {
				shouldRebalance = true
				break
			}
		}
	}

	// Check throughput imbalance
	if !shouldRebalance {
		for _, partitionThroughput := range metrics.ConsumerThroughput {
			if len(partitionThroughput) < 2 {
				continue
			}
			var minT, maxT float64
			minT = math.MaxFloat64
			maxT = -1.0
			for _, t := range partitionThroughput {
				if t < minT {
					minT = t
				}
				if t > maxT {
					maxT = t
				}
			}
			if minT > 0 && maxT/minT > throughputImbalanceThreshold {
				shouldRebalance = true
				break
			}
		}
	}

	if shouldRebalance {
		b.coordinator.RebalanceGroupByID(groupID)
	}
}

// ReplicationManager returns the broker's replication manager
func (b *Broker) ReplicationManager() *replication.ReplicationManager {
	return b.replicationMgr
}

// Implement BrokerObserver methods
func (b *Broker) OnTopicCreated(meta *cluster.TopicMetadata) {
	cfg := &topic.TopicConfig{
		NumPartitions:     meta.Partitions,
		ReplicationFactor: meta.Replicas,
	}
	_ = b.topicManager.CreateTopic(meta.Name, cfg)
}

func (b *Broker) OnTopicDeleted(topicName string) {
	_ = b.topicManager.DeleteTopic(topicName)
}

func (b *Broker) OnPartitionAssigned(topic string, partition int, assignment *cluster.PartitionAssignment) {
	if b.replicationMgr != nil {
		b.replicationMgr.SetLeader(topic, partition, assignment.Leader)
		b.replicationMgr.SetFollowers(topic, partition, assignment.Replicas)
	}
}

// handleRaftJoin handles /internal/raft-join requests for Raft cluster join
func (b *Broker) HandleRaftJoin(w http.ResponseWriter, r *http.Request) {
	if b.cluster == nil || !b.cluster.IsLeader() {
		http.Error(w, "Not the leader", http.StatusBadRequest)
		return
	}

	var joinRequest struct {
		NodeID   string `json:"node_id"`
		RaftAddr string `json:"raft_addr"`
	}

	if err := json.NewDecoder(r.Body).Decode(&joinRequest); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err := b.cluster.AddBrokerToRaft(joinRequest.NodeID, joinRequest.RaftAddr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}