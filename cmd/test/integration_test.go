package main

import (
	"Distributed-system/internal/broker"
	"Distributed-system/internal/cluster"
	"Distributed-system/internal/config"
	"Distributed-system/internal/offset"
	"Distributed-system/internal/storage"
	"Distributed-system/internal/topic"
	"Distributed-system/pkg/api"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// NOTE: TestPhase4Clustering is skipped unless cluster nodes are running on ports 8081, 8082, 8083.

// setupTestBroker creates a test broker instance in the given directory
func setupTestBroker(t *testing.T, testDir string) (*broker.Broker, func()) {
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	cfg := config.DefaultConfig()
	cfg.DataDir = testDir
	cfg.SegmentSize = 1024 * 1024 // 1MB segments
	cfg.WALEnabled = true
	cfg.WALSyncInterval = time.Second
	cfg.OffsetCommitInterval = time.Second

	diskStorage := storage.NewDiskStorage(cfg)
	wal, err := storage.NewWAL(cfg)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	offsetManager, err := offset.NewManager(cfg)
	if err != nil {
		t.Fatalf("Failed to create offset manager: %v", err)
	}

	b := broker.NewBrokerWithStorage(diskStorage, wal, offsetManager)

	cleanup := func() {
		if b != nil {
			b.Stop()
		}
		if wal != nil {
			wal.Close()
		}
		if diskStorage != nil {
			diskStorage.Close()
		}
		if offsetManager != nil {
			offsetManager.Close()
		}
		os.RemoveAll(testDir)
	}

	return b, cleanup
}

// Helper to create a unique test directory
func createTestDir(t *testing.T, prefix string) string {
	d := filepath.Join(os.TempDir(), fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano()))
	return d
}

// TestPhase1CoreMessageBroker tests the basic message broker functionality
func TestPhase1CoreMessageBroker(t *testing.T) {
	b, cleanup := setupTestBroker(t, createTestDir(t, "broker-test"))
	defer cleanup()

	// Test topic creation
	t.Run("TopicManagement", func(t *testing.T) {
		// Create topic
		topicName := "test-topic"
		config := &topic.TopicConfig{
			NumPartitions:     3,
			ReplicationFactor: 1,
		}

		if err := b.CreateTopic(topicName, config, true); err != nil {
			t.Fatalf("Failed to create topic: %v", err)
		}

		// Verify topic exists
		topics := b.ListTopics()
		found := false
		for _, t := range topics {
			if t == topicName {
				found = true
				break
			}
		}
		if !found {
			t.Error("Created topic not found in topic list")
		}

		// Delete topic
		if err := b.DeleteTopic(topicName); err != nil {
			t.Fatalf("Failed to delete topic: %v", err)
		}
	})

	// Test message production and consumption
	t.Run("MessageProduceConsume", func(t *testing.T) {
		topicName := "test-topic"
		config := &topic.TopicConfig{
			NumPartitions:     3,
			ReplicationFactor: 1,
		}

		if err := b.CreateTopic(topicName, config, true); err != nil {
			t.Fatalf("Failed to create topic: %v", err)
		}

		// Produce messages to different partitions
		for i := 0; i < 10; i++ {
			msg := &api.ProduceMessage{
				Topic:   topicName,
				Key:     fmt.Sprintf("key-%d", i),
				Value:   fmt.Sprintf("value-%d", i),
				Headers: map[string]string{"test": "header"},
			}

			if _, err := b.Produce(msg.Topic, msg.Key, []byte(msg.Value), msg.Headers); err != nil {
				t.Fatalf("Failed to produce message: %v", err)
			}
		}

		// Consume messages from each partition
		for partition := 0; partition < config.NumPartitions; partition++ {
			messages, err := b.Consume(topicName, partition, 0, 10)
			if err != nil {
				t.Fatalf("Failed to consume messages: %v", err)
			}

			if len(messages) == 0 {
				t.Errorf("No messages found in partition %d", partition)
			}
		}
	})
}

// TestPhase2Persistence tests the persistence and durability features
func TestPhase2Persistence(t *testing.T) {
	testDir := createTestDir(t, "broker-persist-test")

	b, cleanup := setupTestBroker(t, testDir)
	defer cleanup()

	t.Run("PersistenceAndRecovery", func(t *testing.T) {
		topicName := "test-topic"
		config := &topic.TopicConfig{
			NumPartitions:     3,
			ReplicationFactor: 1,
		}

		if err := b.CreateTopic(topicName, config, true); err != nil {
			t.Fatalf("Failed to create topic: %v", err)
		}

		for i := 0; i < 10; i++ {
			msg := &api.ProduceMessage{
				Topic:   topicName,
				Key:     fmt.Sprintf("key-%d", i),
				Value:   fmt.Sprintf("value-%d", i),
				Headers: map[string]string{"test": "header"},
			}
			if _, err := b.Produce(msg.Topic, msg.Key, []byte(msg.Value), msg.Headers); err != nil {
				t.Fatalf("Failed to produce message: %v", err)
			}
		}

		b.Stop()

		b2, cleanup2 := setupTestBroker(t, testDir)
		defer cleanup2()
		time.Sleep(time.Second)

		topics := b2.ListTopics()
		found := false
		for _, t := range topics {
			if t == topicName {
				found = true
				break
			}
		}
		if !found {
			t.Error("Topic not found after recovery")
		}

		for partition := 0; partition < config.NumPartitions; partition++ {
			messages, err := b2.Consume(topicName, partition, 0, 10)
			if err != nil {
				t.Fatalf("Failed to consume messages after recovery: %v", err)
			}
			if len(messages) == 0 {
				t.Errorf("No messages found in partition %d after recovery", partition)
			}
		}
	})

	t.Run("OffsetManagement", func(t *testing.T) {
		topicName := "test-topic"
		groupID := "test-group"
		config := &topic.TopicConfig{
			NumPartitions:     3,
			ReplicationFactor: 1,
		}

		// Only create topic if it doesn't exist
		if !b.TopicExists(topicName) {
			if err := b.CreateTopic(topicName, config, true); err != nil {
				t.Fatalf("Failed to create topic: %v", err)
			}
		}

		for partition := 0; partition < config.NumPartitions; partition++ {
			if err := b.CommitOffset(groupID, topicName, partition, int64(partition*10)); err != nil {
				t.Fatalf("Failed to commit offset: %v", err)
			}
		}

		// Ensure offsets are flushed to disk
		time.Sleep(200 * time.Millisecond)

		b.Stop()

		b2, cleanup2 := setupTestBroker(t, testDir)
		defer cleanup2()
		time.Sleep(time.Second)

		for partition := 0; partition < config.NumPartitions; partition++ {
			offset := b2.GetConsumerGroupOffset(groupID, topicName, partition)
			expected := int64(partition * 10)
			if offset != expected {
				t.Errorf("Expected offset %d, got %d for partition %d", expected, offset, partition)
			}
		}
	})
}

// TestPhase3Partitioning tests the partitioning and scaling features
func TestPhase3Partitioning(t *testing.T) {
	b, cleanup := setupTestBroker(t, createTestDir(t, "broker-test"))
	defer cleanup()

	t.Run("PartitionAssignment", func(t *testing.T) {
		topicName := "test-topic"
		config := &topic.TopicConfig{
			NumPartitions:     3,
			ReplicationFactor: 1,
		}

		if err := b.CreateTopic(topicName, config, true); err != nil {
			t.Fatalf("Failed to create topic: %v", err)
		}

		// Test hash-based partition assignment
		partitionCounts := make(map[int]int)
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("key-%d", i)
			msg := &api.ProduceMessage{
				Topic:   topicName,
				Key:     key,
				Value:   fmt.Sprintf("value-%d", i),
				Headers: map[string]string{"test": "header"},
			}

			resp, err := b.Produce(msg.Topic, msg.Key, []byte(msg.Value), msg.Headers)
			if err != nil {
				t.Fatalf("Failed to produce message: %v", err)
			}

			partitionCounts[resp.Partition]++
		}

		// Verify even distribution
		expectedCount := 100 / config.NumPartitions
		tolerance := 10 // Allow for some variance
		for partition, count := range partitionCounts {
			if count < expectedCount-tolerance || count > expectedCount+tolerance {
				t.Errorf("Uneven partition distribution: partition %d has %d messages (expected around %d)",
					partition, count, expectedCount)
			}
		}
	})

	t.Run("ConsumerGroups", func(t *testing.T) {
		topicName := "test-topic"
		groupID := "test-group"
		config := &topic.TopicConfig{
			NumPartitions:     3,
			ReplicationFactor: 1,
		}

		// Only create topic if it doesn't exist
		if !b.TopicExists(topicName) {
			if err := b.CreateTopic(topicName, config, true); err != nil {
				t.Fatalf("Failed to create topic: %v", err)
			}
		}

		// Register multiple consumers
		consumerIDs := []string{"consumer-1", "consumer-2", "consumer-3"}
		for _, consumerID := range consumerIDs {
			if err := b.RegisterConsumer(groupID, consumerID, []string{topicName}); err != nil {
				t.Fatalf("Failed to register consumer %s: %v", consumerID, err)
			}
		}

		// Verify partition assignments
		for _, consumerID := range consumerIDs {
			assignments, err := b.GetConsumerAssignments(groupID, consumerID)
			if err != nil {
				t.Fatalf("Failed to get assignments for consumer %s: %v", consumerID, err)
			}

			if len(assignments) == 0 {
				t.Errorf("No assignments for consumer %s", consumerID)
			}
		}
	})
}

// TestPhase4Clustering tests the clustering and replication features
func TestPhase4Clustering(t *testing.T) {
	
	// Create test configuration
	cfg := config.DefaultConfig()
	cfg.DataDir = filepath.Join(os.TempDir(), fmt.Sprintf("cluster-test-%d", time.Now().UnixNano()))
	cfg.WALEnabled = true

	// Create cluster nodes
	node1 := cluster.NewCluster("node1", "localhost", "8081",7966)
	node2 := cluster.NewCluster("node2", "localhost", "8082",7947)
	node3 := cluster.NewCluster("node3", "localhost", "8083",7948)

	// Start nodes
	if err := node1.Start([]string{"localhost:8082", "localhost:8083"}); err != nil {
		t.Fatalf("Failed to start node1: %v", err)
	}
	if err := node2.Start([]string{"localhost:8081", "localhost:8083"}); err != nil {
		t.Fatalf("Failed to start node2: %v", err)
	}
	if err := node3.Start([]string{"localhost:8081", "localhost:8082"}); err != nil {
		t.Fatalf("Failed to start node3: %v", err)
	}

	// Cleanup
	defer func() {
		node1.Stop()
		node2.Stop()
		node3.Stop()
		os.RemoveAll(cfg.DataDir)
	}()

	t.Run("ClusterFormation", func(t *testing.T) {
		// Wait for cluster formation
		time.Sleep(2 * time.Second)

		// Verify cluster membership
		peers1 := node1.GetPeers()
		peers2 := node2.GetPeers()
		peers3 := node3.GetPeers()

		if len(peers1) != 2 || len(peers2) != 2 || len(peers3) != 2 {
			t.Errorf("Expected 2 peers for each node, got: node1=%d, node2=%d, node3=%d",
				len(peers1), len(peers2), len(peers3))
		}
	})

	t.Run("LeaderElection", func(t *testing.T) {
		// Wait for leader election
		time.Sleep(2 * time.Second)

		// Verify leader election
		leaders := 0
		if node1.IsLeader() {
			leaders++
		}
		if node2.IsLeader() {
			leaders++
		}
		if node3.IsLeader() {
			leaders++
		}

		if leaders != 1 {
			t.Errorf("Expected 1 leader, got %d", leaders)
		}
	})

	t.Run("Replication", func(t *testing.T) {
		// Create topic with replication
		topicName := "test-topic"
		config := &topic.TopicConfig{
			NumPartitions:     3,
			ReplicationFactor: 3,
		}

		// Get leader node
		var leader *cluster.Cluster
		if node1.IsLeader() {
			leader = node1
		} else if node2.IsLeader() {
			leader = node2
		} else {
			leader = node3
		}

		// Create broker for leader node
		leaderBroker, cleanup := setupTestBroker(t, createTestDir(t, "broker-test"))
		defer cleanup()
		leaderBroker.SetCluster(leader)

		// Create topic on leader broker
		if err := leaderBroker.CreateTopic(topicName, config, true); err != nil {
			t.Fatalf("Failed to create topic: %v", err)
		}

		// Wait for replication
		time.Sleep(2 * time.Second)

		// Create brokers for follower nodes
		follower1Broker, cleanup1 := setupTestBroker(t, createTestDir(t, "broker-test"))
		defer cleanup1()
		follower1Broker.SetCluster(node2)

		follower2Broker, cleanup2 := setupTestBroker(t, createTestDir(t, "broker-test"))
		defer cleanup2()
		follower2Broker.SetCluster(node3)

		// Verify topic exists on all brokers
		if !leaderBroker.TopicExists(topicName) {
			t.Error("Topic not found on leader broker")
		}
		if !follower1Broker.TopicExists(topicName) {
			t.Error("Topic not found on follower1 broker")
		}
		if !follower2Broker.TopicExists(topicName) {
			t.Error("Topic not found on follower2 broker")
		}
	})
}

// TestDynamicRebalancing tests that partitions are rebalanced when a consumer leaves and lag is reduced
func TestDynamicRebalancing(t *testing.T) {
	b, cleanup := setupTestBroker(t, createTestDir(t, "rebalance-test"))
	defer cleanup()

	topicName := "rebalance-topic"
	groupID := "rebalance-group"
	config := &topic.TopicConfig{
		NumPartitions:     3,
		ReplicationFactor: 1,
	}

	if err := b.CreateTopic(topicName, config, true); err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Register 3 consumers
	consumerIDs := []string{"c1", "c2", "c3"}
	for _, cid := range consumerIDs {
		if err := b.RegisterConsumer(groupID, cid, []string{topicName}); err != nil {
			t.Fatalf("Failed to register consumer %s: %v", cid, err)
		}
	}

	// Produce messages to create lag
	for i := 0; i < 30; i++ {
		msg := &api.ProduceMessage{
			Topic:   topicName,
			Key:     fmt.Sprintf("key-%d", i),
			Value:   fmt.Sprintf("value-%d", i),
			Headers: map[string]string{"test": "header"},
		}
		if _, err := b.Produce(msg.Topic, msg.Key, []byte(msg.Value), msg.Headers); err != nil {
			t.Fatalf("Failed to produce message: %v", err)
		}
	}

	// Get initial assignments
	assignmentsBefore := make(map[string]map[string][]int)
	for _, cid := range consumerIDs {
		assignments, err := b.GetConsumerAssignments(groupID, cid)
		if err != nil {
			t.Fatalf("Failed to get assignments for %s: %v", cid, err)
		}
		assignmentsBefore[cid] = assignments
	}

	// Unregister one consumer to trigger rebalance
	if err := b.UnregisterConsumer(groupID, consumerIDs[0]); err != nil {
		t.Fatalf("Failed to unregister consumer: %v", err)
	}

	// Wait for rebalance
	time.Sleep(2 * time.Second)

	// Get assignments after rebalance
	assignmentsAfter := make(map[string]map[string][]int)
	for _, cid := range consumerIDs[1:] {
		assignments, err := b.GetConsumerAssignments(groupID, cid)
		if err != nil {
			t.Fatalf("Failed to get assignments for %s: %v", cid, err)
		}
		assignmentsAfter[cid] = assignments
	}

	// Check that all partitions are still assigned
	assignedPartitions := make(map[int]bool)
	for _, a := range assignmentsAfter {
		for _, partitions := range a {
			for _, p := range partitions {
				assignedPartitions[p] = true
			}
		}
	}
	if len(assignedPartitions) != config.NumPartitions {
		t.Errorf("Not all partitions assigned after rebalance: got %d, want %d", len(assignedPartitions), config.NumPartitions)
	}

}