// test/cluster_integration_test.go
package test

import (
	"Distributed-system/internal/broker"
	"Distributed-system/internal/cluster"
	"Distributed-system/internal/config"
	"Distributed-system/internal/offset"
	"Distributed-system/internal/storage"
	"fmt"
	"log"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// --- Test Helper Structs ---

// testNode represents a fully running broker instance for testing purposes.
type testNode struct {
	id      string
	broker  *broker.Broker
	cluster *cluster.Cluster
	cfg     *config.Config
	basedir string
}

// --- The Main Test Function ---

// TestCluster_LeaderFailover is the primary end-to-end test. It simulates
// a real-world leader failure and verifies that the system remains available
// and consistent.
func TestCluster_LeaderFailover(t *testing.T) {
	// --- 1. SETUP: Create three nodes ---
	node1 := setupTestNode(t, "broker-1", true, "")
	node2 := setupTestNode(t, "broker-2", false, GossipAddress(node1.cluster))
	node3 := setupTestNode(t, "broker-3", false, GossipAddress(node1.cluster))

	nodes := map[string]*testNode{
		"broker-1": node1,
		"broker-2": node2,
		"broker-3": node3,
	}

	// Cleanup function to stop all nodes after the test.
	t.Cleanup(func() {
		for _, n := range nodes {
			n.broker.Stop()
			n.cluster.Stop()
		}
	})

	// --- 2. VERIFY INITIAL CLUSTER STATE ---
	log.Println("TEST: Verifying initial cluster formation...")
	// Wait for the cluster to form and elect a leader.
	var leaderID string
	require.Eventually(t, func() bool {
		leaderID = string(node1.cluster.RaftNode.Raft.Leader())
		return leaderID != ""
	}, 10*time.Second, 500*time.Millisecond, "timed out waiting for a leader to be elected")

	log.Printf("TEST: Leader elected: %s", leaderID)
	// Map Raft address to broker ID
	var leaderBrokerID string
	for id, n := range nodes {
		if n.cluster != nil && n.cluster.RaftNode != nil && n.cluster.RaftBindAddr == leaderID {
			leaderBrokerID = id
			break
		}
	}
	leaderNode := nodes[leaderBrokerID]
	require.NotNil(t, leaderNode, "leader node should be in our map")

	// Verify that all nodes are part of the Raft cluster.
	require.Eventually(t, func() bool {
		configFuture := leaderNode.cluster.RaftNode.Raft.GetConfiguration()
		if err := configFuture.Error(); err != nil {
			return false
		}
		return len(configFuture.Configuration().Servers) == 3
	}, 10*time.Second, 500*time.Millisecond, "timed out waiting for all nodes to join the Raft cluster")
	log.Println("TEST: All 3 nodes successfully joined the Raft cluster.")

	// --- 3. CREATE TOPIC AND PRODUCE FIRST MESSAGE ---
	topicName := "failover-test-topic"
	log.Printf("TEST: Creating topic '%s' on leader %s", topicName, leaderID)

	// Create a topic with 1 partition and 3 replicas.
	err := leaderNode.broker.CreateTopic(topicName, 1, 3)
	require.NoError(t, err)

	// Wait for the partition to be assigned.
	require.Eventually(t, func() bool {
		assignments := leaderNode.broker.GetTopicMetadata()
		if topicAssignments, ok := assignments[topicName]; ok {
			return len(topicAssignments) == 1
		}
		return false
	}, 10*time.Second, 500*time.Millisecond, "timed out waiting for partition assignment")
	log.Println("TEST: Topic created and partition assigned.")

	// Produce the first message to the leader.
	msg1, err := leaderNode.broker.Produce(topicName, "key1", []byte("message before failover"), nil)
	require.NoError(t, err)
	log.Printf("TEST: Produced message 1 to leader %s at offset %d", leaderID, msg1.Offset)

	// Wait for the message to be replicated to at least one follower.
	// In a real test, you might have a more deterministic way to check this.
	time.Sleep(2 * time.Second)

	// --- 4. SIMULATE LEADER FAILURE ---
	log.Printf("TEST: >>> Shutting down leader node %s <<<", leaderID)
	leaderNode.broker.Stop()
	leaderNode.cluster.Stop()
	delete(nodes, string(leaderID)) // Remove it from our active nodes map.

	// --- 5. VERIFY NEW LEADER ELECTION ---
	log.Println("TEST: Verifying new leader election among remaining nodes...")
	var newLeaderID string
	var newLeaderNode *testNode
	remainingNodes := []*testNode{nodes["broker-2"], nodes["broker-3"]}
	if nodes["broker-1"] != nil {
		remainingNodes = []*testNode{nodes["broker-1"], nodes["broker-3"]}
	}
	if nodes["broker-2"] != nil && nodes["broker-1"] != nil {
		remainingNodes = []*testNode{nodes["broker-1"], nodes["broker-2"]}
	}


	require.Eventually(t, func() bool {
		for _, n := range remainingNodes {
			if n.cluster.IsLeader() {
				newLeaderID = n.id
				newLeaderNode = n
				return true
			}
		}
		return false
	}, 15*time.Second, 500*time.Millisecond, "timed out waiting for a new leader to be elected")
	log.Printf("TEST: New leader elected: %s", newLeaderID)
	require.NotEqual(t, leaderID, newLeaderID, "new leader should be different from the old one")

	// --- 6. VERIFY SYSTEM AVAILABILITY AND DATA INTEGRITY ---
	log.Println("TEST: Verifying system is available and data is intact.")

	// Produce a second message to the *new* leader.
	msg2, err := newLeaderNode.broker.Produce(topicName, "key2", []byte("message after failover"), nil)
	require.NoError(t, err, "producing to the new leader should succeed")
	log.Printf("TEST: Produced message 2 to new leader %s at offset %d", newLeaderID, msg2.Offset)
	require.Equal(t, int64(1), msg2.Offset, "offset should continue sequentially")

	// Consume from one of the remaining nodes.
	var finalFollowerNode *testNode
	for _, n := range nodes {
		if n.id != newLeaderID {
			finalFollowerNode = n
			break
		}
	}
	require.NotNil(t, finalFollowerNode, "should have one follower left")

	// Consume all messages from the beginning of the partition.
	// We should get both messages, proving no data was lost.
	log.Printf("TEST: Consuming from follower %s to verify data integrity.", finalFollowerNode.id)
	consumedMessages, err := finalFollowerNode.broker.Consume(topicName, 0, 0, 10)
	require.NoError(t, err)
	require.Len(t, consumedMessages, 2, "should have consumed both messages")

	// Verify the content and order of messages.
	require.Equal(t, "message before failover", string(consumedMessages[0].Value))
	require.Equal(t, int64(0), consumedMessages[0].Offset)
	require.Equal(t, "message after failover", string(consumedMessages[1].Value))
	require.Equal(t, int64(1), consumedMessages[1].Offset)

	log.Println("TEST: SUCCESS! Leader failover handled correctly with no data loss.")
}

// --- Helper Functions ---

// setupTestNode creates and starts a full broker instance for use in tests.
func setupTestNode(t *testing.T, id string, bootstrap bool, seed string) *testNode {
	t.Helper()

	// Create a unique data directory for this node.
	baseDir := t.TempDir()

	// Find free ports for all services to avoid conflicts when running tests in parallel.
	apiPort := getFreePort(t)
	gossipPort := getFreePort(t)
	raftPort := getFreePort(t)

	cfg := &config.Config{
		DataDir:    baseDir,
		SegmentSize: 1024, // Small segments for testing
		WALEnabled: true,
		WALSyncInterval: 100 * time.Millisecond, // Ensure positive interval for WAL ticker
	}

	// Initialize all components.
	diskStorage := storage.NewDiskStorage(cfg)
	wal, err := storage.NewWAL(cfg)
	require.NoError(t, err)
	offsetManager, err := offset.NewManager(cfg)
	require.NoError(t, err)

	raftBindAddr := fmt.Sprintf("127.0.0.1:%d", raftPort)
	clusterManager := cluster.NewCluster(id, "127.0.0.1", strconv.Itoa(apiPort), gossipPort, baseDir, raftBindAddr, bootstrap)

	var seeds []string
	if seed != "" {
		seeds = append(seeds, seed)
	}
	err = clusterManager.Start(seeds)
	require.NoError(t, err)

	brokerInstance := broker.NewBrokerWithStorage(id, clusterManager, diskStorage, wal, offsetManager)
	brokerInstance.Start()

	log.Printf("Setup node %s: API on %d, Gossip on %d, Raft on %d", id, apiPort, gossipPort, raftPort)

	return &testNode{
		id:      id,
		broker:  brokerInstance,
		cluster: clusterManager,
		cfg:     cfg,
		basedir: baseDir,
	}
}

// getFreePort asks the kernel for a free open port that is ready to use.
func getFreePort(t *testing.T) int {
	t.Helper()
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	require.NoError(t, err)

	l, err := net.ListenTCP("tcp", addr)
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// GossipAddress returns the address used for the memberlist protocol.
func GossipAddress(c *cluster.Cluster) string {
	// A small helper to get the gossip address for seeding other nodes.
	// This assumes the test runs on localhost.
	return fmt.Sprintf("127.0.0.1:%d", c.GossipPort())
}
