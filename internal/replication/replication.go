// internal/replication/replication.go
package replication

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"Distributed-system/internal/cluster"
	"Distributed-system/internal/message"
	"Distributed-system/internal/storage"
)

// ReplicationManager ensures a broker's local state reflects the authoritative
// assignments from the Raft cluster. It manages which partitions this broker
// should lead and which it should follow.
type ReplicationManager struct {
	brokerID string
	cluster  *cluster.Cluster // Reference to the cluster for peer addresses and state.
	storage  storage.Storage  // Local storage to read/write messages.

	// Local state, which is a cache of the authoritative assignments from Raft.
	assignments map[string]map[int]*cluster.PartitionAssignment
	mu          sync.RWMutex
	stopChan    chan struct{}
}

// NewReplicationManager creates a new replication manager.
func NewReplicationManager(brokerID string, cl *cluster.Cluster, st storage.Storage) *ReplicationManager {
	return &ReplicationManager{
		brokerID:    brokerID,
		cluster:     cl,
		storage:     st,
		assignments: make(map[string]map[int]*cluster.PartitionAssignment),
		stopChan:    make(chan struct{}),
	}
}

// Start begins the background tasks for replication.
func (rm *ReplicationManager) Start() {
	go rm.syncLoop()
	log.Printf("[INFO] ReplicationManager for broker %s started.", rm.brokerID)
}

// Stop gracefully shuts down the replication manager.
func (rm *ReplicationManager) Stop() {
	close(rm.stopChan)
	log.Printf("[INFO] ReplicationManager for broker %s stopped.", rm.brokerID)
}

// syncLoop is the main loop that periodically checks the authoritative state
// from Raft and updates the local replication state.
func (rm *ReplicationManager) syncLoop() {
	ticker := time.NewTicker(5 * time.Second) // Check for new assignments every 5 seconds.
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rm.syncAssignments()
		case <-rm.stopChan:
			return
		}
	}
}

// syncAssignments fetches the latest assignments from the Raft FSM and updates local state.
func (rm *ReplicationManager) syncAssignments() {
	// Get the authoritative assignments from the Raft FSM.
	authoritativeAssignments := rm.cluster.RaftNode.FSM.GetAssignments()

	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Simply replace the local cache with the latest authoritative state.
	rm.assignments = authoritativeAssignments
	log.Printf("[DEBUG] ReplicationManager: Synced assignments. Total topics: %d", len(rm.assignments))
}

// IsLeader checks if this broker is the leader for a given topic and partition.
func (rm *ReplicationManager) IsLeader(topic string, partition int) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if topicAssignments, ok := rm.assignments[topic]; ok {
		if assignment, ok := topicAssignments[partition]; ok {
			return assignment.Leader == rm.brokerID
		}
	}
	return false
}

// GetLeaderBrokerID returns the ID of the leader for a given partition.
func (rm *ReplicationManager) GetLeaderBrokerID(topic string, partition int) string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if topicAssignments, ok := rm.assignments[topic]; ok {
		if assignment, ok := topicAssignments[partition]; ok {
			return assignment.Leader
		}
	}
	return ""
}

// StartFollowerFetchRoutines starts background goroutines for every partition
// this broker is supposed to be a follower for.
func (rm *ReplicationManager) StartFollowerFetchRoutines() {
	go func() {
		ticker := time.NewTicker(10 * time.Second) // Adjust interval as needed
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				rm.runFollowerFetches()
			case <-rm.stopChan:
				return
			}
		}
	}()
}

// runFollowerFetches iterates through all assigned partitions and, if this node
// is a follower, fetches new messages from the leader.
func (rm *ReplicationManager) runFollowerFetches() {
	rm.mu.RLock()
	assignments := rm.assignments
	rm.mu.RUnlock()

	for topic, partitionMap := range assignments {
		for partition, assignment := range partitionMap {
			// Check if we are a follower for this partition.
			isFollower := false
			for _, replicaID := range assignment.Replicas {
				if replicaID == rm.brokerID && assignment.Leader != rm.brokerID {
					isFollower = true
					break
				}
			}

			if isFollower {
				// Run the fetch in a separate goroutine to avoid blocking.
				go rm.fetchFromLeader(topic, partition, assignment.Leader)
			}
		}
	}
}

// fetchFromLeader performs a single fetch operation from the leader for a partition.
func (rm *ReplicationManager) fetchFromLeader(topic string, partition int, leaderID string) {
	leaderAddr := rm.cluster.GetBrokerAddress(leaderID)
	if leaderAddr == "" {
		log.Printf("[WARN] Follower %s: Could not find address for leader %s of %s-%d", rm.brokerID, leaderID, topic, partition)
		return
	}

	// Determine the offset to start fetching from.
	offset := rm.storage.GetHighWaterMark(topic, partition)

	// Construct the request to the leader's consume endpoint.
	url := fmt.Sprintf("http://%s/topics/%s/partitions/%d/consume?offset=%d&limit=100", leaderAddr, topic, partition, offset)

	resp, err := http.Get(url)
	if err != nil {
		log.Printf("[ERROR] Follower %s: Failed to fetch from leader %s for %s-%d: %v", rm.brokerID, leaderID, topic, partition, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("[WARN] Follower %s: Leader %s for %s-%d returned status %d", rm.brokerID, leaderID, topic, partition, resp.StatusCode)
		return
	}

	var consumeResp struct {
		Messages []*message.Message `json:"messages"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&consumeResp); err != nil {
		log.Printf("[ERROR] Follower %s: Failed to decode response from leader %s for %s-%d: %v", rm.brokerID, leaderID, topic, partition, err)
		return
	}

	if len(consumeResp.Messages) > 0 {
		log.Printf("[INFO] Follower %s: Fetched %d messages from leader %s for %s-%d", rm.brokerID, len(consumeResp.Messages), leaderID, topic, partition)
		for _, msg := range consumeResp.Messages {
			if err := rm.storage.Store(msg); err != nil {
				log.Printf("[ERROR] Follower %s: Failed to store replicated message for %s-%d at offset %d: %v", rm.brokerID, topic, partition, msg.Offset, err)
			}
		}
	}
}

// HandleReplicationRequest is an HTTP handler for the leader to send messages to followers.
func (rm *ReplicationManager) HandleReplicationRequest(w http.ResponseWriter, r *http.Request) {
	// In a pull-based system, this endpoint is not used for message replication.
	// A leader never pushes messages to followers.
	http.Error(w, "Endpoint not used in pull-based replication", http.StatusNotImplemented)
}

// ReplicateMessage is called by the produce logic on the leader.
// In a pull-based system, this is a NO-OP. The message is simply stored locally.
// Followers will fetch it later.
func (rm *ReplicationManager) ReplicateMessage(msg *message.Message) error {
	// No action needed. The message is already stored on the leader via broker.Produce.
	// The follower's `fetchFromLeader` loop will replicate it.
	return nil
}
