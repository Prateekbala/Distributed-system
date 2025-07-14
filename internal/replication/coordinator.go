package replication

import (
	"sync"
)

// ReplicationCoordinator manages replication between brokers
type ReplicationCoordinator struct {
	replicaManager *ReplicationManager
	leaderElection *LeaderElection
	brokerID       string
	mu             sync.RWMutex
}

// NewReplicationCoordinator creates a new replication coordinator
func NewReplicationCoordinator(rm *ReplicationManager, brokerID string) *ReplicationCoordinator {
	le := NewLeaderElection(rm.ReplicaManager, rm)
	return &ReplicationCoordinator{
		replicaManager: rm,
		leaderElection: le,
		brokerID:       brokerID,
	}
}

// StartReplication starts replication for a partition
func (rc *ReplicationCoordinator) StartReplication(topic string, partition int) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	// If we're the leader, start replicating to followers
	if rc.leaderElection.IsLeader(topic, partition, rc.brokerID) {
		rc.replicateToFollowers(topic, partition)
	} else {
		// If we're a follower, start fetching from leader
		rc.fetchFromLeader(topic, partition)
	}
}

// replicateToFollowers replicates data from leader to followers
func (rc *ReplicationCoordinator) replicateToFollowers(topic string, partition int) {
	replicas := rc.replicaManager.GetReplicas(topic, partition)
	for _, replica := range replicas {
		if replica.BrokerID != rc.brokerID && replica.Status != StatusOffline {
			// In a real implementation, this would send data to followers
			// For now, we just update the high water mark
			rc.replicaManager.UpdateHighWaterMark(topic, partition, replica.BrokerID, 0)
		}
	}
}

// fetchFromLeader fetches data from leader
func (rc *ReplicationCoordinator) fetchFromLeader(topic string, partition int) {
	leader := rc.replicaManager.GetLeader(topic, partition)
	if leader == "" {
		return
	}

	// In a real implementation, this would fetch data from leader
	// For now, we just update our high water mark
	rc.replicaManager.UpdateHighWaterMark(topic, partition, rc.brokerID, 0)
}

// HandleBrokerFailure handles broker failure
func (rc *ReplicationCoordinator) HandleBrokerFailure(brokerID string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	// Trigger leader election if needed
	rc.leaderElection.HandleBrokerFailure(brokerID)

	// Update replica statuses
	for topic, partitions := range rc.replicaManager.replicas {
		for partition := range partitions {
			rc.replicaManager.UpdateReplicaStatus(topic, partition, brokerID, StatusOffline)
		}
	}
}

// GetReplicationStatus returns the replication status for a partition
func (rc *ReplicationCoordinator) GetReplicationStatus(topic string, partition int) map[string]ReplicaStatus {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	status := make(map[string]ReplicaStatus)
	replicas := rc.replicaManager.GetReplicas(topic, partition)
	for _, replica := range replicas {
		status[replica.BrokerID] = replica.Status
	}

	return status
}

// UpdateReplicaStatus updates the status of a replica
func (rc *ReplicationCoordinator) UpdateReplicaStatus(topic string, partition int, brokerID string, status ReplicaStatus) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.replicaManager.UpdateReplicaStatus(topic, partition, brokerID, status)

	// If we're the leader and a follower went offline, trigger re-election
	if rc.leaderElection.IsLeader(topic, partition, rc.brokerID) && status == StatusOffline {
		rc.leaderElection.TriggerReelection(topic, partition)
	}
}

// GetHighWaterMark returns the high water mark for a partition
func (rc *ReplicationCoordinator) GetHighWaterMark(topic string, partition int) int64 {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	replica := rc.replicaManager.GetReplicas(topic, partition)
	if len(replica) == 0 {
		return 0
	}

	// Return the minimum high water mark among all replicas
	minHWM := int64(^uint64(0) >> 1) // Max int64
	for _, r := range replica {
		if r.HighWaterMark < minHWM {
			minHWM = r.HighWaterMark
		}
	}

	return minHWM
}

// UpdateHighWaterMark updates the high water mark for a partition
func (rc *ReplicationCoordinator) UpdateHighWaterMark(topic string, partition int, hwm int64) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.replicaManager.UpdateHighWaterMark(topic, partition, rc.brokerID, hwm)
} 