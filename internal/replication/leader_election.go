package replication

import (
	"sync"
	"time"
)

// ISRProvider is an interface for getting ISR for a partition
type ISRProvider interface {
	GetISR(topic string, partition int) []string
}

// LeaderElection manages leader election for partitions
type LeaderElection struct {
	replicaManager *ReplicaManager
	isrProvider    ISRProvider
	mu            sync.RWMutex
}

// NewLeaderElection creates a new leader election manager
func NewLeaderElection(rm *ReplicaManager, isr ISRProvider) *LeaderElection {
	return &LeaderElection{
		replicaManager: rm,
		isrProvider:    isr,
	}
}

// ElectLeader elects a leader for a partition from ISR
func (le *LeaderElection) ElectLeader(topic string, partition int) *Replica {
	le.mu.Lock()
	defer le.mu.Unlock()

	// Only elect from ISR
	isr := le.isrProvider.GetISR(topic, partition)
	if len(isr) == 0 {
		return nil
	}
	// Choose the first in-sync replica as leader
	le.replicaManager.SetLeader(topic, partition, isr[0])
	// Return the corresponding Replica object if available
	replicas := le.replicaManager.GetReplicas(topic, partition)
	for _, replica := range replicas {
		if replica.BrokerID == isr[0] {
			return replica
		}
	}
	return nil
}

// CheckLeaderHealth checks if the current leader is healthy
func (le *LeaderElection) CheckLeaderHealth(topic string, partition int) bool {
	le.mu.RLock()
	defer le.mu.RUnlock()

	leader := le.replicaManager.GetLeader(topic, partition)
	if leader == nil {
		return false
	}

	// Consider leader unhealthy if no update in last 30 seconds
	return time.Since(leader.LastUpdate) < 30*time.Second
}

// TriggerReelection triggers a leader re-election for a partition
func (le *LeaderElection) TriggerReelection(topic string, partition int) *Replica {
	le.mu.Lock()
	defer le.mu.Unlock()

	// Mark current leader as offline
	currentLeader := le.replicaManager.GetLeader(topic, partition)
	if currentLeader != nil {
		le.replicaManager.UpdateReplicaStatus(topic, partition, currentLeader.BrokerID, StatusOffline)
	}

	// Elect new leader
	return le.ElectLeader(topic, partition)
}

// HandleBrokerFailure handles broker failure and triggers re-election if needed
func (le *LeaderElection) HandleBrokerFailure(brokerID string) {
	le.mu.Lock()
	defer le.mu.Unlock()

	// Find all partitions where this broker was leader
	for topic, partitions := range le.replicaManager.replicas {
		for partition, replicas := range partitions {
			for _, replica := range replicas {
				if replica.BrokerID == brokerID && replica.Status == StatusLeader {
					// Trigger re-election for this partition
					le.TriggerReelection(topic, partition)
				}
			}
		}
	}
}

// IsLeader checks if a broker is the leader for a partition
func (le *LeaderElection) IsLeader(topic string, partition int, brokerID string) bool {
	le.mu.RLock()
	defer le.mu.RUnlock()

	leader := le.replicaManager.GetLeader(topic, partition)
	return leader != nil && leader.BrokerID == brokerID
}

// GetLeaderBrokerID returns the broker ID of the leader for a partition
func (le *LeaderElection) GetLeaderBrokerID(topic string, partition int) string {
	le.mu.RLock()
	defer le.mu.RUnlock()

	leader := le.replicaManager.GetLeader(topic, partition)
	if leader == nil {
		return ""
	}

	return leader.BrokerID
} 