package replication

import (
	"sync"
	"time"
)

// ReplicaStatus represents the state of a replica
type ReplicaStatus string

const (
	StatusLeader   ReplicaStatus = "leader"
	StatusFollower ReplicaStatus = "follower"
	StatusOffline  ReplicaStatus = "offline"
)

// Replica represents a copy of a partition on a broker
type Replica struct {
	BrokerID    string
	PartitionID int
	Topic       string
	Status      ReplicaStatus
	LastUpdate  time.Time
	HighWaterMark int64
	mu          sync.RWMutex
}

// ReplicaManager manages replicas for partitions
type ReplicaManager struct {
	replicas    map[string]map[int][]*Replica // topic -> partition -> replicas
	mu          sync.RWMutex
}

// NewReplicaManager creates a new replica manager
func NewReplicaManager() *ReplicaManager {
	return &ReplicaManager{
		replicas: make(map[string]map[int][]*Replica),
	}
}

// AddReplica adds a replica for a partition
func (rm *ReplicaManager) AddReplica(topic string, partition int, brokerID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if _, exists := rm.replicas[topic]; !exists {
		rm.replicas[topic] = make(map[int][]*Replica)
	}
	if _, exists := rm.replicas[topic][partition]; !exists {
		rm.replicas[topic][partition] = make([]*Replica, 0)
	}

	replica := &Replica{
		BrokerID:    brokerID,
		PartitionID: partition,
		Topic:       topic,
		Status:      StatusFollower,
		LastUpdate:  time.Now(),
	}

	rm.replicas[topic][partition] = append(rm.replicas[topic][partition], replica)
}

// RemoveReplica removes a replica for a partition
func (rm *ReplicaManager) RemoveReplica(topic string, partition int, brokerID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if _, exists := rm.replicas[topic]; !exists {
		return
	}
	if _, exists := rm.replicas[topic][partition]; !exists {
		return
	}

	replicas := rm.replicas[topic][partition]
	for i, replica := range replicas {
		if replica.BrokerID == brokerID {
			rm.replicas[topic][partition] = append(replicas[:i], replicas[i+1:]...)
			break
		}
	}
}

// GetReplicas returns all replicas for a partition
func (rm *ReplicaManager) GetReplicas(topic string, partition int) []*Replica {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if _, exists := rm.replicas[topic]; !exists {
		return nil
	}
	if _, exists := rm.replicas[topic][partition]; !exists {
		return nil
	}

	return rm.replicas[topic][partition]
}

// GetLeader returns the leader replica for a partition
func (rm *ReplicaManager) GetLeader(topic string, partition int) *Replica {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if _, exists := rm.replicas[topic]; !exists {
		return nil
	}
	if _, exists := rm.replicas[topic][partition]; !exists {
		return nil
	}

	for _, replica := range rm.replicas[topic][partition] {
		if replica.Status == StatusLeader {
			return replica
		}
	}

	return nil
}

// SetLeader sets the leader replica for a partition
func (rm *ReplicaManager) SetLeader(topic string, partition int, brokerID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if _, exists := rm.replicas[topic]; !exists {
		return
	}
	if _, exists := rm.replicas[topic][partition]; !exists {
		return
	}

	for _, replica := range rm.replicas[topic][partition] {
		if replica.BrokerID == brokerID {
			replica.Status = StatusLeader
			replica.LastUpdate = time.Now()
		} else {
			replica.Status = StatusFollower
		}
	}
}

// UpdateReplicaStatus updates the status of a replica
func (rm *ReplicaManager) UpdateReplicaStatus(topic string, partition int, brokerID string, status ReplicaStatus) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if _, exists := rm.replicas[topic]; !exists {
		return
	}
	if _, exists := rm.replicas[topic][partition]; !exists {
		return
	}

	for _, replica := range rm.replicas[topic][partition] {
		if replica.BrokerID == brokerID {
			replica.Status = status
			replica.LastUpdate = time.Now()
			break
		}
	}
}

// UpdateHighWaterMark updates the high water mark for a replica
func (rm *ReplicaManager) UpdateHighWaterMark(topic string, partition int, brokerID string, hwm int64) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if _, exists := rm.replicas[topic]; !exists {
		return
	}
	if _, exists := rm.replicas[topic][partition]; !exists {
		return
	}

	for _, replica := range rm.replicas[topic][partition] {
		if replica.BrokerID == brokerID {
			replica.HighWaterMark = hwm
			replica.LastUpdate = time.Now()
			break
		}
	}
}

// GetReplicaStatus returns the status of a replica
func (rm *ReplicaManager) GetReplicaStatus(topic string, partition int, brokerID string) ReplicaStatus {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if _, exists := rm.replicas[topic]; !exists {
		return StatusOffline
	}
	if _, exists := rm.replicas[topic][partition]; !exists {
		return StatusOffline
	}

	for _, replica := range rm.replicas[topic][partition] {
		if replica.BrokerID == brokerID {
			return replica.Status
		}
	}

	return StatusOffline
}

// GetReplicaCount returns the number of replicas for a partition
func (rm *ReplicaManager) GetReplicaCount(topic string, partition int) int {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if _, exists := rm.replicas[topic]; !exists {
		return 0
	}
	if _, exists := rm.replicas[topic][partition]; !exists {
		return 0
	}

	return len(rm.replicas[topic][partition])
} 