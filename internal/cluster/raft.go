package cluster

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	raft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// RaftClusterFSM implements raft.FSM for cluster metadata
// (topics, partition assignments, etc.)
type RaftClusterFSM struct {
	mu     sync.RWMutex
	state  *MetadataStore
}

func NewRaftClusterFSM() *RaftClusterFSM {
	return &RaftClusterFSM{
		state: NewMetadataStore(),
	}
}

// GetState returns a copy of the current state (thread-safe)
func (f *RaftClusterFSM) GetState() *MetadataStore {
	f.mu.RLock()
	defer f.mu.RUnlock()
	
	// Create a deep copy
	stateCopy := &MetadataStore{
		Topics:      make(map[string]*TopicMetadata),
		Assignments: make(map[string]map[int]*PartitionAssignment),
	}
	
	for k, v := range f.state.Topics {
		stateCopy.Topics[k] = v
	}
	
	for topic, partitions := range f.state.Assignments {
		stateCopy.Assignments[topic] = make(map[int]*PartitionAssignment)
		for partition, assignment := range partitions {
			stateCopy.Assignments[topic][partition] = assignment
		}
	}
	
	return stateCopy
}

// BrokerObserver is an interface for brokers to receive FSM state change notifications
// (Implemented by the broker)
type BrokerObserver interface {
	OnTopicCreated(meta *TopicMetadata)
	OnTopicDeleted(topic string)
	OnPartitionAssigned(topic string, partition int, assignment *PartitionAssignment)
}

// BrokerRegistry holds registered brokers for FSM notifications
var brokerRegistry = struct {
	mu      sync.RWMutex
	brokers []BrokerObserver
}{brokers: []BrokerObserver{}}

// RegisterBroker adds a broker to the registry
func RegisterBroker(b BrokerObserver) {
	brokerRegistry.mu.Lock()
	defer brokerRegistry.mu.Unlock()
	brokerRegistry.brokers = append(brokerRegistry.brokers, b)
}

// UnregisterBroker removes a broker from the registry
func UnregisterBroker(b BrokerObserver) {
	brokerRegistry.mu.Lock()
	defer brokerRegistry.mu.Unlock()
	for i, br := range brokerRegistry.brokers {
		if br == b {
			brokerRegistry.brokers = append(brokerRegistry.brokers[:i], brokerRegistry.brokers[i+1:]...)
			break
		}
	}
}

// notifyTopicCreated notifies all brokers of a new topic
func notifyTopicCreated(meta *TopicMetadata) {
	brokerRegistry.mu.RLock()
	defer brokerRegistry.mu.RUnlock()
	for _, b := range brokerRegistry.brokers {
		go b.OnTopicCreated(meta) // Async notification to prevent blocking
	}
}

// notifyTopicDeleted notifies all brokers of a topic deletion
func notifyTopicDeleted(topic string) {
	brokerRegistry.mu.RLock()
	defer brokerRegistry.mu.RUnlock()
	for _, b := range brokerRegistry.brokers {
		go b.OnTopicDeleted(topic) // Async notification to prevent blocking
	}
}

// notifyPartitionAssigned notifies all brokers of a partition assignment
func notifyPartitionAssigned(topic string, partition int, assignment *PartitionAssignment) {
	brokerRegistry.mu.RLock()
	defer brokerRegistry.mu.RUnlock()
	for _, b := range brokerRegistry.brokers {
		go b.OnPartitionAssigned(topic, partition, assignment) // Async notification to prevent blocking
	}
}

// Apply applies a Raft log entry to the FSM
func (f *RaftClusterFSM) Apply(logEntry *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	
	var cmd RaftCommand
	if err := json.Unmarshal(logEntry.Data, &cmd); err != nil {
		log.Printf("[RaftFSM] Failed to unmarshal command: %v", err)
		return err
	}
	
	switch cmd.Type {
	case "create_topic":
		var meta TopicMetadata
		if err := json.Unmarshal(cmd.Payload, &meta); err != nil {
			log.Printf("[RaftFSM] Failed to unmarshal topic metadata: %v", err)
			return err
		}
		f.state.Topics[meta.Name] = &meta
		notifyTopicCreated(&meta)
		log.Printf("[RaftFSM] Topic created: %s", meta.Name)
		
	case "delete_topic":
		var topicName string
		if err := json.Unmarshal(cmd.Payload, &topicName); err != nil {
			log.Printf("[RaftFSM] Failed to unmarshal topic name: %v", err)
			return err
		}
		delete(f.state.Topics, topicName)
		delete(f.state.Assignments, topicName)
		notifyTopicDeleted(topicName)
		log.Printf("[RaftFSM] Topic deleted: %s", topicName)
		
	case "assign_partition":
		var ap struct {
			Topic      string               `json:"topic"`
			Partition  int                  `json:"partition"`
			Assignment *PartitionAssignment `json:"assignment"`
		}
		if err := json.Unmarshal(cmd.Payload, &ap); err != nil {
			log.Printf("[RaftFSM] Failed to unmarshal partition assignment: %v", err)
			return err
		}
		if f.state.Assignments[ap.Topic] == nil {
			f.state.Assignments[ap.Topic] = make(map[int]*PartitionAssignment)
		}
		f.state.Assignments[ap.Topic][ap.Partition] = ap.Assignment
		notifyPartitionAssigned(ap.Topic, ap.Partition, ap.Assignment)
		log.Printf("[RaftFSM] Partition assigned: %s-%d to %s", ap.Topic, ap.Partition, ap.Assignment.Leader)
		
	default:
		log.Printf("[RaftFSM] Unknown command type: %s", cmd.Type)
		return nil
	}
	
	return nil
}

// Snapshot returns a snapshot of the FSM state
func (f *RaftClusterFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	
	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(f.state); err != nil {
		return nil, err
	}
	
	return &RaftClusterSnapshot{data: buf.Bytes()}, nil
}

// Restore restores the FSM from a snapshot
func (f *RaftClusterFSM) Restore(rc io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer rc.Close()
	
	newState := NewMetadataStore()
	if err := json.NewDecoder(rc).Decode(newState); err != nil {
		return err
	}
	
	f.state = newState
	log.Printf("[RaftFSM] State restored from snapshot")
	return nil
}

type RaftClusterSnapshot struct {
	data []byte
}

func (s *RaftClusterSnapshot) Persist(sink raft.SnapshotSink) error {
	_, err := sink.Write(s.data)
	if err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *RaftClusterSnapshot) Release() {}

// RaftCommand is a generic command for Raft log
// Type: "create_topic", "delete_topic", "assign_partition"
type RaftCommand struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// RaftNode wraps the raft.Raft instance and FSM
type RaftNode struct {
	Raft *raft.Raft
	FSM  *RaftClusterFSM
}

const raftTimeout = 10 * time.Second

// NewRaftNodeWithElectionControl initializes a Raft node with election control
func NewRaftNodeWithElectionControl(dataDir, nodeID, bindAddr string, peers []string, bootstrap bool) (*RaftNode, error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)
	
	// For non-bootstrap nodes, disable election timeout initially
	if !bootstrap {
		config.HeartbeatTimeout = 1000 * time.Second
		config.ElectionTimeout = 1000 * time.Second
		config.LeaderLeaseTimeout = 1000 * time.Second
	}
	
	// Ensure data directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	// Create log store using BoltDB
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft-log.db"))
	if err != nil {
		return nil, err
	}
	
	// Create stable store using BoltDB
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft-stable.db"))
	if err != nil {
		return nil, err
	}
	
	// Create snapshot store
	snapshots, err := raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
	if err != nil {
		return nil, err
	}
	
	// Create transport
	transport, err := raft.NewTCPTransport(bindAddr, nil, 3, raftTimeout, os.Stderr)
	if err != nil {
		return nil, err
	}

	// Create FSM
	fsm := NewRaftClusterFSM()
	
	// Create Raft instance
	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, err
	}

	// Bootstrap cluster if this is the first node
	if bootstrap {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		f := r.BootstrapCluster(cfg)
		if err := f.Error(); err != nil {
			log.Printf("[RaftNode] Failed to bootstrap cluster: %v", err)
			return nil, err
		}
		log.Printf("[RaftNode] Cluster bootstrapped with node %s", nodeID)
	}

	return &RaftNode{Raft: r, FSM: fsm}, nil
}

// ProposeCommand proposes a command to the Raft log
func (rn *RaftNode) ProposeCommand(cmd RaftCommand) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	
	f := rn.Raft.Apply(data, raftTimeout)
	return f.Error()
}

// IsLeader returns true if this node is the leader
func (rn *RaftNode) IsLeader() bool {
	return rn.Raft.State() == raft.Leader
}

// GetLeader returns the current leader's address
func (rn *RaftNode) GetLeader() string {
	_, leader := rn.Raft.LeaderWithID()
	return string(leader)
}

// Shutdown gracefully shuts down the Raft node
func (rn *RaftNode) Shutdown() error {
	return rn.Raft.Shutdown().Error()
}