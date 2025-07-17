// internal/cluster/raft.go
package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// RaftCommandType defines the type of a command for the Raft log.
type RaftCommandType string

const (
	CmdCreateTopic        RaftCommandType = "create_topic"
	CmdDeleteTopic        RaftCommandType = "delete_topic"
	CmdAssignPartitions   RaftCommandType = "assign_partitions"
	CmdUpdateConsumerGroup RaftCommandType = "update_consumer_group" // New command type
)

// RaftCommand is a generic command applied to the Raft log.
type RaftCommand struct {
	Type    RaftCommandType `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// RaftNode wraps the raft.Raft instance and its FSM.
type RaftNode struct {
	Raft *raft.Raft
	FSM  *RaftClusterFSM
}

// RaftClusterFSM implements raft.FSM for managing cluster metadata.
// It is the single source of truth for topics, partition assignments, AND consumer groups.
type RaftClusterFSM struct {
	mu    sync.RWMutex
	state *MetadataStore // The state managed by Raft consensus.
}

// NewRaftClusterFSM creates a new, empty FSM.
func NewRaftClusterFSM() *RaftClusterFSM {
	return &RaftClusterFSM{
		state: NewMetadataStore(),
	}
}

// Apply applies a Raft log entry to the finite state machine.
func (f *RaftClusterFSM) Apply(logEntry *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	var cmd RaftCommand
	if err := json.Unmarshal(logEntry.Data, &cmd); err != nil {
		log.Printf("[ERROR] RaftFSM: Failed to unmarshal command: %v", err)
		return err
	}

	log.Printf("[DEBUG] RaftFSM: Applying command: %s", cmd.Type)

	switch cmd.Type {
	case CmdCreateTopic:
		var meta TopicMetadata
		if err := json.Unmarshal(cmd.Payload, &meta); err != nil {
			log.Printf("[ERROR] RaftFSM: Failed to unmarshal create_topic payload: %v", err)
			return err
		}
		if f.state.Topics == nil {
			f.state.Topics = make(map[string]*TopicMetadata)
		}
		f.state.Topics[meta.Name] = &meta
		log.Printf("[INFO] RaftFSM: Applied create topic: %s", meta.Name)

	case CmdDeleteTopic:
		var topicName string
		if err := json.Unmarshal(cmd.Payload, &topicName); err != nil {
			log.Printf("[ERROR] RaftFSM: Failed to unmarshal delete_topic payload: %v", err)
			return err
		}
		delete(f.state.Topics, topicName)
		delete(f.state.Assignments, topicName)
		log.Printf("[INFO] RaftFSM: Applied delete topic: %s", topicName)

	case CmdAssignPartitions:
		var assignments map[string]map[int]*PartitionAssignment
		if err := json.Unmarshal(cmd.Payload, &assignments); err != nil {
			log.Printf("[ERROR] RaftFSM: Failed to unmarshal assign_partitions payload: %v", err)
			return err
		}
		for topic, partitionMap := range assignments {
			if _, ok := f.state.Assignments[topic]; !ok {
				f.state.Assignments[topic] = make(map[int]*PartitionAssignment)
			}
			for partition, assignment := range partitionMap {
				f.state.Assignments[topic][partition] = assignment
				log.Printf("[INFO] RaftFSM: Applied assignment for %s-%d -> Leader: %s, Replicas: %v", topic, partition, assignment.Leader, assignment.Replicas)
			}
		}

	case CmdUpdateConsumerGroup:
		var group ConsumerGroup
		if err := json.Unmarshal(cmd.Payload, &group); err != nil {
			log.Printf("[ERROR] RaftFSM: Failed to unmarshal update_consumer_group payload: %v", err)
			return err
		}
		if f.state.ConsumerGroups == nil {
			f.state.ConsumerGroups = make(map[string]*ConsumerGroup)
		}
		f.state.ConsumerGroups[group.ID] = &group
		log.Printf("[INFO] RaftFSM: Applied update for consumer group: %s with %d consumers", group.ID, len(group.Consumers))

	default:
		log.Printf("[WARN] RaftFSM: Unknown command type received: %s", cmd.Type)
	}

	return nil
}

// GetAssignments returns a deep copy of the current partition assignments.
func (f *RaftClusterFSM) GetAssignments() map[string]map[int]*PartitionAssignment {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.state.copyAssignments()
}

// GetTopics returns a copy of the current topics.
func (f *RaftClusterFSM) GetTopics() map[string]*TopicMetadata {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.state.copyTopics()
}

// GetConsumerGroups returns a deep copy of the current consumer groups.
func (f *RaftClusterFSM) GetConsumerGroups() map[string]*ConsumerGroup {
    f.mu.RLock()
    defer f.mu.RUnlock()
    return f.state.copyConsumerGroups()
}


// Snapshot returns a snapshot of the FSM state.
func (f *RaftClusterFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	data, err := json.Marshal(f.state)
	if err != nil {
		return nil, err
	}

	log.Printf("[INFO] RaftFSM: Creating a new state snapshot.")
	return &RaftClusterSnapshot{data: data}, nil
}

// Restore restores the FSM from a snapshot.
func (f *RaftClusterFSM) Restore(rc io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer rc.Close()

	newState := NewMetadataStore()
	if err := json.NewDecoder(rc).Decode(newState); err != nil {
		return fmt.Errorf("could not restore FSM state from snapshot: %w", err)
	}

	f.state = newState
	log.Printf("[INFO] RaftFSM: State restored successfully from snapshot.")
	return nil
}

// RaftClusterSnapshot implements the FSMSnapshot interface.
type RaftClusterSnapshot struct {
	data []byte
}

// Persist writes the snapshot data to a sink.
func (s *RaftClusterSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("failed to write snapshot to sink: %w", err)
	}
	return sink.Close()
}

// Release is a no-op.
func (s *RaftClusterSnapshot) Release() {}

// --- Raft Node Setup ---

const raftTimeout = 10 * time.Second

// NewRaftNode initializes a Raft node.
func NewRaftNode(dataDir, nodeID, bindAddr string, bootstrap bool) (*RaftNode, error) {
    // ... (rest of the function is unchanged)
    config := raft.DefaultConfig()
    config.LocalID = raft.ServerID(nodeID)
    config.SnapshotThreshold = 1024

    raftDataDir := filepath.Join(dataDir, "raft")
    if err := os.MkdirAll(raftDataDir, 0755); err != nil {
        return nil, fmt.Errorf("failed to create raft data dir: %w", err)
    }

    logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDataDir, "raft-log.db"))
    if err != nil {
        return nil, fmt.Errorf("failed to create log store: %w", err)
    }
    stableStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDataDir, "raft-stable.db"))
    if err != nil {
        return nil, fmt.Errorf("failed to create stable store: %w", err)
    }

    snapshots, err := raft.NewFileSnapshotStore(raftDataDir, 2, os.Stderr)
    if err != nil {
        return nil, fmt.Errorf("failed to create snapshot store: %w", err)
    }

    transport, err := raft.NewTCPTransport(bindAddr, nil, 3, raftTimeout, os.Stderr)
    if err != nil {
        return nil, fmt.Errorf("failed to create tcp transport: %w", err)
    }

    fsm := NewRaftClusterFSM()

    r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshots, transport)
    if err != nil {
        return nil, fmt.Errorf("failed to create raft instance: %w", err)
    }

    if bootstrap {
        bootstrapConfig := raft.Configuration{
            Servers: []raft.Server{
                {
                    ID:      config.LocalID,
                    Address: transport.LocalAddr(),
                },
            },
        }
        if f := r.BootstrapCluster(bootstrapConfig); f.Error() != nil {
            return nil, fmt.Errorf("failed to bootstrap cluster: %w", f.Error())
        }
        log.Printf("[INFO] Raft: Cluster bootstrapped successfully on node %s", nodeID)
    }

    return &RaftNode{Raft: r, FSM: fsm}, nil
}

// ProposeCommand proposes a command to the Raft cluster.
func (rn *RaftNode) ProposeCommand(cmd RaftCommand) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal raft command: %w", err)
	}

	f := rn.Raft.Apply(data, raftTimeout)
	if err := f.Error(); err != nil {
		return fmt.Errorf("failed to apply raft command: %w", err)
	}
	return nil
}
