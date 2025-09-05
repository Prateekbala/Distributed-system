// internal/cluster/cluster.go
package cluster

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
)

// Cluster manages service discovery (gossip) and Raft consensus.
type Cluster struct {
	nodeID       string
	address      string
	port         string
	clusterPort  int
	RaftBindAddr string

	ml       *memberlist.Memberlist
	peers    map[string]*BrokerPeer
	RaftNode *RaftNode

	mu       sync.RWMutex
	stopChan chan struct{}
}

// BrokerPeer represents information about another broker discovered via gossip.
type BrokerPeer struct {
	NodeID   string `json:"node_id"`
	Address  string `json:"address"`
	Port     string `json:"port"`
	RaftAddr string `json:"raft_addr"`
}

// --- Metadata and State Structures (The "Source of Truth" model) ---

// MetadataStore holds all data managed by Raft.
type MetadataStore struct {
	Topics         map[string]*TopicMetadata               `json:"topics"`
	Assignments    map[string]map[int]*PartitionAssignment `json:"assignments"`
	ConsumerGroups map[string]*ConsumerGroup               `json:"consumer_groups"`
}

// TopicMetadata defines the properties of a topic.
type TopicMetadata struct {
	Name              string `json:"name"`
	NumPartitions     int    `json:"num_partitions"`
	ReplicationFactor int    `json:"replication_factor"`
}

// PartitionAssignment defines the leader and replicas for a single partition.
type PartitionAssignment struct {
	Leader   string   `json:"leader"`
	Replicas []string `json:"replicas"`
}

// ConsumerGroup represents a group of consumers and their assignments.
type ConsumerGroup struct {
	ID          string              `json:"id"`
	Topics      []string            `json:"topics"`
	Consumers   map[string]*Consumer `json:"consumers"` // consumer_id -> consumer
	Assignments map[string][]int    `json:"assignments"` // consumer_id -> partitions
}

// Consumer represents a single consumer process.
type Consumer struct {
	ID            string `json:"id"`
	LastHeartbeat int64  `json:"last_heartbeat"` 
}

// NewMetadataStore initializes a new metadata store.
func NewMetadataStore() *MetadataStore {
	return &MetadataStore{
		Topics:         make(map[string]*TopicMetadata),
		Assignments:    make(map[string]map[int]*PartitionAssignment),
		ConsumerGroups: make(map[string]*ConsumerGroup),
	}
}

// --- Deep Copy Methods to prevent direct state mutation ---

func (ms *MetadataStore) copyTopics() map[string]*TopicMetadata {
	cpy := make(map[string]*TopicMetadata)
	for k, v := range ms.Topics {
		vCpy := *v
		cpy[k] = &vCpy
	}
	return cpy
}

func (ms *MetadataStore) copyAssignments() map[string]map[int]*PartitionAssignment {
	cpy := make(map[string]map[int]*PartitionAssignment)
	for topic, pMap := range ms.Assignments {
		cpy[topic] = make(map[int]*PartitionAssignment)
		for p, assign := range pMap {
			assignCpy := *assign
			cpy[topic][p] = &assignCpy
		}
	}
	return cpy
}

func (ms *MetadataStore) copyConsumerGroups() map[string]*ConsumerGroup {
    cpy := make(map[string]*ConsumerGroup)
    for groupID, group := range ms.ConsumerGroups {
        groupCpy := &ConsumerGroup{
            ID:          group.ID,
            Topics:      append([]string(nil), group.Topics...),
            Consumers:   make(map[string]*Consumer),
            Assignments: make(map[string][]int),
        }
        for consumerID, consumer := range group.Consumers {
            consumerCpy := *consumer
            groupCpy.Consumers[consumerID] = &consumerCpy
        }
        for consumerID, partitions := range group.Assignments {
            groupCpy.Assignments[consumerID] = append([]int(nil), partitions...)
        }
        cpy[groupID] = groupCpy
    }
    return cpy
}


// NewCluster creates and initializes the cluster manager.
func NewCluster(nodeID, address, port string, clusterPort int, dataDir string, raftBindAddr string, bootstrap bool) *Cluster {
	// ... (constructor is unchanged)
    c := &Cluster{
        nodeID:       nodeID,
        address:      address,
        port:         port,
        clusterPort:  clusterPort,
        RaftBindAddr: raftBindAddr,
        peers:        make(map[string]*BrokerPeer),
        stopChan:     make(chan struct{}),
    }

    raftNode, err := NewRaftNode(dataDir, nodeID, raftBindAddr, bootstrap)
    if err != nil {
        log.Fatalf("Failed to initialize Raft: %v", err)
    }
    c.RaftNode = raftNode

    return c
}

// Start initializes the gossip protocol and background tasks.
func (c *Cluster) Start(seedAddrs []string) error {
	// ... (gossip startup is unchanged)
    config := memberlist.DefaultLANConfig()
    config.Name = c.nodeID
    config.BindAddr = c.address
    config.BindPort = c.clusterPort
    config.Events = &memberlistEventDelegate{cluster: c}

    ml, err := memberlist.Create(config)
    if err != nil {
        return fmt.Errorf("failed to create memberlist: %w", err)
    }
    c.ml = ml

    localMeta, err := json.Marshal(&BrokerPeer{
        NodeID:   c.nodeID,
        Address:  c.address,
        Port:     c.port,
        RaftAddr: c.RaftBindAddr,
    })
    if err != nil {
        ml.Shutdown()
        return fmt.Errorf("failed to marshal local metadata: %w", err)
    }
    c.ml.LocalNode().Meta = localMeta

    if len(seedAddrs) > 0 && seedAddrs[0] != "" {
        if _, err := ml.Join(seedAddrs); err != nil {
            return fmt.Errorf("failed to join cluster: %w", err)
        }
    }

    // Start background rebalancing loops
    go c.leaderLoop()

    log.Printf("[INFO] Cluster: Node %s started. Memberlist listening on %s:%d", c.nodeID, config.BindAddr, config.BindPort)
    return nil
}

// leaderLoop runs tasks that only the leader should perform.
func (c *Cluster) leaderLoop() {
    ticker := time.NewTicker(15 * time.Second) // Rebalance check interval
    defer ticker.Stop()
    for {
        select {
        case <-ticker.C:
            if c.IsLeader() {
                log.Println("[INFO] Leader performing periodic rebalance check...")
                c.RebalancePartitions()
                c.RebalanceAllConsumerGroups()
            }
        case <-c.stopChan:
            return
        }
    }
}

// Stop gracefully shuts down the cluster manager.
func (c *Cluster) Stop() {
    close(c.stopChan)
    if c.ml != nil {
        c.ml.Leave(5 * time.Second)
        c.ml.Shutdown()
    }
    if c.RaftNode != nil && c.RaftNode.Raft != nil {
        if err := c.RaftNode.Raft.Shutdown().Error(); err != nil {
            log.Printf("[ERROR] Failed to shut down Raft: %v", err)
        }
    }
    log.Println("[INFO] Cluster: Shutdown complete.")
}

// IsLeader returns true if this node is the current Raft leader.
func (c *Cluster) IsLeader() bool {
    if c.RaftNode == nil || c.RaftNode.Raft == nil {
        return false
    }
    
    // For single-node clusters, if we're the only node, we're effectively the leader
    if c.RaftNode.Raft.State() == raft.Leader {
        return true
    }
    
    // If we're in a single-node cluster and no other nodes are known, consider ourselves leader
    if c.RaftNode.Raft.State() == raft.Follower && len(c.peers) == 0 {
        // Check if we're the only node in the cluster
        config := c.RaftNode.Raft.GetConfiguration()
        if err := config.Error(); err == nil && len(config.Configuration().Servers) == 1 {
            return true
        }
    }
    
    // For development/testing: if we're the only node and no peers, consider ourselves leader
    if len(c.peers) == 0 && c.RaftNode.Raft.State() != raft.Shutdown {
        return true
    }
    
    // For multi-node clusters, check if we're the leader in Raft
    if c.RaftNode.Raft.State() == raft.Leader {
        return true
    }
    
    return false
}

// GetBrokerAddress returns the API address for a given broker ID.
func (c *Cluster) GetBrokerAddress(brokerID string) string {
	
    c.mu.RLock()
    defer c.mu.RUnlock()

    if brokerID == c.nodeID {
        return fmt.Sprintf("%s:%s", c.address, c.port)
    }

    if peer, exists := c.peers[brokerID]; exists {
        return fmt.Sprintf("%s:%s", peer.Address, peer.Port)
    }
    return ""
}

// --- Topic and Partition Management ---

func (c *Cluster) ProposeTopicCreation(name string, partitions, replicationFactor int) error {

    if !c.IsLeader() {
        leaderAddr := c.GetBrokerAddress(string(c.RaftNode.Raft.Leader()))
        return fmt.Errorf("not the leader, please send request to %s", leaderAddr)
    }

    meta := TopicMetadata{Name: name, NumPartitions: partitions, ReplicationFactor: replicationFactor}
    payload, _ := json.Marshal(meta)
    cmd := RaftCommand{Type: CmdCreateTopic, Payload: payload}
    if err := c.RaftNode.ProposeCommand(cmd); err != nil {
        return fmt.Errorf("failed to propose topic creation: %w", err)
    }

    // Trigger an immediate rebalance
    go c.RebalancePartitions()

    return nil
}

func (c *Cluster) RebalancePartitions() {
	
    if !c.IsLeader() {
        return
    }
    log.Println("[INFO] Cluster: Leader is rebalancing partition assignments.")

    c.mu.RLock()
    brokers := make([]string, 0, len(c.peers)+1)
    brokers = append(brokers, c.nodeID)
    for id := range c.peers {
        brokers = append(brokers, id)
    }
    sort.Strings(brokers)
    c.mu.RUnlock()

    if len(brokers) == 0 {
        log.Println("[WARN] Cluster: No brokers available to assign partitions.")
        return
    }

    fsmState := c.RaftNode.FSM.GetTopics()
    currentAssignments := c.RaftNode.FSM.GetAssignments()
    newAssignments := make(map[string]map[int]*PartitionAssignment)

    for _, topic := range fsmState {
        newAssignments[topic.Name] = make(map[int]*PartitionAssignment)
        repFactor := topic.ReplicationFactor
        if repFactor > len(brokers) {
            log.Printf("[WARN] Replication factor %d for topic %s is greater than broker count %d. Using broker count.", repFactor, topic.Name, len(brokers))
            repFactor = len(brokers)
        }

        for p := 0; p < topic.NumPartitions; p++ {
            if current, ok := currentAssignments[topic.Name][p]; ok && isAssignmentValid(current, brokers) {
                newAssignments[topic.Name][p] = current
                continue
            }

            replicas := make([]string, repFactor)
            for i := 0; i < repFactor; i++ {
                brokerIndex := (p + i) % len(brokers)
                replicas[i] = brokers[brokerIndex]
            }

            newAssignments[topic.Name][p] = &PartitionAssignment{
                Leader:   replicas[0],
                Replicas: replicas,
            }
        }
    }

    payload, _ := json.Marshal(newAssignments)
    cmd := RaftCommand{Type: CmdAssignPartitions, Payload: payload}
    if err := c.RaftNode.ProposeCommand(cmd); err != nil {
        log.Printf("[ERROR] Failed to propose new partition assignments: %v", err)
    }
}

// --- Consumer Group Management (New Logic) ---

func (c *Cluster) RegisterConsumer(groupID, consumerID string, topics []string) error {
    if !c.IsLeader() {
        return fmt.Errorf("not the leader")
    }

    groups := c.RaftNode.FSM.GetConsumerGroups()
    group, exists := groups[groupID]
    if !exists {
        group = &ConsumerGroup{
            ID:        groupID,
            Topics:    topics,
            Consumers: make(map[string]*Consumer),
            Assignments: make(map[string][]int),
        }
    }

    group.Consumers[consumerID] = &Consumer{
        ID:            consumerID,
        LastHeartbeat: time.Now().Unix(),
    }


    payload, _ := json.Marshal(group)
    cmd := RaftCommand{Type: CmdUpdateConsumerGroup, Payload: payload}
    if err := c.RaftNode.ProposeCommand(cmd); err != nil {
        return fmt.Errorf("failed to propose consumer registration: %w", err)
    }

    go c.rebalanceConsumerGroup(group.ID)
    return nil
}

// ConsumerHeartbeat updates a consumer's heartbeat. Leader operation.
func (c *Cluster) ConsumerHeartbeat(groupID, consumerID string) error {
    if !c.IsLeader() {
        return fmt.Errorf("not the leader")
    }

    groups := c.RaftNode.FSM.GetConsumerGroups()
    group, exists := groups[groupID]
    if !exists {
        return fmt.Errorf("consumer group %s not found", groupID)
    }
    consumer, exists := group.Consumers[consumerID]
    if !exists {
        return fmt.Errorf("consumer %s not found in group %s", consumerID, groupID)
    }

    consumer.LastHeartbeat = time.Now().Unix()

    payload, _ := json.Marshal(group)
    cmd := RaftCommand{Type: CmdUpdateConsumerGroup, Payload: payload}
    return c.RaftNode.ProposeCommand(cmd)
}

// RebalanceAllConsumerGroups iterates and rebalances all known groups.
func (c *Cluster) RebalanceAllConsumerGroups() {
    groups := c.RaftNode.FSM.GetConsumerGroups()
    for id := range groups {
        c.rebalanceConsumerGroup(id)
    }
}

// rebalanceConsumerGroup calculates and proposes new assignments for a single group.
func (c *Cluster) rebalanceConsumerGroup(groupID string) {
    if !c.IsLeader() {
        return
    }

    groups := c.RaftNode.FSM.GetConsumerGroups()
    group, exists := groups[groupID]
    if !exists {
        return
    }

    // Filter for active consumers
    activeConsumers := make([]string, 0)
    for id, consumer := range group.Consumers {
        if time.Now().Unix()-consumer.LastHeartbeat < 30 { // 30-second timeout
            activeConsumers = append(activeConsumers, id)
        }
    }
    sort.Strings(activeConsumers)

    if len(activeConsumers) == 0 {
        log.Printf("[WARN] No active consumers in group %s to assign partitions to.", groupID)
        group.Assignments = make(map[string][]int) // Clear assignments
    } else {
        // Simple round-robin assignment logic
        newAssignments := make(map[string][]int)
        for _, id := range activeConsumers {
            newAssignments[id] = []int{}
        }

        topics := c.RaftNode.FSM.GetTopics()
        partitionCounter := 0
        for _, topicName := range group.Topics {
            topic, ok := topics[topicName]
            if !ok {
                continue
            }
            for p := 0; p < topic.NumPartitions; p++ {
                consumerID := activeConsumers[partitionCounter%len(activeConsumers)]
                newAssignments[consumerID] = append(newAssignments[consumerID], p)
                partitionCounter++
            }
        }
        group.Assignments = newAssignments
    }

    // Propose the new state for the group
    payload, _ := json.Marshal(group)
    cmd := RaftCommand{Type: CmdUpdateConsumerGroup, Payload: payload}
    if err := c.RaftNode.ProposeCommand(cmd); err != nil {
        log.Printf("[ERROR] Failed to propose rebalance for group %s: %v", groupID, err)
    }
}

func (c *Cluster) handleLeave(node *memberlist.Node) {
    c.mu.Lock()
    defer c.mu.Unlock()

    delete(c.peers, node.Name)
    log.Printf("[INFO] Cluster: Peer %s (%s) left.", node.Name, node.Address())

    if c.IsLeader() {
        log.Printf("[INFO] Raft: Leader %s attempting to remove %s from the cluster.", c.nodeID, node.Name)
        future := c.RaftNode.Raft.RemoveServer(raft.ServerID(node.Name), 0, 0)
        if err := future.Error(); err != nil {
            log.Printf("[WARN] Raft: Could not remove server %s: %v", node.Name, err)
        } else {
            log.Printf("[INFO] Raft: Successfully removed %s.", node.Name)
        }
        // Trigger rebalances after a node leaves
        go c.RebalancePartitions()
        go c.RebalanceAllConsumerGroups()
    }
}

// (The rest of the file: handleJoin, memberlistEventDelegate, isAssignmentValid, HandleRaftJoin, etc. remain the same)
type memberlistEventDelegate struct {
    cluster *Cluster
}

func (d *memberlistEventDelegate) NotifyJoin(node *memberlist.Node) {
    d.cluster.handleJoin(node)
}

func (d *memberlistEventDelegate) NotifyLeave(node *memberlist.Node) {
    d.cluster.handleLeave(node)
}

func (d *memberlistEventDelegate) NotifyUpdate(node *memberlist.Node) {}

func (c *Cluster) handleJoin(node *memberlist.Node) {
    c.mu.Lock()
    defer c.mu.Unlock()

    var peer BrokerPeer
    if err := json.Unmarshal(node.Meta, &peer); err != nil {
        log.Printf("[ERROR] Cluster: Failed to parse metadata for joining node %s: %v", node.Name, err)
        return
    }
    c.peers[node.Name] = &peer
    log.Printf("[INFO] Cluster: Peer %s (%s) joined.", node.Name, node.Address())

    if c.IsLeader() {
        log.Printf("[INFO] Raft: Leader %s attempting to add %s to the cluster.", c.nodeID, peer.NodeID)
        future := c.RaftNode.Raft.AddVoter(raft.ServerID(peer.NodeID), raft.ServerAddress(peer.RaftAddr), 0, 0)
        if err := future.Error(); err != nil {
            log.Printf("[ERROR] Raft: Failed to add voter %s: %v", peer.NodeID, err)
        } else {
            log.Printf("[INFO] Raft: Successfully added %s as a voter.", peer.NodeID)
        }
    }
}

func isAssignmentValid(assignment *PartitionAssignment, activeBrokers []string) bool {
    brokerSet := make(map[string]struct{}, len(activeBrokers))
    for _, b := range activeBrokers {
        brokerSet[b] = struct{}{}
    }
    for _, replica := range assignment.Replicas {
        if _, ok := brokerSet[replica]; !ok {
            return false
        }
    }
    return true
}

func (c *Cluster) HandleRaftJoin(w http.ResponseWriter, r *http.Request) {
    if !c.IsLeader() {
        http.Error(w, "Not the leader", http.StatusServiceUnavailable)
        return
    }

    var req struct {
        NodeID   string `json:"node_id"`
        RaftAddr string `json:"raft_addr"`
    }

    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "Invalid request body", http.StatusBadRequest)
        return
    }

    log.Printf("[INFO] Raft: Received join request from Node %s at %s", req.NodeID, req.RaftAddr)
    future := c.RaftNode.Raft.AddVoter(raft.ServerID(req.NodeID), raft.ServerAddress(req.RaftAddr), 0, 0)
    if err := future.Error(); err != nil {
        log.Printf("[ERROR] Raft: Failed to add voter %s: %v", req.NodeID, err)
        http.Error(w, "Failed to add voter", http.StatusInternalServerError)
        return
    }

    log.Printf("[INFO] Raft: Successfully added %s as a voter.", req.NodeID)
    w.WriteHeader(http.StatusOK)
}

func (c *Cluster) APIAddress() string {
    return c.address
}

func (c *Cluster) APIPort() string {
    return c.port
}
