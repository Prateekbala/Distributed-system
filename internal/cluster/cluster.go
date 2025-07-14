package cluster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"hash/fnv"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// BrokerPeer represents information about a broker in the cluster
type BrokerPeer struct {
	NodeID    string    `json:"node_id"`
	Address   string    `json:"address"`
	Port      string    `json:"port"`
	RaftPort  string    `json:"raft_port"`
	IsLeader  bool      `json:"is_leader"`
	LastSeen  time.Time `json:"last_seen"`
	Status    string    `json:"status"` // "active", "inactive", "failed"
}

// BrokerLoad represents the load metrics for a broker
type BrokerLoad struct {
	NodeID        string    `json:"node_id"`
	PartitionCount int      `json:"partition_count"`
	LeaderCount    int      `json:"leader_count"`
	FollowerCount  int      `json:"follower_count"`
	CPUUsage       float64  `json:"cpu_usage"`
	MemoryUsage    float64  `json:"memory_usage"`
	LastUpdated    time.Time `json:"last_updated"`
}

// PartitionAssignment represents the assignment of a partition
type PartitionAssignment struct {
	Leader   string   `json:"leader"`
	Replicas []string `json:"replicas"`
}

// Cluster manages the broker cluster state
type Cluster struct {
	nodeID       string
	address      string
	port         string
	clusterPort  int
	peers        map[string]*BrokerPeer
	leaderID     string
	isLeader     bool
	lastElection time.Time
	mu           sync.RWMutex

	// Memberlist for gossip protocol
	ml      *memberlist.Memberlist
	members map[string]*memberlist.Node

	// Partition reassignment
	reassignmentChan chan *PartitionReassignment

	// Load balancing
	brokerLoads     map[string]*BrokerLoad
	rebalancePeriod time.Duration
	stopChan        chan struct{}

	// --- Controller/Metadata Management ---
	metadataStore *MetadataStore // Centralized metadata store (controller only)
	RaftNode *RaftNode // Raft integration
}

// MetadataStore holds cluster-wide metadata (topics, partitions, assignments)
type MetadataStore struct {
	Topics      map[string]*TopicMetadata
	Assignments map[string]map[int]*PartitionAssignment // topic -> partition -> assignment
	mu          sync.RWMutex
}

type TopicMetadata struct {
	Name       string
	Partitions int
	Replicas   int
}

// NewMetadataStore creates a new metadata store
func NewMetadataStore() *MetadataStore {
	return &MetadataStore{
		Topics:      make(map[string]*TopicMetadata),
		Assignments: make(map[string]map[int]*PartitionAssignment),
	}
}

// PartitionReassignment represents a partition reassignment request
type PartitionReassignment struct {
	Topic     string   `json:"topic"`
	Partition int      `json:"partition"`
	NewLeader string   `json:"new_leader"`
	Replicas  []string `json:"replicas"`
}

// NewCluster creates a new cluster manager
func NewCluster(nodeID, address, port string, clusterPort int, dataDir string, raftBindAddr string, raftPeers []string, bootstrap bool) *Cluster {
	log.Printf("[DEBUG] NewCluster called with nodeID=%s, address=%s, port=%s, clusterPort=%d, raftBindAddr=%s", 
		nodeID, address, port, clusterPort, raftBindAddr)
	
	// Calculate expected Raft port for verification
	expectedRaftPort := clusterPort + 1000
	log.Printf("[DEBUG] Expected Raft port: %d (clusterPort=%d + 1000)", expectedRaftPort, clusterPort)
	
	c := &Cluster{
		nodeID:           nodeID,
		address:          address,
		port:             port,
		clusterPort:      clusterPort,
		peers:            make(map[string]*BrokerPeer),
		members:          make(map[string]*memberlist.Node),
		reassignmentChan: make(chan *PartitionReassignment, 100),
		brokerLoads:      make(map[string]*BrokerLoad),
		rebalancePeriod:  time.Minute * 5,
		stopChan:         make(chan struct{}),
	}
	
	log.Printf("[DEBUG] Cluster struct created with clusterPort=%d", c.clusterPort)
	
	// Initialize RaftNode
	raftNode, err := NewRaftNodeWithElectionControl(dataDir, nodeID, raftBindAddr, raftPeers, bootstrap)
	if err != nil {
		log.Fatalf("Failed to initialize Raft: %v", err)
	}
	c.RaftNode = raftNode
	return c
}

// Start starts the cluster management with memberlist
func (c *Cluster) Start(seedAddrs []string) error {
	// Configure memberlist with optimized settings for lower CPU usage
	config := memberlist.DefaultLANConfig()
	config.Name = c.nodeID
	config.BindAddr = c.address
	config.BindPort = c.clusterPort
	
	// Optimize for lower CPU usage - less frequent gossip
	config.GossipInterval = 2 * time.Second      // Reduced from 200ms to 2s
	config.PushPullInterval = 30 * time.Second   // Reduced from 10s to 30s
	config.ProbeInterval = 5 * time.Second       // Reduced probe frequency
	config.ProbeTimeout = 3 * time.Second        // Faster timeout
	config.SuspicionMult = 3                     // Reduced suspicion multiplier
	
	config.UDPBufferSize = 1400                  // Optimize buffer size

	// Create memberlist instance
	ml, err := memberlist.Create(config)
	if err != nil {
		return fmt.Errorf("failed to create memberlist: %v", err)
	}
	c.ml = ml

	// Set initial metadata once
	c.setLocalMetadata()

	// Join existing cluster if seed addresses provided
	if len(seedAddrs) > 0 {
		_, err := ml.Join(seedAddrs)
		if err != nil {
			return fmt.Errorf("failed to join cluster: %v", err)
		}
		// Multiple metadata updates to ensure propagation
		time.Sleep(1 * time.Second)
		c.setLocalMetadata()
		time.Sleep(2 * time.Second)
		c.setLocalMetadata()
		time.Sleep(3 * time.Second)
		c.setLocalMetadata()
	}

	// Start background tasks with reduced frequency
	go c.membershipLoop()
	go c.handlePartitionReassignments()
	go c.loadRebalancingLoop()

	// Optimized leader state updates - less frequent
	go func() {
		// Initial frequent updates for first 30 seconds
		initialTicker := time.NewTicker(1 * time.Second) // More frequent during initial period
		defer initialTicker.Stop()
		
		// Regular updates after initial period
		regularTicker := time.NewTicker(10 * time.Second)
		defer regularTicker.Stop()
		
		var lastIsLeader bool
		var lastLeaderID string
		startTime := time.Now()
		
		for {
			select {
			case <-initialTicker.C:
				if time.Since(startTime) > 30*time.Second {
					initialTicker.Stop()
					continue
				}
				currentIsLeader := c.IsLeader()
				currentLeaderID := c.GetLeader()
				c.mu.Lock()
				c.isLeader = currentIsLeader
				c.leaderID = currentLeaderID
				c.mu.Unlock()
				// Update metadata more frequently during initial period
				c.setLocalMetadata() // Always update during initial period
				if currentIsLeader != lastIsLeader || currentLeaderID != lastLeaderID {
					lastIsLeader = currentIsLeader
					lastLeaderID = currentLeaderID
				}
			case <-regularTicker.C:
				if time.Since(startTime) <= 30*time.Second {
					continue
				}
				currentIsLeader := c.IsLeader()
				currentLeaderID := c.GetLeader()
				c.mu.Lock()
				c.isLeader = currentIsLeader
				c.leaderID = currentLeaderID
				c.mu.Unlock()
				// Only update metadata if state actually changed
				if currentIsLeader != lastIsLeader || currentLeaderID != lastLeaderID {
					c.setLocalMetadata()
					lastIsLeader = currentIsLeader
					lastLeaderID = currentLeaderID
				}
			case <-c.stopChan:
				return
			}
		}
	}()

	log.Printf("Cluster started for node %s with optimized gossip protocol", c.nodeID)
	return nil
}

// membershipLoop updates peer information from memberlist
func (c *Cluster) membershipLoop() {
	ticker := time.NewTicker(5 * time.Second) 
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.updatePeersFromMemberlist()
		case <-c.stopChan:
			return
		}
	}
}

// --- Consistent Hash Ring Utility ---
type hashRing struct {
	nodes []string
}

func newHashRing(brokers []string) *hashRing {
	sorted := make([]string, len(brokers))
	copy(sorted, brokers)
	sort.Strings(sorted)
	return &hashRing{nodes: sorted}
}

// getN returns the primary and N-1 next nodes for replication
func (h *hashRing) getN(key string, n int) []string {
	if len(h.nodes) == 0 || n == 0 {
		return nil
	}
	hash := fnv32(key)
	idx := int(hash) % len(h.nodes)
	result := make([]string, 0, n)
	for i := 0; i < n; i++ {
		result = append(result, h.nodes[(idx+i)%len(h.nodes)])
	}
	return result
}

func fnv32(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

// --- Advanced Assignment Features ---
// Sticky assignment: prefer to keep previous assignments if possible
// Rack-awareness: spread replicas across racks if metadata is present
// Assignment inspection: expose a function to get the current assignment plan

// getBrokerRacks returns a map of brokerID -> rack (if available)
func (c *Cluster) getBrokerRacks() map[string]string {
	racks := make(map[string]string)
	for id, peer := range c.peers {
		if strings.HasPrefix(peer.Status, "rack:") {
			racks[id] = strings.TrimPrefix(peer.Status, "rack:")
		}
	}
	racks[c.nodeID] = "default" // fallback
	return racks
}

// calculatePartitionAssignmentsAdvanced does sticky and rack-aware assignment
func (c *Cluster) CalculatePartitionAssignmentsAdvanced() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.metadataStore == nil {
		c.metadataStore = NewMetadataStore()
	}
	brokers := make([]string, 0, len(c.peers)+1)
	brokers = append(brokers, c.nodeID)
	for id := range c.peers {
		brokers = append(brokers, id)
	}
	if len(brokers) == 0 {
		return
	}
	ring := newHashRing(brokers)
	racks := c.getBrokerRacks()
	for topic, meta := range c.metadataStore.Topics {
		for p := 0; p < meta.Partitions; p++ {
			key := fmt.Sprintf("%s-%d", topic, p)
			// Sticky: try to keep previous leader if still present
			var prevLeader string
			if c.metadataStore.Assignments[topic] != nil {
				if prev, ok := c.metadataStore.Assignments[topic][p]; ok && contains(brokers, prev.Leader) {
					prevLeader = prev.Leader
				}
			}
			var leader string
			if prevLeader != "" {
				leader = prevLeader
			} else {
				leader = ring.getN(key, 1)[0]
			}
			// Rack-aware: try to spread replicas across racks
			replicas := []string{leader}
			usedRacks := map[string]struct{}{racks[leader]: {}}
			for _, b := range brokers {
				if b == leader || len(replicas) >= meta.Replicas {
					continue
				}
				rack := racks[b]
				if _, used := usedRacks[rack]; !used {
					replicas = append(replicas, b)
					usedRacks[rack] = struct{}{}
				}
			}
			// If not enough racks, fill with any broker
			for _, b := range brokers {
				if len(replicas) >= meta.Replicas {
					break
				}
				if !contains(replicas, b) {
					replicas = append(replicas, b)
				}
			}
			assignment := &PartitionAssignment{
				Leader:   leader,
				Replicas: replicas,
			}
			// Propose the assignment via Raft (only the leader should call this)
			if c.IsLeader() {
				c.StorePartitionAssignment(topic, p, assignment)
			}
		}
	}
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// GetAssignmentPlan returns a copy of the current assignment plan
func (c *Cluster) GetAssignmentPlan() map[string]map[int]*PartitionAssignment {
	return c.GetPartitionAssignments()
}

// --- Partition Assignment Calculation ---
func (c *Cluster) calculatePartitionAssignmentsConsistentHashing() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.metadataStore == nil {
		c.metadataStore = NewMetadataStore()
	}
	brokers := make([]string, 0, len(c.peers)+1)
	brokers = append(brokers, c.nodeID) // always include self
	for id := range c.peers {
		brokers = append(brokers, id)
	}
	if len(brokers) == 0 {
		return
	}
	ring := newHashRing(brokers)
	for topic, meta := range c.metadataStore.Topics {
		if c.metadataStore.Assignments[topic] == nil {
			c.metadataStore.Assignments[topic] = make(map[int]*PartitionAssignment)
		}
		for p := 0; p < meta.Partitions; p++ {
			key := fmt.Sprintf("%s-%d", topic, p)
			replicas := ring.getN(key, meta.Replicas)
			if len(replicas) == 0 {
				continue
			}
			assignment := &PartitionAssignment{
				Leader:   replicas[0],
				Replicas: replicas,
			}
			c.metadataStore.Assignments[topic][p] = assignment
			// Propagate to brokers
			go c.notifyBrokersOfAssignment(topic, p, assignment)
		}
	}
}

// updatePeersFromMemberlist updates peer information from memberlist
func (c *Cluster) updatePeersFromMemberlist() {
	nodes := c.ml.Members()
	c.mu.Lock()
	defer c.mu.Unlock()

	oldPeers := make(map[string]struct{})
	for id := range c.peers {
		oldPeers[id] = struct{}{}
	}
	changed := false
	
	// Update peers from memberlist
	for _, node := range nodes {
		if node.Name == c.nodeID {
			continue
		}

		// Parse metadata if available
		var metadata map[string]interface{}
		if len(node.Meta) > 0 {
			log.Printf("[DEBUG] Raw metadata for node %s: %s", node.Name, string(node.Meta))
			if err := json.Unmarshal(node.Meta, &metadata); err != nil {
				log.Printf("Failed to parse metadata for node %s: %v", node.Name, err)
				continue
			}
			// Debug logging for metadata
			log.Printf("[DEBUG] Parsed metadata for node %s: %+v", node.Name, metadata)
		} else {
			log.Printf("[DEBUG] No metadata available for node %s (Meta length: %d)", node.Name, len(node.Meta))
		}

		// Update or create peer
		peer, exists := c.peers[node.Name]
		if !exists {
			changed = true
			// Extract peer info from metadata or use defaults
			peerAddress := node.Addr.String()
			peerPort := "8080" // default
			peerRaftPort := "8966" // default
			
			if address, ok := metadata["address"].(string); ok {
				peerAddress = address
			}
			if port, ok := metadata["port"].(string); ok {
				peerPort = port
			}
			if raftPort, ok := metadata["raft_port"].(string); ok {
				peerRaftPort = raftPort
			} else if raftPort, ok := metadata["raft_port"].(float64); ok {
				// Handle case where raft_port is stored as number
				peerRaftPort = fmt.Sprintf("%.0f", raftPort)
			}
			
			log.Printf("[DEBUG] Creating peer %s with address=%s, port=%s, raft_port=%s, metadata=%+v", 
				node.Name, peerAddress, peerPort, peerRaftPort, metadata)
			
			peer = &BrokerPeer{
				NodeID:   node.Name,
				Address:  peerAddress,
				Port:     peerPort,
				RaftPort: peerRaftPort,
				Status:   "active",
				LastSeen: time.Now(),
			}
			
			// Update leader status if available in metadata
			if isLeader, ok := metadata["is_leader"].(bool); ok {
				peer.IsLeader = isLeader
			} else {
				peer.IsLeader = false
			}
			
			c.peers[node.Name] = peer
			log.Printf("[Cluster] New peer discovered: %s (%s:%s, raft:%s)", peer.NodeID, peer.Address, peer.Port, peer.RaftPort)
			// Add to Raft cluster - Check if we need to join existing cluster
			if c.RaftNode != nil {
				go c.handleNewPeerRaft(peer)
			}
		} else {
			peer.LastSeen = time.Now()
			peer.Status = "active"
			// Update port/address/raft_port if available in metadata
			if port, ok := metadata["port"].(string); ok {
				peer.Port = port
			}
			if address, ok := metadata["address"].(string); ok {
				peer.Address = address
			}
			if raftPort, ok := metadata["raft_port"].(string); ok {
				peer.RaftPort = raftPort
			} else if raftPort, ok := metadata["raft_port"].(float64); ok {
				// Handle case where raft_port is stored as number
				peer.RaftPort = fmt.Sprintf("%.0f", raftPort)
			}
			// Update leader status if available in metadata
			if isLeader, ok := metadata["is_leader"].(bool); ok {
				peer.IsLeader = isLeader
			}
		}
		delete(oldPeers, node.Name)
	}
	
	// Remove peers not in memberlist
	for id := range oldPeers {
		changed = true
		log.Printf("[Cluster] Peer %s left or failed", id)
		// Remove from Raft cluster if this node is the leader
		if c.RaftNode != nil && c.IsLeader() {
			log.Printf("[Raft] Leader removing broker %s from Raft", id)
			err := c.RemoveBrokerFromRaft(id)
			if err != nil {
				log.Printf("[Raft] Failed to remove broker %s from Raft: %v", id, err)
			} else {
				log.Printf("[Raft] Removed broker %s from Raft", id)
				c.PrintRaftConfig()
			}
		}
		delete(c.peers, id)
	}
	
	// If controller and membership changed, recalculate assignments
	if c.IsLeader() && changed {
		log.Printf("[Cluster] Membership changed, recalculating partition assignments...")
		go c.CalculatePartitionAssignmentsAdvanced()
	}
}


// Stop stops the cluster management
func (c *Cluster) Stop() {
	close(c.stopChan)
	if c.ml != nil {
		c.ml.Leave(time.Second * 5)
		c.ml.Shutdown()
	}
}

// GetNodeID returns the node ID of this broker
func (c *Cluster) GetNodeID() string {
	return c.nodeID
}

// GetLeader returns the current Raft leader node ID
func (c *Cluster) GetLeader() string {
	if c.RaftNode == nil || c.RaftNode.Raft == nil {
		log.Printf("[DEBUG] GetLeader: RaftNode or Raft is nil")
		return ""
	}
	leader := string(c.RaftNode.Raft.Leader())
	log.Printf("[DEBUG] GetLeader: leader=%s", leader)
	return leader
}

// IsLeader returns true if this node is the Raft leader
func (c *Cluster) IsLeader() bool {
	if c.RaftNode == nil || c.RaftNode.Raft == nil {
		log.Printf("[DEBUG] IsLeader: RaftNode or Raft is nil")
		return false
	}
	state := c.RaftNode.Raft.State()
	isLeader := state == raft.Leader
	log.Printf("[DEBUG] IsLeader: state=%s, isLeader=%v", state, isLeader)
	return isLeader
}

// GetPeers returns all peers in the cluster
func (c *Cluster) GetPeers() map[string]*BrokerPeer {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	peers := make(map[string]*BrokerPeer)
	for id, peer := range c.peers {
		peers[id] = peer
	}
	return peers
}

// GetClusterStatus returns the current cluster status
func (c *Cluster) GetClusterStatus() *ClusterStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Get leader info from Raft
	leaderID := c.GetLeader()
	isLeader := c.IsLeader()
	
	log.Printf("[DEBUG] GetClusterStatus for node %s: leaderID=%s, isLeader=%v", c.nodeID, leaderID, isLeader)

	// Build peer list including self
	peers := make([]*BrokerPeer, 0, len(c.peers)+1)
	// Add self
	self := &BrokerPeer{
		NodeID:   c.nodeID,
		Address:  c.address,
		Port:     c.port,
		RaftPort: fmt.Sprintf("%d", c.clusterPort+1000),
		IsLeader: isLeader,
		LastSeen: time.Now(),
		Status:   "active",
	}
	peers = append(peers, self)
	log.Printf("[DEBUG] Self peer: %+v", self)
	
	// Add other peers
	for _, peer := range c.peers {
		peers = append(peers, peer)
		log.Printf("[DEBUG] Peer: %+v", peer)
	}

	status := &ClusterStatus{
		NodeID:    c.nodeID,
		LeaderID:  leaderID,
		IsLeader:  isLeader,
		PeerCount: len(peers),
		Peers:     peers,
	}
	
	log.Printf("[DEBUG] ClusterStatus: %+v", status)
	return status
}

// ClusterStatus represents the current cluster status
type ClusterStatus struct {
	NodeID    string        `json:"node_id"`
	LeaderID  string        `json:"leader_id"`
	IsLeader  bool          `json:"is_leader"`
	PeerCount int           `json:"peer_count"`
	Peers     []*BrokerPeer `json:"peers"`
}

// UpdateBrokerLoad updates the load metrics for a broker
func (c *Cluster) UpdateBrokerLoad(nodeID string, load *BrokerLoad) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	c.brokerLoads[nodeID] = load
	load.LastUpdated = time.Now()
	
	// Update metadata in memberlist with load information
	if c.ml != nil {
		c.ml.UpdateNode(time.Second)
	} else {
		log.Printf("[WARN] memberlist is nil in UpdateBrokerLoad for node %s", nodeID)
	}
}

// handlePartitionReassignments handles partition reassignment requests
func (c *Cluster) handlePartitionReassignments() {
	for {
		select {
		case reassignment := <-c.reassignmentChan:
			c.processPartitionReassignment(reassignment)
		case <-c.stopChan:
			return
		}
	}
}

// processPartitionReassignment processes a partition reassignment
func (c *Cluster) processPartitionReassignment(reassignment *PartitionReassignment) {
	// Notify all brokers about the reassignment
	c.mu.RLock()
	peers := make([]*BrokerPeer, 0, len(c.peers))
	for _, peer := range c.peers {
		if peer.Status == "active" {
			peers = append(peers, peer)
		}
	}
	c.mu.RUnlock()

	for _, peer := range peers {
		go func(peer *BrokerPeer) {
			url := fmt.Sprintf("http://%s:%s/reassign", peer.Address, peer.Port)
			data, err := json.Marshal(reassignment)
			if err != nil {
				log.Printf("Failed to marshal reassignment request: %v", err)
				return
			}

			resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
			if err != nil {
				log.Printf("Failed to send reassignment to %s: %v", peer.NodeID, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				log.Printf("Reassignment failed for %s with status %d", peer.NodeID, resp.StatusCode)
			}
		}(peer)
	}
}

// TriggerPartitionReassignment triggers a partition reassignment
func (c *Cluster) TriggerPartitionReassignment(topic string, partition int, newLeader string, replicas []string) {
	reassignment := &PartitionReassignment{
		Topic:     topic,
		Partition: partition,
		NewLeader: newLeader,
		Replicas:  replicas,
	}

	select {
	case c.reassignmentChan <- reassignment:
		log.Printf("Triggered partition reassignment: %s-%d", topic, partition)
	default:
		log.Printf("Reassignment queue is full, dropping request for %s-%d", topic, partition)
	}
}

// loadRebalancingLoop periodically checks if rebalancing is needed
func (c *Cluster) loadRebalancingLoop() {
	ticker := time.NewTicker(c.rebalancePeriod)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			if c.isLeader {
				c.checkAndRebalance()
			}
		case <-c.stopChan:
			return
		}
	}
}

// checkAndRebalance checks if rebalancing is needed and triggers it
func (c *Cluster) checkAndRebalance() {
	c.mu.RLock()
	loads := make(map[string]*BrokerLoad)
	for id, load := range c.brokerLoads {
		loads[id] = load
	}
	c.mu.RUnlock()

	// Skip if we don't have enough data
	if len(loads) < 2 {
		return
	}

	// Calculate average load
	var totalPartitions, totalLeaders int
	for _, load := range loads {
		totalPartitions += load.PartitionCount
		totalLeaders += load.LeaderCount
	}
	
	avgPartitions := float64(totalPartitions) / float64(len(loads))
	avgLeaders := float64(totalLeaders) / float64(len(loads))

	// Find overloaded and underloaded brokers
	var overloaded, underloaded []string
	for nodeID, load := range loads {
		// Consider a broker overloaded if it has 20% more than average
		if float64(load.PartitionCount) > avgPartitions*1.2 || 
		   float64(load.LeaderCount) > avgLeaders*1.2 {
			overloaded = append(overloaded, nodeID)
		}
		// Consider a broker underloaded if it has 20% less than average
		if float64(load.PartitionCount) < avgPartitions*0.8 || 
		   float64(load.LeaderCount) < avgLeaders*0.8 {
			underloaded = append(underloaded, nodeID)
		}
	}

	// If we have both overloaded and underloaded brokers, trigger rebalancing
	if len(overloaded) > 0 && len(underloaded) > 0 {
		c.rebalanceLoad(overloaded, underloaded)
	}
}

// rebalanceLoad rebalances load between overloaded and underloaded brokers
func (c *Cluster) rebalanceLoad(overloaded, underloaded []string) {
	c.mu.RLock()
	loads := make(map[string]*BrokerLoad)
	for id, load := range c.brokerLoads {
		loads[id] = load
	}
	c.mu.RUnlock()

	// Get all partition assignments
	assignments := c.GetPartitionAssignments()
	
	// For each overloaded broker
	for _, overloadedID := range overloaded {
		// Find partitions to move
		for topic, partitions := range assignments {
			for partition, assignment := range partitions {
				// Skip if this broker is not the leader
				if assignment.Leader != overloadedID {
					continue
				}

				// Find the most underloaded broker
				var targetBroker string
				minLoad := float64(1<<63 - 1)
				for _, underloadedID := range underloaded {
					load := loads[underloadedID]
					if float64(load.PartitionCount) < minLoad {
						minLoad = float64(load.PartitionCount)
						targetBroker = underloadedID
					}
				}

				if targetBroker != "" {
					// Create new replica list
					newReplicas := make([]string, 0)
					for _, replica := range assignment.Replicas {
						if replica != overloadedID {
							newReplicas = append(newReplicas, replica)
						}
					}
					newReplicas = append(newReplicas, targetBroker)

					// Trigger reassignment
					c.TriggerPartitionReassignment(topic, partition, targetBroker, newReplicas)
					
					// Update load metrics
					loads[overloadedID].PartitionCount--
					loads[overloadedID].LeaderCount--
					loads[targetBroker].PartitionCount++
					loads[targetBroker].LeaderCount++
					
					// Break if broker is no longer overloaded
					if float64(loads[overloadedID].PartitionCount) <= float64(loads[targetBroker].PartitionCount) {
						break
					}
				}
			}
		}
	}
}

// GetPartitionAssignments returns the current partition assignments from Raft FSM
func (c *Cluster) GetPartitionAssignments() map[string]map[int]*PartitionAssignment {
	if c.RaftNode != nil && c.RaftNode.FSM != nil {
		c.RaftNode.FSM.mu.Lock()
		defer c.RaftNode.FSM.mu.Unlock()
		result := make(map[string]map[int]*PartitionAssignment)
		for topic, partitions := range c.RaftNode.FSM.state.Assignments {
			result[topic] = make(map[int]*PartitionAssignment)
			for pid, assignment := range partitions {
				copyAssignment := *assignment
				result[topic][pid] = &copyAssignment
			}
		}
		return result
	}
	return make(map[string]map[int]*PartitionAssignment)
}

// GetBrokerAddress returns the address of a broker by its ID
func (c *Cluster) GetBrokerAddress(brokerID string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if peer, exists := c.peers[brokerID]; exists {
		return fmt.Sprintf("%s:%s", peer.Address, peer.Port)
	}
	return ""
}

// IsController returns true if this node is the controller (cluster leader)
func (c *Cluster) IsController() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.isLeader
}

// StoreTopicMetadata via Raft
func (c *Cluster) StoreTopicMetadata(topic string, meta *TopicMetadata) {
	if c.RaftNode != nil && c.IsLeader() {
		payload, _ := json.Marshal(meta)
		cmd := RaftCommand{Type: "create_topic", Payload: payload}
		_ = c.RaftNode.ProposeCommand(cmd)
	}
}

// StorePartitionAssignment via Raft
func (c *Cluster) StorePartitionAssignment(topic string, partition int, assignment *PartitionAssignment) {
	if c.RaftNode != nil && c.IsLeader() {
		ap := struct {
			Topic     string
			Partition int
			Assignment *PartitionAssignment
		}{
			Topic: topic, Partition: partition, Assignment: assignment,
		}
		payload, _ := json.Marshal(ap)
		cmd := RaftCommand{Type: "assign_partition", Payload: payload}
		_ = c.RaftNode.ProposeCommand(cmd)
	}
}

// DeleteTopic via Raft
func (c *Cluster) DeleteTopic(topic string) {
	if c.RaftNode != nil && c.IsLeader() {
		payload, _ := json.Marshal(topic)
		cmd := RaftCommand{Type: "delete_topic", Payload: payload}
		_ = c.RaftNode.ProposeCommand(cmd)
	}
}

// GetTopicMetadata returns topic metadata from Raft FSM
func (c *Cluster) GetTopicMetadata(topic string) (*TopicMetadata, bool) {
	if c.RaftNode != nil && c.RaftNode.FSM != nil {
		c.RaftNode.FSM.mu.Lock()
		defer c.RaftNode.FSM.mu.Unlock()
		meta, ok := c.RaftNode.FSM.state.Topics[topic]
		return meta, ok
	}
	return nil, false
}

// TriggerLeaderReelection triggers leader re-election for all partitions led by a failed broker
func (c *Cluster) TriggerLeaderReelection(failedBrokerID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.metadataStore == nil {
		return
	}
	for topic, partitions := range c.metadataStore.Assignments {
		for partition, assignment := range partitions {
			if assignment.Leader == failedBrokerID {
				// Elect new leader from ISR (excluding failed broker)
				newISR := make([]string, 0, len(assignment.Replicas))
				for _, replica := range assignment.Replicas {
					if replica != failedBrokerID {
						newISR = append(newISR, replica)
					}
				}
				if len(newISR) == 0 {
					continue // No eligible leader
				}
				assignment.Leader = newISR[0]
				assignment.Replicas = newISR
				// Notify all brokers of new assignment
				go c.notifyBrokersOfAssignment(topic, partition, assignment)
			}
		}
	}
}

// notifyBrokersOfAssignment notifies all brokers of a new assignment for a partition
func (c *Cluster) notifyBrokersOfAssignment(topic string, partition int, assignment *PartitionAssignment) {
	peers := c.GetPeers()
	for peerID, peer := range peers {
		url := fmt.Sprintf("http://%s:%s/internal/assignments/%s", peer.Address, peer.Port, topic)
		assignments := map[string]*PartitionAssignment{
			fmt.Sprintf("%d", partition): assignment,
		}
		data, err := json.Marshal(assignments)
		if err != nil {
			log.Printf("Failed to marshal assignment for %s: %v", peerID, err)
			continue
		}
		go func(url string, data []byte, peerID string) {
			resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
			if err != nil {
				log.Printf("Failed to send assignment to %s: %v", peerID, err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				log.Printf("Broker %s returned error status %d for assignment update", peerID, resp.StatusCode)
			}
		}(url, data, peerID)
	}
}

func (c *Cluster) handleNewPeerRaft(peer *BrokerPeer) {
	time.Sleep(2 * time.Second) // Wait for peer to be ready
	
	// Use the peer's advertised Raft port from metadata
	raftAddr := fmt.Sprintf("%s:%s", peer.Address, peer.RaftPort)
	
	// If I'm the leader, add the new peer
	if c.IsLeader() {
		log.Printf("[Raft] Leader attempting to add broker %s to Raft at %s", peer.NodeID, raftAddr)
		
		// Check if this address is already in use by checking current Raft configuration
		if c.RaftNode != nil && c.RaftNode.Raft != nil {
			f := c.RaftNode.Raft.GetConfiguration()
			if err := f.Error(); err == nil {
				config := f.Configuration()
				for _, server := range config.Servers {
					if string(server.Address) == raftAddr {
						log.Printf("[Raft] Address %s is already in use by server %s, skipping", raftAddr, server.ID)
						return
					}
				}
			}
		}
		
		err := c.AddBrokerToRaft(peer.NodeID, raftAddr)
		if err != nil {
			log.Printf("[Raft] Failed to add broker %s to Raft: %v", peer.NodeID, err)
		} else {
			log.Printf("[Raft] Successfully added broker %s to Raft at %s", peer.NodeID, raftAddr)
			c.PrintRaftConfig()
		}
	} else {
		// If I'm not the leader and I'm not in any Raft cluster, 
		// try to join the existing cluster
		if c.RaftNode.Raft.State() == raft.Follower {
			return // Already part of cluster
		}
		
		// Find the leader and request to join
		for _, p := range c.peers {
			if p.IsLeader {
				c.requestToJoinRaft(p)
				break
			}
		}
	}
}

func (c *Cluster) requestToJoinRaft(leaderPeer *BrokerPeer) {
	raftPort := c.clusterPort + 1000
	myRaftAddr := fmt.Sprintf("%s:%d", c.address, raftPort)
	
	joinRequest := struct {
		NodeID   string `json:"node_id"`
		RaftAddr string `json:"raft_addr"`
	}{
		NodeID:   c.nodeID,
		RaftAddr: myRaftAddr,
	}
	
	data, _ := json.Marshal(joinRequest)
	url := fmt.Sprintf("http://%s:%s/internal/raft-join", leaderPeer.Address, leaderPeer.Port)
	
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Printf("[Raft] Failed to request join from leader %s: %v", leaderPeer.NodeID, err)
		return
	}
	defer resp.Body.Close()
	
	if resp.StatusCode == http.StatusOK {
		log.Printf("[Raft] Successfully requested to join Raft cluster via leader %s", leaderPeer.NodeID)
	} else {
		log.Printf("[Raft] Join request rejected by leader %s: %d", leaderPeer.NodeID, resp.StatusCode)
	}
}

// Add setLocalMetadata to propagate is_leader, port, and address
func (c *Cluster) setLocalMetadata() {
	raftPort := fmt.Sprintf("%d", c.clusterPort+1000)
	metadata := map[string]interface{}{
		"is_leader": c.IsLeader(),
		"port":      c.port,
		"address":   c.address,
		"raft_port": raftPort,
	}
	metaBytes, _ := json.Marshal(metadata)
	
	// Debug logging
	log.Printf("[DEBUG] Setting local metadata for node %s: address=%s, port=%s, raft_port=%s, is_leader=%v, clusterPort=%d, metadata=%+v", 
		c.nodeID, c.address, c.port, raftPort, c.IsLeader(), c.clusterPort, metadata)
	
	// Only log metadata changes, not every update
	if c.ml != nil {
		if c.ml.LocalNode() != nil {
			c.ml.LocalNode().Meta = metaBytes
			log.Printf("[DEBUG] Updated memberlist metadata for node %s with size %d bytes", c.nodeID, len(metaBytes))
		} else {
			log.Printf("[DEBUG] LocalNode is nil for node %s", c.nodeID)
		}
		// Trigger metadata update less frequently
		c.ml.UpdateNode(5 * time.Second)
	} else {
		log.Printf("[DEBUG] memberlist is nil for node %s", c.nodeID)
	}
}

// AddBrokerToRaft adds a broker as a Raft voter (to be called on join)
func (c *Cluster) AddBrokerToRaft(nodeID, addr string) error {
	if c.RaftNode == nil || c.RaftNode.Raft == nil {
		return fmt.Errorf("raft not initialized")
	}
	
	// Check if this address is already in use
	f := c.RaftNode.Raft.GetConfiguration()
	if err := f.Error(); err == nil {
		config := f.Configuration()
		for _, server := range config.Servers {
			if string(server.Address) == addr {
				if string(server.ID) == nodeID {
					log.Printf("[Raft] Node %s is already in cluster at %s", nodeID, addr)
					return nil // Already added
				} else {
					return fmt.Errorf("address %s is already in use by server %s", addr, server.ID)
				}
			}
		}
	}
	
	// Step 1: Add as non-voter
	var lastErr error
	for i := 0; i < 5; i++ {
		future := c.RaftNode.Raft.AddNonvoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
		err := future.Error()
		if err == nil {
			log.Printf("[Raft] Added %s as non-voter at %s", nodeID, addr)
			break
		}
		lastErr = err
		log.Printf("[Raft] AddNonvoter failed for %s (%s): %v (retrying...)", nodeID, addr, err)
		time.Sleep(2 * time.Second)
	}
	if lastErr != nil {
		return lastErr
	}
	// Step 2: Wait for log catch-up (simple sleep for now, can be improved)
	time.Sleep(3 * time.Second)
	// Step 3: Promote to voter
	for i := 0; i < 5; i++ {
		future := c.RaftNode.Raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
		err := future.Error()
		if err == nil {
			log.Printf("[Raft] Promoted %s to voter", nodeID)
			// Try to re-enable election timer on the new node (via HTTP call)
			go func() {
				url := fmt.Sprintf("http://%s/internal/enable-election", addr)
				client := &http.Client{Timeout: 2 * time.Second}
				resp, err := client.Post(url, "application/json", nil)
				if err != nil {
					log.Printf("[Raft] Could not notify %s to enable election timer: %v", nodeID, err)
					return
				}
				resp.Body.Close()
				log.Printf("[Raft] Notified %s to enable election timer", nodeID)
			}()
			return nil
		}
		lastErr = err
		log.Printf("[Raft] AddVoter (promote) failed for %s: %v (retrying...)", nodeID, err)
		time.Sleep(2 * time.Second)
	}
	return lastErr
}

// RemoveBrokerFromRaft removes a broker from Raft (to be called on leave)
func (c *Cluster) RemoveBrokerFromRaft(nodeID string) error {
	if c.RaftNode == nil || c.RaftNode.Raft == nil {
		return fmt.Errorf("raft not initialized")
	}
	future := c.RaftNode.Raft.RemoveServer(raft.ServerID(nodeID), 0, 0)
	return future.Error()
}

// Add PrintRaftConfig to print current Raft configuration
func (c *Cluster) PrintRaftConfig() {
	if c.RaftNode == nil || c.RaftNode.Raft == nil {
		log.Println("[Raft] Not initialized")
		return
	}
	f := c.RaftNode.Raft.GetConfiguration()
	if err := f.Error(); err != nil {
		log.Printf("[Raft] GetConfiguration error: %v", err)
		return
	}
	for _, srv := range f.Configuration().Servers {
		log.Printf("[Raft] Server: ID=%s, Address=%s, Suffrage=%s", srv.ID, srv.Address, srv.Suffrage)
	}
}

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

// GetClusterPort returns the cluster port
func (c *Cluster) GetClusterPort() int {
	return c.clusterPort
}

// GetAddress returns the node address
func (c *Cluster) GetAddress() string {
	return c.address
}

// GetPort returns the node port
func (c *Cluster) GetPort() string {
	return c.port
}
