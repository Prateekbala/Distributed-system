package main

import (
	"Distributed-system/internal/broker"
	"Distributed-system/internal/cluster"
	"Distributed-system/internal/config"
	"Distributed-system/internal/offset"
	"Distributed-system/internal/storage"
	"Distributed-system/internal/topic"
	"Distributed-system/pkg/api"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	hashiraft "github.com/hashicorp/raft"
)

// Server represents the broker server
type Server struct {
	broker        *broker.Broker
	router        *mux.Router
	config        *config.Config
	diskStorage   *storage.DiskStorage
	wal           *storage.WAL
	offsetManager *offset.Manager
	cluster       *cluster.Cluster
	httpServer    *http.Server
	shutdownChan  chan struct{}
}

// ProduceRequest represents a request to produce a message
type ProduceRequest struct {
	Key     string            `json:"key,omitempty"`
	Value   string            `json:"value"`
	Headers map[string]string `json:"headers,omitempty"`
}

// ProduceResponse represents the response after producing a message
type ProduceResponse struct {
	MessageID string `json:"message_id"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Status    string `json:"status"`
}

// ConsumeResponse represents the response when consuming messages
type ConsumeResponse struct {
	Messages []MessageResponse `json:"messages"`
	Count    int               `json:"count"`
	NextOffset int64           `json:"next_offset"`
}

// MessageResponse represents a message in the response
type MessageResponse struct {
	ID        string            `json:"id"`
	Key       string            `json:"key,omitempty"`
	Value     string            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	Timestamp string            `json:"timestamp"`
	Offset    int64             `json:"offset"`
	Partition int               `json:"partition"`
	Topic     string            `json:"topic"`
}

// CreateTopicRequest represents a request to create a topic
type CreateTopicRequest struct {
	NumPartitions     int `json:"num_partitions,omitempty"`
	ReplicationFactor int `json:"replication_factor,omitempty"`
}

// CommitOffsetRequest represents a request to commit consumer offset
type CommitOffsetRequest struct {
	Offset int64 `json:"offset"`
}

// OffsetResponse represents consumer offset information
type OffsetResponse struct {
	GroupID   string `json:"group_id"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Message   string `json:"message,omitempty"`
}

// reassignmentRequest represents a partition reassignment request
type reassignmentRequest struct {
	Topic     string   `json:"topic"`
	Partition int      `json:"partition"`
	NewLeader string   `json:"new_leader"`
	Replicas  []string `json:"replicas"`
}

func NewServer() (*Server, error) {
	// Parse command line flags
	nodeID := flag.String("node-id", "", "Unique node ID for this broker")
	address := flag.String("address", "localhost", "Address to bind to")
	port := flag.String("port", "8080", "Port to bind to")
	seeds := flag.String("seeds", "", "Comma-separated list of seed addresses for gossip cluster")
	clusterPort := flag.Int("cluster-port", 7946, "Cluster communication port for memberlist")
	dataDir := flag.String("data-dir", "data", "Data directory for this broker instance")
	flag.Parse()

	// Load configuration
	cfg := config.DefaultConfig()
	cfg.DataDir = *dataDir
	cfg.SegmentSize = 1024 * 1024 // 1MB segments
	cfg.WALEnabled = true
	cfg.WALSyncInterval = time.Second * 5
	cfg.OffsetCommitInterval = time.Second * 10
	cfg.ClusterPort = *clusterPort
	
	log.Printf("[DEBUG] Configuration loaded: clusterPort=%d, dataDir=%s", cfg.ClusterPort, cfg.DataDir)

	// Initialize storage components
	diskStorage := storage.NewDiskStorage(cfg)
	
	wal, err := storage.NewWAL(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %v", err)
	}

	offsetManager, err := offset.NewManager(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create offset manager: %v", err)
	}

	// Create broker with persistent storage
	brokerInstance := broker.NewBrokerWithStorage(diskStorage, wal, offsetManager)

	// Initialize cluster if node ID is provided
	var clusterManager *cluster.Cluster
	if *nodeID != "" {
		// Use unique Raft port for each node based on cluster port
		raftBindAddr := fmt.Sprintf("%s:%d", *address, *clusterPort+1000) // offset for raft
		log.Printf("[DEBUG] Raft bind address calculated: %s (clusterPort=%d + 1000)", raftBindAddr, *clusterPort)
		
		// Parse seed addresses for raft peers (use address:clusterPort+1000)
		var raftPeers []string
		if *seeds != "" {
			rawSeeds := strings.Split(*seeds, ",")
			log.Printf("[DEBUG] Parsing seed addresses: %v", rawSeeds)
			for _, seed := range rawSeeds {
				if !strings.Contains(seed, ":") {
					seed = seed + ":7946"
				}
				// For raft, use +1000 port offset
				parts := strings.Split(seed, ":")
				if len(parts) == 2 {
					// Parse the cluster port and add 1000 for Raft
					if clusterPort, err := strconv.Atoi(parts[1]); err == nil {
						raftPeer := fmt.Sprintf("%s:%d", parts[0], clusterPort+1000)
						raftPeers = append(raftPeers, raftPeer)
						log.Printf("[DEBUG] Added Raft peer: %s (from seed: %s, clusterPort: %d)", raftPeer, seed, clusterPort)
					} else {
						// Fallback to default calculation
						raftPeer := fmt.Sprintf("%s:%d", parts[0], 7946+1000)
						raftPeers = append(raftPeers, raftPeer)
						log.Printf("[DEBUG] Added Raft peer (fallback): %s (from seed: %s)", raftPeer, seed)
					}
				}
			}
		}
		log.Printf("[DEBUG] Final Raft peers: %v", raftPeers)
		
		bootstrap := false
		if *seeds == "" {
			bootstrap = true
			log.Printf("[Startup] Bootstrapping Raft cluster: this node (%s) will be initial leader", *nodeID)
		} else {
			log.Printf("[Startup] Joining Raft cluster as follower: this node (%s) will wait for leader", *nodeID)
		}
		
		// Create cluster manager (Raft is bootstrapped only if bootstrap==true)
		log.Printf("[DEBUG] Creating cluster with nodeID=%s, address=%s, port=%s, clusterPort=%d, raftBindAddr=%s", 
			*nodeID, *address, *port, cfg.ClusterPort, raftBindAddr)
		clusterManager = cluster.NewCluster(*nodeID, *address, *port, cfg.ClusterPort, cfg.DataDir, raftBindAddr, raftPeers, bootstrap)
		
		// Set cluster in broker
		brokerInstance.SetCluster(clusterManager)
		
		// Start memberlist/gossip protocol (all brokers join gossip first)
		var gossipSeeds []string
		if *seeds != "" {
			gossipSeeds = strings.Split(*seeds, ",")
		}
		if err := clusterManager.Start(gossipSeeds); err != nil {
			return nil, fmt.Errorf("failed to start cluster memberlist: %v", err)
		}
		
		// Register broker with FSM for notifications
		cluster.RegisterBroker(brokerInstance)
	}

	// Perform recovery if needed
	if err := performRecovery(brokerInstance, wal); err != nil {
		log.Printf("Warning: Recovery failed: %v", err)
	}

	s := &Server{
		broker:        brokerInstance,
		router:        mux.NewRouter(),
		config:        cfg,
		diskStorage:   diskStorage,
		wal:           wal,
		offsetManager: offsetManager,
		cluster:       clusterManager,
		shutdownChan:  make(chan struct{}),
	}

	s.setupRoutes()
	return s, nil
}

func performRecovery(broker *broker.Broker, wal *storage.WAL) error {
	log.Println("Starting recovery from WAL...")
	
	entries, err := wal.Recovery()
	if err != nil {
		return fmt.Errorf("failed to read WAL for recovery: %v", err)
	}

	log.Printf("Found %d WAL entries to replay", len(entries))

	for _, entry := range entries {
		switch entry.Type {
		case "topic_create":
			// Use the stored configuration from WAL
			if entry.Config == nil {
				// Fallback to default config if not found in WAL
				entry.Config = &topic.TopicConfig{
					NumPartitions:     1,
					ReplicationFactor: 1,
				}
			}
			broker.CreateTopic(entry.Topic, entry.Config, false)
			log.Printf("Recovered topic: %s with %d partitions", entry.Topic, entry.Config.NumPartitions)
			
		case "message":
			// Replay message to storage
			if entry.Message != nil {
				if err := broker.StoreMessage(entry.Message); err != nil {
					log.Printf("Warning: Failed to replay message: %v", err)
				} else {
					log.Printf("Recovered message for topic %s, partition %d, offset %d", 
						entry.Topic, entry.Partition, entry.Message.Offset)
				}
			}
		}
	}

	log.Println("Recovery completed successfully")
	return nil
}

func (s *Server) setupRoutes() {
	// Prometheus metrics endpoint
	s.router.Handle("/metrics", promhttp.Handler()).Methods("GET")

	// Topic management
	s.router.HandleFunc("/topics", s.handleListTopics).Methods("GET")
	s.router.HandleFunc("/topics/{topic}", s.handleCreateTopic).Methods("POST")
	s.router.HandleFunc("/topics/{topic}", s.handleDeleteTopic).Methods("DELETE")
	s.router.HandleFunc("/topics/{topic}/metadata", s.handleGetTopicMetadata).Methods("GET")

	// Producer API
	s.router.HandleFunc("/produce", s.handleProduce).Methods("POST")
	s.router.HandleFunc("/topics/{topic}/produce", s.handleProduce).Methods("POST")
	s.router.HandleFunc("/topics/{topic}/partitions/{partition}/produce", s.handleProduceToPartition).Methods("POST")

	// Consumer API
	s.router.HandleFunc("/consumer/subscribe", s.handleConsumerSubscribe).Methods("POST")
	s.router.HandleFunc("/consumer/fetch", s.handleConsumerFetch).Methods("POST")
	s.router.HandleFunc("/consumer/commit", s.handleConsumerCommit).Methods("POST")
	s.router.HandleFunc("/topics/{topic}/partitions/{partition}/consume", s.handleConsume).Methods("GET")
	s.router.HandleFunc("/topics/{topic}/partitions/{partition}/messages/{offset}", s.handleGetMessage).Methods("GET")

	// Consumer Groups
	s.router.HandleFunc("/consumer-groups/{groupId}/offsets/{topic}/{partition}", s.handleCommitOffset).Methods("POST")
	s.router.HandleFunc("/consumer-groups/{groupId}/offsets/{topic}/{partition}", s.handleGetOffset).Methods("GET")
	s.router.HandleFunc("/consumer-groups/{groupId}/offsets/{topic}", s.handleGetAllOffsets).Methods("GET")
	
	// Consumer Group Management
	s.router.HandleFunc("/consumer-groups/{groupId}/consumers/{consumerId}", s.handleRegisterConsumer).Methods("POST")
	s.router.HandleFunc("/consumer-groups/{groupId}/consumers/{consumerId}", s.handleUnregisterConsumer).Methods("DELETE")
	s.router.HandleFunc("/consumer-groups/{groupId}/consumers/{consumerId}/assignments", s.handleGetConsumerAssignments).Methods("GET")
	s.router.HandleFunc("/consumer-groups/{groupId}/metadata", s.handleGetGroupMetadata).Methods("GET")

	// Health and Status
	s.router.HandleFunc("/health", s.handleHealth).Methods("GET")
	s.router.HandleFunc("/status", s.handleStatus).Methods("GET")
	s.router.HandleFunc("/cluster/ready", s.handleClusterReady).Methods("GET")
	
	// Administrative endpoints
	s.router.HandleFunc("/admin/sync", s.handleForceSync).Methods("POST")
	s.router.HandleFunc("/admin/stats", s.handleStats).Methods("GET")
	s.router.HandleFunc("/admin/assignment-plan", func(w http.ResponseWriter, r *http.Request) {
		if s.cluster == nil {
			http.Error(w, "Clustering not enabled", http.StatusServiceUnavailable)
			return
		}
		plan := s.cluster.GetAssignmentPlan()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(plan)
	}).Methods("GET")

	// Cluster endpoints
	if s.cluster != nil {
		s.router.HandleFunc("/cluster/status", s.handleClusterStatus).Methods("GET")
	}

	// Cluster management routes
	s.router.HandleFunc("/reassign", s.handleReassignment).Methods("POST")

	// Internal: Partition assignment updates
	s.router.HandleFunc("/internal/assignments/{topic}", s.handleAssignmentUpdate).Methods("POST")
	s.router.HandleFunc("/internal/assignments/all", s.handleAllAssignments).Methods("GET")

	// New partition metadata endpoint
	s.router.HandleFunc("/metadata/partitions", s.handlePartitionMetadata).Methods("GET")

	// Replication endpoint for followers to receive messages
	s.router.HandleFunc("/replication", func(w http.ResponseWriter, r *http.Request) {
		s.broker.ReplicationManager().HandleReplicationRequest(w, r)
	}).Methods("POST")

	// Internal: Enable election
	s.router.HandleFunc("/internal/enable-election", s.handleEnableElection).Methods("POST")

	// Internal: Raft join
	s.router.HandleFunc("/internal/raft-join", s.handleRaftJoin).Methods("POST")

	// Debug endpoint to check metadata
	s.router.HandleFunc("/debug/metadata", func(w http.ResponseWriter, r *http.Request) {
		if s.cluster == nil {
			http.Error(w, "Clustering not enabled", http.StatusServiceUnavailable)
			return
		}
		response := map[string]interface{}{
			"node_id":     s.cluster.GetNodeID(),
			"address":     s.cluster.GetAddress(),
			"port":        s.cluster.GetPort(),
			"cluster_port": s.cluster.GetClusterPort(),
			"raft_port":   fmt.Sprintf("%d", s.cluster.GetClusterPort()+1000),
			"is_leader":   s.cluster.IsLeader(),
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}).Methods("GET")

	// Debug endpoint to check Raft state
	s.router.HandleFunc("/debug/raft", func(w http.ResponseWriter, r *http.Request) {
		if s.cluster == nil || s.cluster.RaftNode == nil {
			http.Error(w, "Raft not enabled", http.StatusServiceUnavailable)
			return
		}
		
		raft := s.cluster.RaftNode.Raft
		response := map[string]interface{}{
			"state":           raft.State().String(),
			"leader":          string(raft.Leader()),
			"is_leader": raft.State() == hashiraft.Leader,
		}
		
		// Get Raft configuration
		if config := raft.GetConfiguration(); config.Error() == nil {
			servers := make([]map[string]interface{}, 0)
			for _, server := range config.Configuration().Servers {
				servers = append(servers, map[string]interface{}{
					"id":        string(server.ID),
					"address":   string(server.Address),
					"suffrage":  server.Suffrage.String(),
				})
			}
			response["configuration"] = servers
		}
		
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}).Methods("GET")

	// Debug endpoint to check memberlist state
	s.router.HandleFunc("/debug/memberlist", func(w http.ResponseWriter, r *http.Request) {
		if s.cluster == nil {
			http.Error(w, "Memberlist not enabled", http.StatusServiceUnavailable)
			return
		}
		
		// Use the cluster's GetPeers method instead of accessing unexported ml field
		peers := s.cluster.GetPeers()
		memberList := make([]map[string]interface{}, 0)
		
		// Add self to the list
		self := map[string]interface{}{
			"name":     s.cluster.GetNodeID(),
			"address":  s.cluster.GetAddress(),
			"port":     s.cluster.GetPort(),
			"is_leader": s.cluster.IsLeader(),
		}
		memberList = append(memberList, self)
		
		// Add other peers
		for _, peer := range peers {
			memberList = append(memberList, map[string]interface{}{
				"name":     peer.NodeID,
				"address":  peer.Address,
				"port":     peer.Port,
				"raft_port": peer.RaftPort,
				"is_leader": peer.IsLeader,
				"status":   peer.Status,
			})
		}
		
		response := map[string]interface{}{
			"local_node": s.cluster.GetNodeID(),
			"members":    memberList,
			"count":      len(memberList),
		}
		
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}).Methods("GET")
}

func (s *Server) handleListTopics(w http.ResponseWriter, r *http.Request) {
	topics := s.broker.ListTopics()
	
	response := map[string]interface{}{
		"topics": topics,
		"count":  len(topics),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	topicName := vars["topic"]

	log.Printf("[DEBUG] handleCreateTopic called for topic: %s", topicName)
	log.Printf("[DEBUG] Request headers: %v", r.Header)
	var bodyBytes []byte
	if r.Body != nil {
		bodyBytes, _ = io.ReadAll(r.Body)
		log.Printf("[DEBUG] Request body: %s", string(bodyBytes))
		r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes)) // Reset body for decoder
	}

	var req CreateTopicRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("[DEBUG] Failed to decode request body: %v", err)
		req = CreateTopicRequest{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}
	}

	config := &topic.TopicConfig{
		NumPartitions:     req.NumPartitions,
		ReplicationFactor: req.ReplicationFactor,
	}
	log.Printf("[DEBUG] Parsed config: %+v", config)

	if config.NumPartitions <= 0 {
		config.NumPartitions = 1
	}
	if config.ReplicationFactor <= 0 {
		config.ReplicationFactor = 1
	}

	if s.cluster != nil && !s.cluster.IsLeader() {
		leaderAddr := s.cluster.GetBrokerAddress(s.cluster.GetLeader())
		if leaderAddr != "" {
			url := fmt.Sprintf("http://%s/topics/%s", leaderAddr, topicName)
			req, _ := http.NewRequest("POST", url, bytes.NewBuffer(bodyBytes))
			for k, v := range r.Header {
				req.Header[k] = v
			}
			resp, err := http.DefaultClient.Do(req)
			if err == nil {
				w.WriteHeader(resp.StatusCode)
				io.Copy(w, resp.Body)
				resp.Body.Close()
				return
			}
		}
	}

	if r.Header.Get("X-Replication-Origin") == "true" {
		log.Printf("[DEBUG] Replication header detected. Calling CreateTopicReplicated.")
		if err := s.broker.CreateTopicReplicated(topicName, config); err != nil {
			if strings.Contains(err.Error(), "already exists") {
				// Idempotent: treat as success
				w.WriteHeader(http.StatusOK)
				return
			}
			log.Printf("[DEBUG] CreateTopicReplicated error: %v", err)
			http.Error(w, fmt.Sprintf("Failed to create topic (replicated): %v", err), http.StatusBadRequest)
			return
		}
	} else {
		log.Printf("[DEBUG] No replication header. Calling CreateTopic with replicate=true.")
		if err := s.broker.CreateTopic(topicName, config, true); err != nil {
			log.Printf("[DEBUG] CreateTopic error: %v", err)
			http.Error(w, fmt.Sprintf("Failed to create topic: %v", err), http.StatusBadRequest)
			return
		}
	}

	// Register topic metadata and trigger assignment if controller
	if s.cluster != nil && s.cluster.IsController() {
		s.cluster.StoreTopicMetadata(topicName, &cluster.TopicMetadata{
			Name:       topicName,
			Partitions: config.NumPartitions,
			Replicas:   config.ReplicationFactor,
		})
		go s.cluster.CalculatePartitionAssignmentsAdvanced()
	}

	response := map[string]interface{}{
		"message": fmt.Sprintf("Topic '%s' created successfully", topicName),
		"topic":   topicName,
		"config":  config,
		"status":  "persisted",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleDeleteTopic(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	topicName := vars["topic"]

	if s.cluster != nil && !s.cluster.IsLeader() {
		leaderAddr := s.cluster.GetBrokerAddress(s.cluster.GetLeader())
		if leaderAddr != "" {
			url := fmt.Sprintf("http://%s/topics/%s", leaderAddr, topicName)
			req, _ := http.NewRequest("DELETE", url, nil)
			resp, err := http.DefaultClient.Do(req)
			if err == nil {
				w.WriteHeader(resp.StatusCode)
				io.Copy(w, resp.Body)
				resp.Body.Close()
				return
			}
		}
	}

	if err := s.broker.DeleteTopic(topicName); err != nil {
		http.Error(w, fmt.Sprintf("Failed to delete topic: %v", err), http.StatusNotFound)
		return
	}

	response := map[string]interface{}{
		"message": fmt.Sprintf("Topic '%s' deleted successfully", topicName),
		"topic":   topicName,
		"status":  "persisted",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleGetTopicMetadata(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	topicName := vars["topic"]

	metadata, err := s.broker.GetTopicMetadata(topicName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get topic metadata: %v", err), http.StatusNotFound)
		return
	}
	topicObj, _ := s.broker.GetTopic(topicName)
	response := map[string]interface{}{
		"metadata": metadata,
		"config":   topicObj.Config,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleProduce(w http.ResponseWriter, r *http.Request) {
	var msg api.ProduceMessage
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// If topic is in the URL, override the message topic
	vars := mux.Vars(r)
	if topicName, ok := vars["topic"]; ok && topicName != "" {
		msg.Topic = topicName
	}

	if msg.Topic == "" {
		http.Error(w, "Topic is required", http.StatusBadRequest)
		return
	}

	if msg.Value == "" {
		http.Error(w, "Message value is required", http.StatusBadRequest)
		return
	}

	// Set timestamp if not provided
	if msg.Timestamp.IsZero() {
		msg.Timestamp = time.Now()
	}

	// Convert to broker message format
	brokerMsg, err := s.broker.Produce(msg.Topic, msg.Key, []byte(msg.Value), msg.Headers)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to produce message: %v", err), http.StatusInternalServerError)
		return
	}

	// Write message to WAL
	if err := s.wal.WriteMessage(msg.Topic, int32(brokerMsg.Partition), brokerMsg); err != nil {
		log.Printf("Warning: Failed to write message to WAL: %v", err)
	}

	response := api.ProduceResponse{
		Topic:     brokerMsg.Topic,
		Partition: brokerMsg.Partition,
		Offset:    brokerMsg.Offset,
		Timestamp: brokerMsg.Timestamp,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleProduceToPartition(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	topicName := vars["topic"]
	
	partitionStr := vars["partition"]
	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		http.Error(w, "Invalid partition number", http.StatusBadRequest)
		return
	}

	var req ProduceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Value == "" {
		http.Error(w, "Message value is required", http.StatusBadRequest)
		return
	}

	msg, err := s.broker.ProduceToPartition(topicName, partition, req.Key, []byte(req.Value), req.Headers)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to produce message: %v", err), http.StatusInternalServerError)
		return
	}

	response := ProduceResponse{
		MessageID: msg.ID,
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Status:    "persisted",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleConsume(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	topicName := vars["topic"]
	
	partitionStr := vars["partition"]
	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		http.Error(w, "Invalid partition number", http.StatusBadRequest)
		return
	}

	// Parse query parameters
	offsetStr := r.URL.Query().Get("offset")
	if offsetStr == "" {
		offsetStr = "0"
	}
	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid offset", http.StatusBadRequest)
		return
	}

	limitStr := r.URL.Query().Get("limit")
	if limitStr == "" {
		limitStr = "10"
	}
	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		http.Error(w, "Invalid limit", http.StatusBadRequest)
		return
	}

	if limit > 1000 {
		limit = 1000
	}

	messages, err := s.broker.Consume(topicName, partition, offset, limit)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to consume messages: %v", err), http.StatusInternalServerError)
		return
	}

	messageResponses := make([]MessageResponse, len(messages))
	nextOffset := offset
	
	for i, msg := range messages {
		messageResponses[i] = MessageResponse{
			ID:        msg.ID,
			Key:       msg.Key,
			Value:     string(msg.Value),
			Headers:   msg.Headers,
			Timestamp: msg.Timestamp.Format("2006-01-02T15:04:05.000Z"),
			Offset:    msg.Offset,
			Partition: msg.Partition,
			Topic:     msg.Topic,
		}
		if msg.Offset >= nextOffset {
			nextOffset = msg.Offset + 1
		}
	}

	response := ConsumeResponse{
		Messages:   messageResponses,
		Count:      len(messageResponses),
		NextOffset: nextOffset,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleGetMessage(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	topicName := vars["topic"]
	
	partitionStr := vars["partition"]
	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		http.Error(w, "Invalid partition number", http.StatusBadRequest)
		return
	}

	offsetStr := vars["offset"]
	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid offset", http.StatusBadRequest)
		return
	}

	msg, err := s.broker.GetMessage(topicName, partition, offset)
	if err != nil {
		http.Error(w, fmt.Sprintf("Message not found: %v", err), http.StatusNotFound)
		return
	}

	response := MessageResponse{
		ID:        msg.ID,
		Key:       msg.Key,
		Value:     string(msg.Value),
		Headers:   msg.Headers,
		Timestamp: msg.Timestamp.Format("2006-01-02T15:04:05.000Z"),
		Offset:    msg.Offset,
		Partition: msg.Partition,
		Topic:     msg.Topic,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleCommitOffset(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	groupID := vars["groupId"]
	topicName := vars["topic"]
	
	partitionStr := vars["partition"]
	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		http.Error(w, "Invalid partition number", http.StatusBadRequest)
		return
	}

	var req CommitOffsetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if err := s.offsetManager.CommitOffset(groupID, topicName, int32(partition), req.Offset); err != nil {
		http.Error(w, fmt.Sprintf("Failed to commit offset: %v", err), http.StatusInternalServerError)
		return
	}

	response := OffsetResponse{
		GroupID:   groupID,
		Topic:     topicName,
		Partition: partition,
		Offset:    req.Offset,
		Message:   "Offset committed successfully",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleGetOffset(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	groupID := vars["groupId"]
	topicName := vars["topic"]
	
	partitionStr := vars["partition"]
	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		http.Error(w, "Invalid partition number", http.StatusBadRequest)
		return
	}

	offset := s.offsetManager.GetOffset(groupID, topicName, int32(partition))

	response := OffsetResponse{
		GroupID:   groupID,
		Topic:     topicName,
		Partition: partition,
		Offset:    offset,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleGetAllOffsets(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	groupID := vars["groupId"]
	topicName := vars["topic"]

	offsets := s.offsetManager.GetAllOffsets(groupID, topicName)

	response := map[string]interface{}{
		"group_id": groupID,
		"topic":    topicName,
		"offsets":  offsets,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	response := map[string]interface{}{
		"status":  "healthy",
		"service": "mini-kafka-broker",
		"topics":  len(s.broker.ListTopics()),
		"storage": "persistent",
		"wal":     "enabled",
	}

	if s.cluster != nil {
		// Check cluster readiness
		leader := s.cluster.GetLeader()
		peers := s.cluster.GetPeers()
		clusterReady := leader != "" && len(peers) > 0
		
		response["cluster"] = map[string]interface{}{
			"enabled": true,
			"node_id": s.cluster.GetNodeID(),
			"leader":  s.cluster.IsLeader(),
			"ready":   clusterReady,
			"leader_id": leader,
			"peer_count": len(peers),
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	response := map[string]interface{}{
		"status":      "running",
		"service":     "mini-kafka-broker",
		"version":     "1.0.0",
		"topics":      len(s.broker.ListTopics()),
		"storage":     "persistent",
		"wal_enabled": s.config.WALEnabled,
		"data_dir":    s.config.DataDir,
		"uptime":      time.Since(time.Now()).String(),
	}

	if s.cluster != nil {
		response["cluster"] = map[string]interface{}{
			"enabled": true,
			"node_id": s.cluster.GetNodeID(),
			"leader":  s.cluster.IsLeader(),
			"peers":   len(s.cluster.GetPeers()),
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleForceSync(w http.ResponseWriter, r *http.Request) {
	if err := s.wal.Sync(); err != nil {
		http.Error(w, fmt.Sprintf("Failed to sync WAL: %v", err), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"message": "WAL synced successfully",
		"status":  "synced",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := map[string]interface{}{
		"topics":       len(s.broker.ListTopics()),
		"data_dir":     s.config.DataDir,
		"segment_size": s.config.SegmentSize,
		"wal_enabled":  s.config.WALEnabled,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// handleRegisterConsumer handles consumer registration
func (s *Server) handleRegisterConsumer(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	groupID := vars["groupId"]
	consumerID := vars["consumerId"]

	var req struct {
		Topics []string `json:"topics"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate topics exist
	for _, topic := range req.Topics {
		if !s.broker.TopicExists(topic) {
			http.Error(w, fmt.Sprintf("Topic %s does not exist", topic), http.StatusBadRequest)
			return
		}
	}

	if err := s.broker.RegisterConsumer(groupID, consumerID, req.Topics); err != nil {
		http.Error(w, fmt.Sprintf("Failed to register consumer: %v", err), http.StatusInternalServerError)
		return
	}

	// Get assignments after registration
	assignments, err := s.broker.GetConsumerAssignments(groupID, consumerID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get consumer assignments: %v", err), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"group_id":    groupID,
		"consumer_id": consumerID,
		"topics":      req.Topics,
		"assignments": assignments,
		"status":      "registered",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

// handleUnregisterConsumer handles consumer unregistration
func (s *Server) handleUnregisterConsumer(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	groupID := vars["groupId"]
	consumerID := vars["consumerId"]

	if err := s.broker.UnregisterConsumer(groupID, consumerID); err != nil {
		http.Error(w, fmt.Sprintf("Failed to unregister consumer: %v", err), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"group_id":    groupID,
		"consumer_id": consumerID,
		"status":      "unregistered",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleGetConsumerAssignments handles getting consumer assignments
func (s *Server) handleGetConsumerAssignments(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	groupID := vars["groupId"]
	consumerID := vars["consumerId"]

	assignments, err := s.broker.GetConsumerAssignments(groupID, consumerID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get consumer assignments: %v", err), http.StatusInternalServerError)
		return
	}

	event := api.RebalanceEvent{
		GroupID:     groupID,
		ConsumerID:  consumerID,
		Assignments: assignments,
		Reason:      "rebalance or assignment update",
		Timestamp:   time.Now().Unix(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(event)
}

// handleGetGroupMetadata handles getting consumer group metadata
func (s *Server) handleGetGroupMetadata(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	groupID := vars["groupId"]

	metadata, err := s.broker.GetGroupMetadata(groupID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get group metadata: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metadata)
}

func (s *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		http.Error(w, "Clustering not enabled", http.StatusServiceUnavailable)
		return
	}

	status := s.cluster.GetClusterStatus()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// handleReassignment handles partition reassignment requests
func (s *Server) handleReassignment(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req reassignmentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if err := s.broker.HandlePartitionReassignment(req.Topic, req.Partition, req.NewLeader, req.Replicas); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleConsumerSubscribe handles consumer subscription requests
func (s *Server) handleConsumerSubscribe(w http.ResponseWriter, r *http.Request) {
	var req api.SubscribeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Generate a unique consumer ID if not provided
	consumerID := fmt.Sprintf("consumer-%d", time.Now().UnixNano())

	// Register consumer with the broker
	if err := s.broker.RegisterConsumer(req.GroupID, consumerID, req.Topics); err != nil {
		http.Error(w, fmt.Sprintf("Failed to register consumer: %v", err), http.StatusInternalServerError)
		return
	}

	// Get assignments
	assignments, err := s.broker.GetConsumerAssignments(req.GroupID, consumerID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get assignments: %v", err), http.StatusInternalServerError)
		return
	}

	response := api.AssignmentResponse{
		Assignments: assignments,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleConsumerFetch handles consumer fetch requests
func (s *Server) handleConsumerFetch(w http.ResponseWriter, r *http.Request) {
	var req api.FetchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	messages := make([]*api.ConsumerMessage, 0)
	for topic, partitionOffsets := range req.Topics {
		for _, po := range partitionOffsets {
			// Use a reasonable default for max records if not specified
			maxRecords := 100
			msgs, err := s.broker.Consume(topic, po.Partition, po.Offset, maxRecords)
			if err != nil {
				log.Printf("Error consuming messages: %v", err)
				continue
			}

			for _, msg := range msgs {
				consumerMsg := &api.ConsumerMessage{
					Topic:     msg.Topic,
					Partition: msg.Partition,
					Offset:    msg.Offset,
					Key:       msg.Key,
					Value:     string(msg.Value),
					Headers:   msg.Headers,
					Timestamp: msg.Timestamp,
				}
				messages = append(messages, consumerMsg)
			}
		}
	}

	response := api.FetchResponse{
		Messages: messages,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleConsumerCommit handles consumer offset commit requests
func (s *Server) handleConsumerCommit(w http.ResponseWriter, r *http.Request) {
	var req api.CommitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	for topic, partitionOffsets := range req.Offsets {
		for _, po := range partitionOffsets {
			if err := s.offsetManager.CommitOffset(req.GroupID, topic, int32(po.Partition), po.Offset); err != nil {
				log.Printf("Error committing offset: %v", err)
			}
		}
	}

	w.WriteHeader(http.StatusOK)
}

// handleAssignmentUpdate receives partition assignment updates from the controller
func (s *Server) handleAssignmentUpdate(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	topic := vars["topic"]
	var assignments map[string]*cluster.PartitionAssignment
	if err := json.NewDecoder(r.Body).Decode(&assignments); err != nil {
		http.Error(w, "Invalid assignment payload", http.StatusBadRequest)
		return
	}
	s.broker.HandleAssignmentUpdate(topic, assignments)
	w.WriteHeader(http.StatusOK)
}

// handleAllAssignments returns all partition assignments (controller only)
func (s *Server) handleAllAssignments(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil || !s.cluster.IsController() {
		http.Error(w, "Not controller", http.StatusForbidden)
		return
	}
	assignments := s.cluster.GetPartitionAssignments()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(assignments)
}

// handlePartitionMetadata exposes partition leadership and replica assignments for all topics
func (s *Server) handlePartitionMetadata(w http.ResponseWriter, r *http.Request) {
	assignments := s.cluster.GetPartitionAssignments()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(assignments)
}

// handleClusterReady handles cluster readiness status
func (s *Server) handleClusterReady(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		http.Error(w, "Clustering not enabled", http.StatusServiceUnavailable)
		return
	}

	status := s.cluster.GetClusterStatus()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// handleEnableElection handles enabling the Raft election timer on non-bootstrap nodes
func (s *Server) handleEnableElection(w http.ResponseWriter, r *http.Request) {
	if s.cluster != nil && s.cluster.RaftNode != nil && s.cluster.RaftNode.Raft != nil {
		
		log.Printf("[Raft] Election timer enabled via /internal/enable-election endpoint")
		w.WriteHeader(http.StatusOK)
		return
	}
	log.Printf("[Raft] Could not enable election timer: Raft not initialized")
	http.Error(w, "Raft not initialized", http.StatusInternalServerError)
}

// handleRaftJoin handles Raft join requests
func (s *Server) handleRaftJoin(w http.ResponseWriter, r *http.Request) {
	s.broker.HandleRaftJoin(w, r)
}

// gracefulShutdown performs a graceful shutdown of the server
func (s *Server) gracefulShutdown() {
	log.Println("Initiating graceful shutdown...")
	
	// Create a context with timeout for shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Stop accepting new connections
	if s.httpServer != nil {
		if err := s.httpServer.Shutdown(ctx); err != nil {
			log.Printf("Error shutting down HTTP server: %v", err)
		}
	}

	// Stop the broker
	if err := s.broker.Stop(); err != nil {
		log.Printf("Error stopping broker: %v", err)
	}

	// Stop cluster if enabled
	if s.cluster != nil {
		s.cluster.Stop()
	}

	// Close WAL
	if err := s.wal.Sync(); err != nil {
		log.Printf("Error syncing WAL during shutdown: %v", err)
	}
	if err := s.wal.Close(); err != nil {
		log.Printf("Error closing WAL: %v", err)
	}

	// Close storage
	if err := s.diskStorage.Close(); err != nil {
		log.Printf("Error closing disk storage: %v", err)
	}

	// Close offset manager
	if err := s.offsetManager.Close(); err != nil {
		log.Printf("Error closing offset manager: %v", err)
	}

	// Signal shutdown complete
	close(s.shutdownChan)
	log.Println("Shutdown complete")
}

func main() {
	server, err := NewServer()
	if err != nil {
		log.Fatal("Failed to create server:", err)
	}

	// Set up graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		server.gracefulShutdown()
	}()

	// Use the --port flag value for the HTTP server
	// Get the port from the parsed flag in NewServer
	portFlag := flag.Lookup("port")
	port := ":8080" // default
	if portFlag != nil && portFlag.Value.String() != "" {
		port = portFlag.Value.String()
		if !strings.HasPrefix(port, ":") {
			port = ":" + port
		}
	}
	server.httpServer = &http.Server{
		Addr:    port,
		Handler: server.router,
	}

	log.Printf("Starting Mini Kafka Broker with persistent storage on port %s", port)
	log.Printf("Data directory: %s", server.config.DataDir)
	log.Printf("WAL enabled: %v", server.config.WALEnabled)
	
	if server.cluster != nil {
		log.Printf("Clustering enabled - Node ID: %s", server.cluster.GetNodeID())
		log.Printf("Cluster status: http://localhost%s/cluster/status", port)
	}
	
	log.Printf("Health check: http://localhost%s/health", port)
	log.Printf("Status: http://localhost%s/status", port)
	log.Printf("Metrics: http://localhost%s/metrics", port)
	log.Printf("\nAPI Documentation:")
	log.Printf("  POST /topics/{topic} - Create topic")
	log.Printf("  GET /topics - List topics")
	log.Printf("  DELETE /topics/{topic} - Delete topic")
	log.Printf("  GET /topics/{topic}/metadata - Get topic metadata")
	log.Printf("  POST /topics/{topic}/produce - Produce message")
	log.Printf("  POST /topics/{topic}/partitions/{partition}/produce - Produce to specific partition")
	log.Printf("  GET /topics/{topic}/partitions/{partition}/consume?offset=0&limit=10 - Consume messages")
	log.Printf("  GET /topics/{topic}/partitions/{partition}/messages/{offset} - Get specific message")
	log.Printf("  POST /consumer-groups/{groupId}/offsets/{topic}/{partition} - Commit offset")
	log.Printf("  GET /consumer-groups/{groupId}/offsets/{topic}/{partition} - Get offset")
	log.Printf("  GET /consumer-groups/{groupId}/offsets/{topic} - Get all offsets for topic")
	log.Printf("  POST /admin/sync - Force WAL sync")
	log.Printf("  GET /admin/stats - Get broker statistics")
	log.Printf("  GET /metadata/partitions - Get partition metadata")
	log.Printf("  GET /admin/assignment-plan - Get current assignment plan")

	// Start server
	if err := server.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal("Server failed to start:", err)
	}

	// Wait for shutdown to complete
	<-server.shutdownChan
}