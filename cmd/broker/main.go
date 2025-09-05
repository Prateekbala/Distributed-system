// cmd/broker/main.go
package main

import (
	"Distributed-system/internal/broker"
	"Distributed-system/internal/cluster"
	"Distributed-system/internal/config"
	"Distributed-system/internal/message"
	"Distributed-system/internal/offset"
	"Distributed-system/internal/storage"
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
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/mux"
)

// Server represents the main application server, wrapping the broker and HTTP router.
type Server struct {
	broker     *broker.Broker
	cluster    *cluster.Cluster
	config     *config.Config
	router     *mux.Router
	httpServer *http.Server
}

// --- API Request/Response Structs ---

type CreateTopicRequest struct {
	NumPartitions     int `json:"num_partitions"`
	ReplicationFactor int `json:"replication_factor"`
}

type ProduceRequest struct {
	Key     string            `json:"key,omitempty"`
	Value   string            `json:"value"`
	Headers map[string]string `json:"headers,omitempty"`
}

type ProduceResponse struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Status    string `json:"status"`
}

type ConsumeResponse struct {
	Messages   []*message.Message `json:"messages"`
	Count      int                `json:"count"`
	NextOffset int64              `json:"next_offset"`
}

type CommitOffsetRequest struct {
	Offset int64 `json:"offset"`
}

// --- New Consumer Group Structs ---
type RegisterConsumerRequest struct {
	ConsumerID string   `json:"consumer_id"`
	Topics     []string `json:"topics"`
}

type ConsumerHeartbeatRequest struct {
	ConsumerID string `json:"consumer_id"`
}

// NewServer initializes all components of the broker application.
func NewServer() (*Server, error) {
	// --- 1. Flag Parsing ---
	nodeID := flag.String("node-id", "broker-1", "Unique node ID for this broker")
	address := flag.String("address", "localhost", "Address to bind to for the API")
	port := flag.String("port", "8080", "Port for the API")
	clusterPort := flag.Int("cluster-port", 7946, "Port for the internal gossip protocol")
	dataDir := flag.String("data-dir", "data", "Root directory for logs and data")
	seeds := flag.String("seeds", "", "Comma-separated list of seed node addresses (e.g., localhost:7946)")
	bootstrap := flag.Bool("bootstrap", false, "Bootstrap a new Raft cluster (only for the very first node)")
	flag.Parse()

	if *nodeID == "" {
		return nil, fmt.Errorf("node-id is a required flag")
	}

	// --- 2. Configuration ---
	cfg := config.DefaultConfig()
	cfg.DataDir = filepath.Join(*dataDir, *nodeID)
	cfg.WALEnabled = true
	cfg.WALSyncInterval = time.Second * 1
	cfg.SegmentSize = 10 * 1024 * 1024 // 10MB segments

	// --- 3. Storage Initialization ---
	// REFACTOR: Pass the cluster object to DiskStorage so it can create topic directories
	// when a new topic is detected in the FSM.
	diskStorage := storage.NewDiskStorage(cfg)
	wal, err := storage.NewWAL(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}
	offsetManager, err := offset.NewManager(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create offset manager: %w", err)
	}

	// --- 4. Cluster Initialization ---
	raftBindAddr := fmt.Sprintf("%s:%d", *address, *clusterPort+1000)
	clusterManager := cluster.NewCluster(*nodeID, *address, *port, *clusterPort, cfg.DataDir, raftBindAddr, *bootstrap)

	var seedList []string
	if *seeds != "" {
		seedList = strings.Split(*seeds, ",")
	}
	if err := clusterManager.Start(seedList); err != nil {
		return nil, fmt.Errorf("failed to start cluster: %w", err)
	}

	// Wait for Raft to stabilize if this is a bootstrap node
	if *bootstrap {
		time.Sleep(2 * time.Second)
	}

	// --- 5. Broker Initialization ---
	// REFACTOR: Broker no longer needs local managers. It gets all state from the cluster.
	brokerInstance := broker.NewBroker(*nodeID, clusterManager, diskStorage, wal, offsetManager)
	brokerInstance.Start()

	// --- 6. HTTP Server Setup ---
	s := &Server{
		broker:  brokerInstance,
		cluster: clusterManager,
		config:  cfg,
		router:  mux.NewRouter(),
	}
	s.setupRoutes()
	return s, nil
}

// setupRoutes defines all the API endpoints for the broker.
func (s *Server) setupRoutes() {
	// Topic and Message routes
	s.router.HandleFunc("/topics", s.handleCreateTopic).Methods("POST")
	s.router.HandleFunc("/topics", s.handleGetTopics).Methods("GET")
	s.router.HandleFunc("/topics/{topic}/produce", s.handleProduce).Methods("POST")
	s.router.HandleFunc("/topics/{topic}/partitions/{partition}/consume", s.handleConsume).Methods("GET")

	// Consumer Offset routes
	s.router.HandleFunc("/consumer-groups/{groupID}/offsets/{topic}/{partition}", s.handleCommitOffset).Methods("POST")
	s.router.HandleFunc("/consumer-groups/{groupID}/offsets/{topic}/{partition}", s.handleGetOffset).Methods("GET")

	// --- New Consumer Group Routes ---
	s.router.HandleFunc("/consumer-groups/{groupID}/register", s.handleRegisterConsumer).Methods("POST")
	s.router.HandleFunc("/consumer-groups/{groupID}/heartbeat", s.handleConsumerHeartbeat).Methods("POST")
	s.router.HandleFunc("/consumer-groups/{groupID}", s.handleGetConsumerGroup).Methods("GET")

	// Cluster and Health routes
	s.router.HandleFunc("/cluster/join", s.cluster.HandleRaftJoin).Methods("POST")
	s.router.HandleFunc("/health", s.handleHealth).Methods("GET")
}

// --- HTTP Handlers ---

func (s *Server) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	var req CreateTopicRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	topicName := r.URL.Query().Get("name")
	if topicName == "" {
		http.Error(w, "Query parameter 'name' is required for topic creation", http.StatusBadRequest)
		return
	}

	// REFACTOR: Call broker method which now proxies to the cluster.
	if err := s.broker.CreateTopic(topicName, req.NumPartitions, req.ReplicationFactor); err != nil {
		if strings.Contains(err.Error(), "not the leader") {
			s.forwardRequest(w, r)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, "Topic '%s' creation proposed to cluster leader.\n", topicName)
}

func (s *Server) handleGetTopics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	// REFACTOR: Get metadata directly from the broker, which reads from the FSM.
	json.NewEncoder(w).Encode(s.broker.GetTopicMetadata())
}

func (s *Server) handleProduce(w http.ResponseWriter, r *http.Request) {
	// ... (no change in logic)
	vars := mux.Vars(r)
	topicName := vars["topic"]
	var req ProduceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	msg, err := s.broker.Produce(topicName, req.Key, []byte(req.Value), req.Headers)
	if err != nil {
		if strings.Contains(err.Error(), "not leader") {
			s.forwardRequest(w, r)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(&ProduceResponse{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Status:    "persisted",
	})
}

func (s *Server) handleConsume(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	topicName := vars["topic"]
	partition, _ := strconv.Atoi(vars["partition"])
	offset, _ := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 10
	}

	messages, err := s.broker.Consume(topicName, partition, offset, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	nextOffset := offset
	if len(messages) > 0 {
		nextOffset = messages[len(messages)-1].Offset + 1
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(&ConsumeResponse{
		Messages:   messages,
		Count:      len(messages),
		NextOffset: nextOffset,
	})
}

func (s *Server) handleCommitOffset(w http.ResponseWriter, r *http.Request) {
	// ... (no change in logic)
	vars := mux.Vars(r)
	groupID := vars["groupID"]
	topicName := vars["topic"]
	partition, _ := strconv.Atoi(vars["partition"])

	var req CommitOffsetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if err := s.broker.CommitOffset(groupID, topicName, partition, req.Offset); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetOffset(w http.ResponseWriter, r *http.Request) {
	// ... (no change in logic)
	vars := mux.Vars(r)
	groupID := vars["groupID"]
	topicName := vars["topic"]
	partition, _ := strconv.Atoi(vars["partition"])

	offset := s.broker.GetOffset(groupID, topicName, partition)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int64{"offset": offset})
}

// --- New Consumer Group Handlers ---

func (s *Server) handleRegisterConsumer(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	groupID := vars["groupID"]
	var req RegisterConsumerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if err := s.broker.RegisterConsumer(groupID, req.ConsumerID, req.Topics); err != nil {
		if strings.Contains(err.Error(), "not the leader") {
			s.forwardRequest(w, r)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, "Consumer %s registration for group %s proposed.\n", req.ConsumerID, groupID)
}

func (s *Server) handleConsumerHeartbeat(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	groupID := vars["groupID"]
	var req ConsumerHeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if err := s.broker.ConsumerHeartbeat(groupID, req.ConsumerID); err != nil {
		if strings.Contains(err.Error(), "not the leader") {
			s.forwardRequest(w, r)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetConsumerGroup(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	groupID := vars["groupID"]

	group := s.broker.GetConsumerGroup(groupID)
	if group == nil {
		http.Error(w, "Consumer group not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(group)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	// ... (no change in logic)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "healthy",
		"node_id":   s.broker.GetBrokerID(),
		"is_leader": s.cluster.IsLeader(),
		"leader_id": s.cluster.RaftNode.Raft.Leader(),
	})
}

func (s *Server) forwardRequest(w http.ResponseWriter, r *http.Request) {
	// ... (no change in logic)
	leaderID := s.cluster.RaftNode.Raft.Leader()
	leaderAddr := s.cluster.GetBrokerAddress(string(leaderID))
	if leaderAddr == "" {
		http.Error(w, "No leader found to forward request", http.StatusServiceUnavailable)
		return
	}

	url := fmt.Sprintf("http://%s%s", leaderAddr, r.URL.RequestURI())
	log.Printf("[INFO] Forwarding request to leader at %s", url)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}
	r.Body.Close()

	proxyReq, err := http.NewRequest(r.Method, url, bytes.NewReader(body))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	proxyReq.Header = r.Header

	// Use a client with a timeout
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(proxyReq)
	if err != nil {
		http.Error(w, "Failed to forward request to leader", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func (s *Server) gracefulShutdown() {
	// ... (no change in logic)
	log.Println("Initiating graceful shutdown...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if s.httpServer != nil {
		if err := s.httpServer.Shutdown(ctx); err != nil {
			log.Printf("[ERROR] HTTP server shutdown error: %v", err)
		}
	}

	if s.broker != nil {
		s.broker.Stop()
	}

	if s.cluster != nil {
		s.cluster.Stop()
	}

	log.Println("Shutdown complete.")
}

func main() {
	// ... (no change in logic)
	server, err := NewServer()
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-shutdownChan
		server.gracefulShutdown()
	}()

	port := server.broker.APIPort()
	server.httpServer = &http.Server{
		Addr:    ":" + port,
		Handler: server.router,
	}

	log.Printf("Mini Kafka Broker starting...")
	log.Printf("Node ID: %s", server.broker.GetBrokerID())
	log.Printf("API server listening on: http://%s:%s", server.broker.APIAddress(), port)
	log.Printf("Data directory: %s", server.config.DataDir)

	if err := server.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server failed to start: %v", err)
	}
}
