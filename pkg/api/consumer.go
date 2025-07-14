package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// ConsumerConfig holds configuration for the consumer
type ConsumerConfig struct {
	BrokerAddresses   []string      // List of broker addresses
	GroupID           string        // Consumer group ID
	ClientID          string        // Client identifier
	Timeout           time.Duration // Request timeout
	SessionTimeout    time.Duration // Session timeout for group membership
	HeartbeatInterval time.Duration // Heartbeat interval
	AutoOffsetReset   string        // earliest, latest, none
	EnableAutoCommit  bool          // Auto-commit offsets
	AutoCommitInterval time.Duration // Auto-commit interval
	MaxPollRecords    int           // Max records per poll
	FetchMinBytes     int           // Minimum bytes to fetch
	FetchMaxWait      time.Duration // Max wait time for fetch
}

// DefaultConsumerConfig returns default consumer configuration
func DefaultConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		BrokerAddresses:    []string{"localhost:8080"},
		GroupID:            "mini-kafka-group",
		ClientID:           "mini-kafka-consumer",
		Timeout:            30 * time.Second,
		SessionTimeout:     30 * time.Second,
		HeartbeatInterval:  3 * time.Second,
		AutoOffsetReset:    "latest",
		EnableAutoCommit:   true,
		AutoCommitInterval: 5 * time.Second,
		MaxPollRecords:     500,
		FetchMinBytes:      1,
		FetchMaxWait:       500 * time.Millisecond,
	}
}

// Consumer represents a message consumer
type Consumer struct {
	config         *ConsumerConfig
	httpClient     *http.Client
	brokerIndex    int
	subscriptions  []string
	assignments    map[string][]int // topic -> partitions
	offsets        map[string]map[int]int64 // topic -> partition -> offset
	running        bool
	mu             sync.RWMutex
	heartbeatStop  chan struct{}
	autoCommitStop chan struct{}
}

// ConsumerMessage represents a consumed message
type ConsumerMessage struct {
	Topic     string            `json:"topic"`
	Partition int               `json:"partition"`
	Offset    int64             `json:"offset"`
	Key       string            `json:"key"`
	Value     string            `json:"value"`
	Headers   map[string]string `json:"headers"`
	Timestamp time.Time         `json:"timestamp"`
}

// FetchRequest represents a fetch request
type FetchRequest struct {
	GroupID        string                       `json:"group_id"`
	Topics         map[string][]PartitionOffset `json:"topics"` // topic -> partition offsets
	MaxWaitTime    int                          `json:"max_wait_time_ms"`
	MinBytes       int                          `json:"min_bytes"`
	MaxBytes       int                          `json:"max_bytes,omitempty"`
}

// PartitionOffset represents partition and offset to fetch from
type PartitionOffset struct {
	Partition int   `json:"partition"`
	Offset    int64 `json:"offset"`
}

// FetchResponse represents fetch response
type FetchResponse struct {
	Messages []*ConsumerMessage `json:"messages"`
	Error    string             `json:"error,omitempty"`
}

// CommitRequest represents offset commit request
type CommitRequest struct {
	GroupID string                       `json:"group_id"`
	Offsets map[string][]PartitionOffset `json:"offsets"` // topic -> partition offsets
}

// SubscribeRequest represents subscription request
type SubscribeRequest struct {
	GroupID string   `json:"group_id"`
	Topics  []string `json:"topics"`
}

// AssignmentResponse represents partition assignment response
type AssignmentResponse struct {
	Assignments map[string][]int `json:"assignments"` // topic -> partitions
	Error       string           `json:"error,omitempty"`
}

// RebalanceEvent represents a rebalance notification to the consumer
// (for future extensibility, e.g., push or polling)
type RebalanceEvent struct {
	GroupID     string              `json:"group_id"`
	ConsumerID  string              `json:"consumer_id"`
	Assignments map[string][]int    `json:"assignments"`
	Reason      string              `json:"reason,omitempty"`
	Timestamp   int64               `json:"timestamp"`
}

// NewConsumer creates a new consumer instance
func NewConsumer(config *ConsumerConfig) *Consumer {
	if config == nil {
		config = DefaultConsumerConfig()
	}

	return &Consumer{
		config: config,
		httpClient: &http.Client{
			Timeout: config.Timeout,
		},
		assignments:    make(map[string][]int),
		offsets:        make(map[string]map[int]int64),
		heartbeatStop:  make(chan struct{}),
		autoCommitStop: make(chan struct{}),
	}
}

// Subscribe subscribes to topics
func (c *Consumer) Subscribe(topics []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.subscriptions = topics

	// Send subscription request to broker
	req := &SubscribeRequest{
		GroupID: c.config.GroupID,
		Topics:  topics,
	}

	broker := c.selectBroker()
	url := fmt.Sprintf("http://%s/consumer/subscribe", broker)

	jsonData, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal subscribe request: %w", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to send subscribe request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("broker returned status %d", resp.StatusCode)
	}

	var assignment AssignmentResponse
	if err := json.NewDecoder(resp.Body).Decode(&assignment); err != nil {
		return fmt.Errorf("failed to decode assignment response: %w", err)
	}

	if assignment.Error != "" {
		return fmt.Errorf("subscription error: %s", assignment.Error)
	}

	// Update assignments
	c.assignments = assignment.Assignments

	// Initialize offsets for assigned partitions
	for topic, partitions := range c.assignments {
		if c.offsets[topic] == nil {
			c.offsets[topic] = make(map[int]int64)
		}
		for _, partition := range partitions {
			if _, exists := c.offsets[topic][partition]; !exists {
				// Get committed offset or use auto offset reset
				offset, err := c.getCommittedOffset(topic, partition)
				if err != nil || offset < 0 {
					// Use auto offset reset strategy
					if c.config.AutoOffsetReset == "earliest" {
						offset = 0
					} else {
						offset = -1 // Latest will be resolved by broker
					}
				}
				c.offsets[topic][partition] = offset
			}
		}
	}

	return nil
}

// Poll polls for messages
func (c *Consumer) Poll(timeout time.Duration) ([]*ConsumerMessage, error) {
	c.mu.RLock()
	assignments := make(map[string][]int)
	for topic, partitions := range c.assignments {
		assignments[topic] = make([]int, len(partitions))
		copy(assignments[topic], partitions)
	}
	offsets := make(map[string]map[int]int64)
	for topic, partitionOffsets := range c.offsets {
		offsets[topic] = make(map[int]int64)
		for partition, offset := range partitionOffsets {
			offsets[topic][partition] = offset
		}
	}
	c.mu.RUnlock()

	if len(assignments) == 0 {
		return nil, fmt.Errorf("no topic assignments")
	}

	// Build fetch request
	topics := make(map[string][]PartitionOffset)
	for topic, partitions := range assignments {
		var partitionOffsets []PartitionOffset
		for _, partition := range partitions {
			offset := offsets[topic][partition]
			partitionOffsets = append(partitionOffsets, PartitionOffset{
				Partition: partition,
				Offset:    offset,
			})
		}
		topics[topic] = partitionOffsets
	}

	req := &FetchRequest{
		GroupID:     c.config.GroupID,
		Topics:      topics,
		MaxWaitTime: int(timeout.Milliseconds()),
		MinBytes:    c.config.FetchMinBytes,
		MaxBytes:    c.config.MaxPollRecords * 1024, // Rough estimate
	}

	broker := c.selectBroker()
	url := fmt.Sprintf("http://%s/consumer/fetch", broker)

	jsonData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal fetch request: %w", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to send fetch request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("broker returned status %d", resp.StatusCode)
	}

	var fetchResp FetchResponse
	if err := json.NewDecoder(resp.Body).Decode(&fetchResp); err != nil {
		return nil, fmt.Errorf("failed to decode fetch response: %w", err)
	}

	if fetchResp.Error != "" {
		return nil, fmt.Errorf("fetch error: %s", fetchResp.Error)
	}

	// Update offsets for consumed messages
	c.mu.Lock()
	for _, msg := range fetchResp.Messages {
		if c.offsets[msg.Topic] == nil {
			c.offsets[msg.Topic] = make(map[int]int64)
		}
		c.offsets[msg.Topic][msg.Partition] = msg.Offset + 1
	}
	c.mu.Unlock()

	// Auto-commit if enabled
	if c.config.EnableAutoCommit && len(fetchResp.Messages) > 0 {
		go c.commitAsync()
	}

	return fetchResp.Messages, nil
}

// Commit commits current offsets synchronously
func (c *Consumer) Commit() error {
	c.mu.RLock()
	offsets := make(map[string][]PartitionOffset)
	for topic, partitionOffsets := range c.offsets {
		var pos []PartitionOffset
		for partition, offset := range partitionOffsets {
			pos = append(pos, PartitionOffset{
				Partition: partition,
				Offset:    offset,
			})
		}
		offsets[topic] = pos
	}
	c.mu.RUnlock()

	if len(offsets) == 0 {
		return nil
	}

	req := &CommitRequest{
		GroupID: c.config.GroupID,
		Offsets: offsets,
	}

	broker := c.selectBroker()
	url := fmt.Sprintf("http://%s/consumer/commit", broker)

	jsonData, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal commit request: %w", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to send commit request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("broker returned status %d", resp.StatusCode)
	}

	return nil
}

// commitAsync commits offsets asynchronously
func (c *Consumer) commitAsync() {
	if err := c.Commit(); err != nil {
		// In production, you'd want to log this error properly
		fmt.Printf("Failed to commit offsets: %v\n", err)
	}
}

// getCommittedOffset gets the committed offset for a partition
func (c *Consumer) getCommittedOffset(topic string, partition int) (int64, error) {
	broker := c.selectBroker()
	url := fmt.Sprintf("http://%s/consumer/offset?group_id=%s&topic=%s&partition=%d", 
		broker, c.config.GroupID, topic, partition)

	resp, err := http.Get(url)
	if err != nil {
		return -1, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return -1, fmt.Errorf("broker returned status %d", resp.StatusCode)
	}

	var result struct {
		Offset int64  `json:"offset"`
		Error  string `json:"error,omitempty"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return -1, err
	}

	if result.Error != "" {
		return -1, fmt.Errorf("offset fetch error: %s", result.Error)
	}

	return result.Offset, nil
}

// StartConsuming starts background consumption with a message handler
func (c *Consumer) StartConsuming(handler func([]*ConsumerMessage) error) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return fmt.Errorf("consumer is already running")
	}
	c.running = true
	c.mu.Unlock()

	// Start heartbeat goroutine
	go c.heartbeatLoop()

	// Start auto-commit goroutine if enabled
	if c.config.EnableAutoCommit {
		go c.autoCommitLoop()
	}

	// Main consumption loop
	go c.consumeLoop(handler)

	return nil
}

// consumeLoop is the main consumption loop
func (c *Consumer) consumeLoop(handler func([]*ConsumerMessage) error) {
	for {
		c.mu.RLock()
		running := c.running
		c.mu.RUnlock()

		if !running {
			break
		}

		messages, err := c.Poll(c.config.FetchMaxWait)
		if err != nil {
			// Log error and continue
			fmt.Printf("Poll error: %v\n", err)
			time.Sleep(1 * time.Second)
			continue
		}

		if len(messages) > 0 {
			if err := handler(messages); err != nil {
				// Log error and continue
				fmt.Printf("Handler error: %v\n", err)
			}
		}
	}
}

// heartbeatLoop sends periodic heartbeats
func (c *Consumer) heartbeatLoop() {
	ticker := time.NewTicker(c.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Send heartbeat
			c.sendHeartbeat()
		case <-c.heartbeatStop:
			return
		}
	}
}

// sendHeartbeat sends a heartbeat to the broker
func (c *Consumer) sendHeartbeat() {
	broker := c.selectBroker()
	url := fmt.Sprintf("http://%s/consumer/heartbeat", broker)

	req := map[string]string{
		"group_id":  c.config.GroupID,
		"client_id": c.config.ClientID,
	}

	jsonData, _ := json.Marshal(req)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return
	}
	resp.Body.Close()
}

// autoCommitLoop periodically commits offsets
func (c *Consumer) autoCommitLoop() {
	ticker := time.NewTicker(c.config.AutoCommitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.commitAsync()
		case <-c.autoCommitStop:
			return
		}
	}
}

// StopConsuming stops the consumer
func (c *Consumer) StopConsuming() error {
	c.mu.Lock()
	c.running = false
	c.mu.Unlock()

	// Stop background goroutines
	close(c.heartbeatStop)
	if c.config.EnableAutoCommit {
		close(c.autoCommitStop)
	}

	// Final commit
	return c.Commit()
}

// selectBroker selects a broker using round-robin
func (c *Consumer) selectBroker() string {
	if len(c.config.BrokerAddresses) == 0 {
		return "localhost:8080"
	}

	broker := c.config.BrokerAddresses[c.brokerIndex]
	c.brokerIndex = (c.brokerIndex + 1) % len(c.config.BrokerAddresses)
	return broker
}

// Close closes the consumer and cleans up resources
func (c *Consumer) Close() error {
	return c.StopConsuming()
}