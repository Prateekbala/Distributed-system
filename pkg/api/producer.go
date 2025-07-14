package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// ProducerConfig holds configuration for the producer
type ProducerConfig struct {
	BrokerAddresses []string      // List of broker addresses
	ClientID        string        // Client identifier
	Timeout         time.Duration // Request timeout
	RetryAttempts   int           // Number of retry attempts
	BatchSize       int           // Number of messages to batch together
	BatchTimeout    time.Duration // Max time to wait for batch to fill
}

// DefaultProducerConfig returns default producer configuration
func DefaultProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		BrokerAddresses: []string{"localhost:8080"},
		ClientID:        "mini-kafka-producer",
		Timeout:         30 * time.Second,
		RetryAttempts:   3,
		BatchSize:       100,
		BatchTimeout:    10 * time.Millisecond,
	}
}

// Producer represents a message producer
type Producer struct {
	config      *ProducerConfig
	httpClient  *http.Client
	brokerIndex int // For round-robin broker selection
	batchBuffer map[string][]*ProduceMessage
	batchTimer  *time.Timer
}

// ProduceMessage represents a message to be produced
type ProduceMessage struct {
	Topic     string            `json:"topic"`
	Partition int               `json:"partition,omitempty"` // -1 for auto-assignment
	Key       string            `json:"key,omitempty"`
	Value     string            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
}

// ProduceResponse represents the response from produce request
type ProduceResponse struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
	Error     string `json:"error,omitempty"`
}

// BatchProduceRequest represents a batch of messages
type BatchProduceRequest struct {
	Messages []*ProduceMessage `json:"messages"`
}

// BatchProduceResponse represents response for batch produce
type BatchProduceResponse struct {
	Responses []*ProduceResponse `json:"responses"`
}

// NewProducer creates a new producer instance
func NewProducer(config *ProducerConfig) *Producer {
	if config == nil {
		config = DefaultProducerConfig()
	}

	return &Producer{
		config: config,
		httpClient: &http.Client{
			Timeout: config.Timeout,
		},
		batchBuffer: make(map[string][]*ProduceMessage),
	}
}

// Send sends a single message synchronously
func (p *Producer) Send(msg *ProduceMessage) (*ProduceResponse, error) {
	// Set timestamp if not provided
	if msg.Timestamp.IsZero() {
		msg.Timestamp = time.Now()
	}

	// Auto-assign partition if not specified
	if msg.Partition == 0 {
		msg.Partition = -1
	}

	broker := p.selectBroker()
	url := fmt.Sprintf("http://%s/produce", broker)

	jsonData, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt <= p.config.RetryAttempts; attempt++ {
		if attempt > 0 {
			// Exponential backoff
			time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
			broker = p.selectBroker() // Try different broker
			url = fmt.Sprintf("http://%s/produce", broker)
		}

		resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			lastErr = fmt.Errorf("broker returned status %d", resp.StatusCode)
			continue
		}

		var produceResp ProduceResponse
		err = json.NewDecoder(resp.Body).Decode(&produceResp)
		resp.Body.Close()

		if err != nil {
			lastErr = err
			continue
		}

		if produceResp.Error != "" {
			lastErr = fmt.Errorf("broker error: %s", produceResp.Error)
			continue
		}

		return &produceResp, nil
	}

	return nil, fmt.Errorf("failed to send message after %d attempts: %w", 
		p.config.RetryAttempts+1, lastErr)
}

// SendAsync sends a message asynchronously using batching
func (p *Producer) SendAsync(msg *ProduceMessage, callback func(*ProduceResponse, error)) {
	// Set timestamp if not provided
	if msg.Timestamp.IsZero() {
		msg.Timestamp = time.Now()
	}

	// Auto-assign partition if not specified
	if msg.Partition == 0 {
		msg.Partition = -1
	}

	// Add to batch buffer
	if p.batchBuffer[msg.Topic] == nil {
		p.batchBuffer[msg.Topic] = make([]*ProduceMessage, 0, p.config.BatchSize)
	}

	p.batchBuffer[msg.Topic] = append(p.batchBuffer[msg.Topic], msg)

	// Store callback (simplified - in real implementation would need message tracking)
	go func() {
		// This is a simplified callback mechanism
		// In production, you'd want to track individual messages in batches
		if callback != nil {
			resp, err := p.Send(msg)
			callback(resp, err)
		}
	}()

	// Check if batch is full
	if len(p.batchBuffer[msg.Topic]) >= p.config.BatchSize {
		p.flushBatch(msg.Topic)
	} else if p.batchTimer == nil {
		// Start batch timer
		p.batchTimer = time.AfterFunc(p.config.BatchTimeout, func() {
			p.flushAllBatches()
			p.batchTimer = nil
		})
	}
}

// SendBatch sends multiple messages in a single request
func (p *Producer) SendBatch(messages []*ProduceMessage) ([]*ProduceResponse, error) {
	if len(messages) == 0 {
		return nil, nil
	}

	// Set timestamps for messages that don't have them
	for _, msg := range messages {
		if msg.Timestamp.IsZero() {
			msg.Timestamp = time.Now()
		}
		if msg.Partition == 0 {
			msg.Partition = -1
		}
	}

	batch := &BatchProduceRequest{Messages: messages}
	broker := p.selectBroker()
	url := fmt.Sprintf("http://%s/produce/batch", broker)

	jsonData, err := json.Marshal(batch)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal batch: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt <= p.config.RetryAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
			broker = p.selectBroker()
			url = fmt.Sprintf("http://%s/produce/batch", broker)
		}

		resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			lastErr = fmt.Errorf("broker returned status %d", resp.StatusCode)
			continue
		}

		var batchResp BatchProduceResponse
		err = json.NewDecoder(resp.Body).Decode(&batchResp)
		resp.Body.Close()

		if err != nil {
			lastErr = err
			continue
		}

		return batchResp.Responses, nil
	}

	return nil, fmt.Errorf("failed to send batch after %d attempts: %w", 
		p.config.RetryAttempts+1, lastErr)
}

// flushBatch flushes messages for a specific topic
func (p *Producer) flushBatch(topic string) {
	messages := p.batchBuffer[topic]
	if len(messages) == 0 {
		return
	}

	// Clear the buffer
	p.batchBuffer[topic] = nil

	// Send batch asynchronously
	go func() {
		_, err := p.SendBatch(messages)
		if err != nil {
			// In production, you'd want to handle this error properly
			// Maybe retry or call error callbacks
			fmt.Printf("Failed to send batch for topic %s: %v\n", topic, err)
		}
	}()
}

// flushAllBatches flushes all pending batches
func (p *Producer) flushAllBatches() {
	for topic := range p.batchBuffer {
		p.flushBatch(topic)
	}
}

// Flush forces all pending messages to be sent
func (p *Producer) Flush() error {
	p.flushAllBatches()
	// Wait a bit for async sends to complete
	time.Sleep(100 * time.Millisecond)
	return nil
}

// selectBroker selects a broker using round-robin
func (p *Producer) selectBroker() string {
	if len(p.config.BrokerAddresses) == 0 {
		return "localhost:8080"
	}

	broker := p.config.BrokerAddresses[p.brokerIndex]
	p.brokerIndex = (p.brokerIndex + 1) % len(p.config.BrokerAddresses)
	return broker
}

// Close cleans up producer resources
func (p *Producer) Close() error {
	// Flush any pending messages
	p.Flush()
	
	// Stop batch timer
	if p.batchTimer != nil {
		p.batchTimer.Stop()
	}
	
	return nil
}