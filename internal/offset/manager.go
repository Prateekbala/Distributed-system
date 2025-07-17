// internal/offset/manager.go
package offset

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"Distributed-system/internal/config"
)

// ConsumerOffset represents a single consumer offset record
type ConsumerOffset struct {
	GroupID   string    `json:"group_id"`
	Topic     string    `json:"topic"`
	Partition int32     `json:"partition"`
	Offset    int64     `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
}

// Manager handles consumer offset management
type Manager struct {
	config  *config.Config
	offsets map[string]map[string]map[int32]int64 // groupId -> topic -> partition -> offset
	mu      sync.RWMutex
}

// NewManager creates a new offset manager
func NewManager(cfg *config.Config) (*Manager, error) {
	m := &Manager{
		config:  cfg,
		offsets: make(map[string]map[string]map[int32]int64),
	}
	
	// Create offsets directory
	offsetDir := filepath.Join(cfg.DataDir, "offsets")
	if err := os.MkdirAll(offsetDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create offsets directory: %v", err)
	}
	
	// Load existing offsets
	if err := m.loadOffsets(); err != nil {
		return nil, fmt.Errorf("failed to load offsets: %v", err)
	}
	
	return m, nil
}

// CommitOffset commits a consumer offset for a specific group, topic, and partition
func (m *Manager) CommitOffset(groupID, topic string, partition int32, offset int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.offsets[groupID] == nil {
		m.offsets[groupID] = make(map[string]map[int32]int64)
	}
	if m.offsets[groupID][topic] == nil {
		m.offsets[groupID][topic] = make(map[int32]int64)
	}
	
	m.offsets[groupID][topic][partition] = offset
	
	// Create consumer offset record
	consumerOffset := ConsumerOffset{
		GroupID:   groupID,
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Timestamp: time.Now(),
	}
	
	return m.persistOffset(consumerOffset)
}

// GetOffset retrieves the committed offset for a specific group, topic, and partition
func (m *Manager) GetOffset(groupID, topic string, partition int32) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	if m.offsets[groupID] != nil && m.offsets[groupID][topic] != nil {
		return m.offsets[groupID][topic][partition]
	}
	return 0
}

// GetAllOffsets retrieves all offsets for a specific group and topic
func (m *Manager) GetAllOffsets(groupID, topic string) map[int32]int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	if m.offsets[groupID] != nil && m.offsets[groupID][topic] != nil {
		result := make(map[int32]int64)
		for partition, offset := range m.offsets[groupID][topic] {
			result[partition] = offset
		}
		return result
	}
	return make(map[int32]int64)
}

// persistOffset persists a single consumer offset to disk
func (m *Manager) persistOffset(offset ConsumerOffset) error {
	offsetDir := filepath.Join(m.config.DataDir, "offsets", offset.GroupID)
	if err := os.MkdirAll(offsetDir, 0755); err != nil {
		return fmt.Errorf("failed to create group directory: %v", err)
	}
	
	filename := fmt.Sprintf("%s-%d.json", offset.Topic, offset.Partition)
	filepath := filepath.Join(offsetDir, filename)
	
	data, err := json.Marshal(offset)
	if err != nil {
		return fmt.Errorf("failed to marshal offset: %v", err)
	}
	
	// Write to a temp file first, then rename for atomicity
	tmpFile := filepath + ".tmp"
	f, err := os.OpenFile(tmpFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open temp offset file: %v", err)
	}
	_, err = f.Write(data)
	if err != nil {
		f.Close()
		return fmt.Errorf("failed to write offset: %v", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("failed to sync offset file: %v", err)
	}
	f.Close()
	if err := os.Rename(tmpFile, filepath); err != nil {
		return fmt.Errorf("failed to rename temp offset file: %v", err)
	}
	return nil
}

// loadOffsets loads all consumer offsets from disk
func (m *Manager) loadOffsets() error {
	offsetDir := filepath.Join(m.config.DataDir, "offsets")
	
	// Walk through all group directories
	return filepath.Walk(offsetDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		
		if info.IsDir() {
			return nil
		}
		
		// Read offset file
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("failed to read offset file %s: %v", path, err)
		}
		
		var offset ConsumerOffset
		if err := json.Unmarshal(data, &offset); err != nil {
			return fmt.Errorf("failed to unmarshal offset from %s: %v", path, err)
		}
		
		// Update in-memory map
		if m.offsets[offset.GroupID] == nil {
			m.offsets[offset.GroupID] = make(map[string]map[int32]int64)
		}
		if m.offsets[offset.GroupID][offset.Topic] == nil {
			m.offsets[offset.GroupID][offset.Topic] = make(map[int32]int64)
		}
		m.offsets[offset.GroupID][offset.Topic][offset.Partition] = offset.Offset
		
		return nil
	})
}

// Close persists all offsets to disk before shutting down
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Persist all offsets
	for groupID, topics := range m.offsets {
		for topic, partitions := range topics {
			for partition, offset := range partitions {
				consumerOffset := ConsumerOffset{
					GroupID:   groupID,
					Topic:     topic,
					Partition: partition,
					Offset:    offset,
					Timestamp: time.Now(),
				}
				if err := m.persistOffset(consumerOffset); err != nil {
					return fmt.Errorf("failed to persist offset during shutdown: %v", err)
				}
			}
		}
	}
	
	return nil
}