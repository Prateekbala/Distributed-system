// internal/storage/disk.go
package storage

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"Distributed-system/internal/cluster"
	"Distributed-system/internal/config"
	"Distributed-system/internal/message"
)

// DiskStorage manages a collection of partitions on disk. It acts as a high-level
// coordinator, delegating all segment and file-level logic to Partition objects.
type DiskStorage struct {
	config     *config.Config
	partitions map[string]map[int]*Partition // topic -> partition -> Partition
	mu         sync.RWMutex
}

// NewDiskStorage creates a new disk storage engine.
func NewDiskStorage(cfg *config.Config) *DiskStorage {
	ds := &DiskStorage{
		config:     cfg,
		partitions: make(map[string]map[int]*Partition),
	}

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		log.Printf("[ERROR] Failed to create base data directory: %v", err)
	}

	ds.loadExistingPartitions()

	return ds
}

// loadExistingPartitions scans the disk and loads all found partitions by
// delegating the loading of segments to the NewPartition constructor.
func (ds *DiskStorage) loadExistingPartitions() {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	topicsDir := filepath.Join(ds.config.DataDir, "topics")
	// Create topics directory if it doesn't exist
	if err := os.MkdirAll(topicsDir, 0755); err != nil {
		log.Printf("[ERROR] Failed to create topics directory: %v", err)
		return
	}

	topicDirs, err := os.ReadDir(topicsDir)
	if err != nil {
		log.Printf("[INFO] Topics directory not found or unreadable, starting fresh. Error: %v", err)
		return
	}

	for _, topicDir := range topicDirs {
		if !topicDir.IsDir() {
			continue
		}
		topicName := topicDir.Name()
		ds.partitions[topicName] = make(map[int]*Partition)

		partitionDirs, err := os.ReadDir(filepath.Join(topicsDir, topicName))
		if err != nil {
			log.Printf("[WARN] Could not read partitions for topic %s: %v", topicName, err)
			continue
		}

		for _, pDir := range partitionDirs {
			if !pDir.IsDir() || !strings.HasPrefix(pDir.Name(), "partition-") {
				continue
			}
			pNumStr := strings.TrimPrefix(pDir.Name(), "partition-")
			partitionNum, err := strconv.Atoi(pNumStr)
			if err != nil {
				continue
			}

			pPath := filepath.Join(topicsDir, topicName, pDir.Name())
			partition, err := NewPartition(pPath, ds.config)
			if err != nil {
				log.Printf("[WARN] Failed to load partition %s-%d: %v", topicName, partitionNum, err)
				continue
			}
			ds.partitions[topicName][partitionNum] = partition
		}
	}
}

// getOrCreatePartition is the central internal method to safely access or initialize a partition.
func (ds *DiskStorage) getOrCreatePartition(topic string, partitionNum int) (*Partition, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.partitions[topic] == nil {
		ds.partitions[topic] = make(map[int]*Partition)
	}

	p, exists := ds.partitions[topic][partitionNum]
	if !exists {
		pPath := filepath.Join(ds.config.DataDir, "topics", topic, fmt.Sprintf("partition-%d", partitionNum))
		var err error
		p, err = NewPartition(pPath, ds.config)
		if err != nil {
			return nil, err
		}
		ds.partitions[topic][partitionNum] = p
	}
	return p, nil
}

// Store delegates the message append to the correct partition.
func (ds *DiskStorage) Store(msg *message.Message) error {
	p, err := ds.getOrCreatePartition(msg.Topic, msg.Partition)
	if err != nil {
		return err
	}
	// The message offset is now expected to be set by the broker before this call.
	return p.Append(msg)
}

// GetMessages delegates the read to the correct partition.
func (ds *DiskStorage) GetMessages(topic string, partition int, startOffset int64, limit int) ([]*message.Message, error) {
	// Use RLock for read operations
	ds.mu.RLock()
	p, exists := ds.partitions[topic][partition]
	ds.mu.RUnlock()

	if !exists {
		return []*message.Message{}, nil
	}
	return p.Read(startOffset, limit)
}

// GetHighWaterMark delegates the call to the correct partition.
func (ds *DiskStorage) GetHighWaterMark(topic string, partition int) int64 {
	// Use RLock for read operations
	ds.mu.RLock()
	p, exists := ds.partitions[topic][partition]
	ds.mu.RUnlock()

	if !exists {
		// If the partition doesn't exist on disk yet, its HWM is 0.
		return 0
	}
	return p.GetHighWaterMark()
}

// GetMessage is a convenience wrapper.
func (ds *DiskStorage) GetMessage(topic string, partition int, offset int64) (*message.Message, error) {
	messages, err := ds.GetMessages(topic, partition, offset, 1)
	if err != nil {
		return nil, err
	}
	if len(messages) == 0 || messages[0].Offset != offset {
		return nil, fmt.Errorf("message not found at offset %d", offset)
	}
	return messages[0], nil
}

// Close closes all managed partitions.
func (ds *DiskStorage) Close() error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	for _, pMap := range ds.partitions {
		for _, p := range pMap {
			p.Close()
		}
	}
	return nil
}

// EnsureTopicDirs creates directories for a topic and its partitions by ensuring
// the partition object is initialized.
func (ds *DiskStorage) EnsureTopicDirs(meta *cluster.TopicMetadata) error {
	for i := 0; i < meta.NumPartitions; i++ {
		if _, err := ds.getOrCreatePartition(meta.Name, i); err != nil {
			return fmt.Errorf("failed to ensure partition %s-%d: %w", meta.Name, i, err)
		}
	}
	return nil
}
