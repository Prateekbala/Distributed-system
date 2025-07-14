package storage

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"Distributed-system/internal/config"
	"Distributed-system/internal/message"
	"Distributed-system/internal/topic"
)

type DiskStorage struct {
    config    *config.Config
    segments  map[string]map[int32]*Segment // topic -> partition -> segment
    offsets   map[string]map[int32]int64    // topic -> partition -> next offset
    mu        sync.RWMutex
}

func NewDiskStorage(cfg *config.Config) *DiskStorage {
    ds := &DiskStorage{
        config:   cfg,
        segments: make(map[string]map[int32]*Segment),
        offsets:  make(map[string]map[int32]int64),
    }
    
    // Create data directory if it doesn't exist
    os.MkdirAll(cfg.DataDir, 0755)
    
    // Load existing segments
    ds.loadExistingSegments()
    
    return ds
}

func (ds *DiskStorage) loadExistingSegments() {
    topicsDir := filepath.Join(ds.config.DataDir, "topics")
    topics, err := os.ReadDir(topicsDir)
    if err != nil {
        log.Printf("Warning: Failed to read topics directory: %v", err)
        return
    }

    for _, topicDir := range topics {
        if !topicDir.IsDir() {
            continue
        }

        topicName := topicDir.Name()
        ds.segments[topicName] = make(map[int32]*Segment)
        ds.offsets[topicName] = make(map[int32]int64)

        partitionsDir := filepath.Join(topicsDir, topicName)
        partitions, err := os.ReadDir(partitionsDir)
        if err != nil {
            log.Printf("Warning: Failed to read partitions for topic %s: %v", topicName, err)
            continue
        }

        for _, partitionDir := range partitions {
            if !partitionDir.IsDir() {
                continue
            }

            partitionStr := strings.TrimPrefix(partitionDir.Name(), "partition-")
            partitionNum, err := strconv.Atoi(partitionStr)
            if err != nil {
                log.Printf("Warning: Invalid partition number in directory %s: %v", partitionDir.Name(), err)
                continue
            }

            partition := int32(partitionNum)
            segmentDir := filepath.Join(partitionsDir, partitionDir.Name())
            segments, err := os.ReadDir(segmentDir)
            if err != nil {
                log.Printf("Warning: Failed to read segments for partition %d: %v", partition, err)
                continue
            }

            // Load all segments for this partition
            var latestOffset int64
            for _, segmentFile := range segments {
                if !strings.HasSuffix(segmentFile.Name(), ".log") {
                    continue
                }

                baseOffsetStr := strings.TrimSuffix(segmentFile.Name(), ".log")
                baseOffset, err := strconv.ParseInt(baseOffsetStr, 10, 64)
                if err != nil {
                    continue
                }

                segment, err := NewSegment(ds.config.DataDir, topicName, partition, baseOffset)
                if err != nil {
                    log.Printf("Warning: Failed to load segment %s: %v", segmentFile.Name(), err)
                    continue
                }

                // Update latest offset
                if baseOffset > latestOffset {
                    latestOffset = baseOffset
                }

                // Store segment
                ds.segments[topicName][partition] = segment
            }

            // Set the next offset to be used
            ds.offsets[topicName][partition] = latestOffset + 1
            log.Printf("Loaded segments for topic %s partition %d with latest offset %d", 
                topicName, partition, latestOffset)
        }
    }
}

func (ds *DiskStorage) Store(msg *message.Message) error {
    ds.mu.Lock()
    defer ds.mu.Unlock()
    
    // Initialize topic if it doesn't exist
    if ds.segments[msg.Topic] == nil {
        ds.segments[msg.Topic] = make(map[int32]*Segment)
        ds.offsets[msg.Topic] = make(map[int32]int64)
    }
    
    partition := int32(msg.Partition)
    
    // Get or create segment for this partition
    segment := ds.segments[msg.Topic][partition]
    if segment == nil || segment.Size() >= ds.config.SegmentSize {
        // Create new segment
        baseOffset := ds.offsets[msg.Topic][partition]
        newSegment, err := NewSegment(ds.config.DataDir, msg.Topic, partition, baseOffset)
        if err != nil {
            return err
        }
        
        // Close old segment
        if segment != nil {
            segment.Close()
        }
        
        ds.segments[msg.Topic][partition] = newSegment
        segment = newSegment
    }
    
    // Store message
    if err := segment.Append(msg); err != nil {
        return err
    }
    
    // Increment offset
    ds.offsets[msg.Topic][partition]++
    
    return nil
}

func (ds *DiskStorage) Retrieve(topic string, partition int32, startOffset int64, maxMessages int) ([]*message.Message, error) {
    ds.mu.RLock()
    defer ds.mu.RUnlock()
    
    if ds.segments[topic] == nil || ds.segments[topic][partition] == nil {
        return []*message.Message{}, nil
    }
    
    segment := ds.segments[topic][partition]
    messages, err := segment.ReadFrom(startOffset)
    if err != nil {
        return nil, fmt.Errorf("failed to read from segment: %w", err)
    }
    
    // Limit number of messages returned
    if maxMessages > 0 && len(messages) > maxMessages {
        messages = messages[:maxMessages]
    }
    
    log.Printf("Retrieved %d messages from topic %s partition %d starting at offset %d", 
        len(messages), topic, partition, startOffset)
    
    return messages, nil
}

func (ds *DiskStorage) GetLatestOffset(topic string, partition int32) int64 {
    ds.mu.RLock()
    defer ds.mu.RUnlock()
    
    if ds.offsets[topic] == nil {
        return 0
    }
    
    return ds.offsets[topic][partition]
}

func (ds *DiskStorage) GetHighWaterMark(topic string, partition int) int64 {
    ds.mu.RLock()
    defer ds.mu.RUnlock()
    
    if ds.offsets[topic] == nil {
        return 0
    }
    return ds.offsets[topic][int32(partition)]
}

func (ds *DiskStorage) GetMessage(topic string, partition int, offset int64) (*message.Message, error) {
    ds.mu.RLock()
    defer ds.mu.RUnlock()
    
    if ds.segments[topic] == nil || ds.segments[topic][int32(partition)] == nil {
        return nil, fmt.Errorf("partition not found")
    }
    
    segment := ds.segments[topic][int32(partition)]
    messages, err := segment.ReadFrom(offset)
    if err != nil {
        return nil, err
    }
    
    if len(messages) == 0 {
        return nil, fmt.Errorf("message not found")
    }
    
    return messages[0], nil
}

func (ds *DiskStorage) GetMessages(topic string, partition int, startOffset int64, limit int) ([]*message.Message, error) {
    return ds.Retrieve(topic, int32(partition), startOffset, limit)
}

func (ds *DiskStorage) Close() error {
    ds.mu.Lock()
    defer ds.mu.Unlock()
    
    for topic := range ds.segments {
        for partition := range ds.segments[topic] {
            if ds.segments[topic][partition] != nil {
                ds.segments[topic][partition].Close()
            }
        }
    }
    
    return nil
}

// ListTopics returns all topic names found in the data directory
func (ds *DiskStorage) ListTopics() []string {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	topicsDir := filepath.Join(ds.config.DataDir, "topics")
	dirs, err := os.ReadDir(topicsDir)
	if err != nil {
		return nil
	}

	topics := make([]string, 0)
	for _, dir := range dirs {
		if dir.IsDir() {
			topics = append(topics, dir.Name())
		}
	}
	return topics
}

// ReadTopicConfig returns a default config for a topic (since config persistence is not implemented)
func (ds *DiskStorage) ReadTopicConfig(topicName string) (*topic.TopicConfig, error) {
	// For now, return a default config; in a real system, this would read from a config file
	return topic.DefaultTopicConfig(), nil
}

// CreateTopicDirsAndConfig creates topic and partition directories and writes the topic config
func (ds *DiskStorage) CreateTopicDirsAndConfig(topicName string, config *topic.TopicConfig) error {
	topicsDir := filepath.Join(ds.config.DataDir, "topics")
	topicDir := filepath.Join(topicsDir, topicName)
	if err := os.MkdirAll(topicDir, 0755); err != nil {
		return err
	}
	// Create partition directories
	for i := 0; i < config.NumPartitions; i++ {
		partitionDir := filepath.Join(topicDir, "partition-"+strconv.Itoa(i))
		if err := os.MkdirAll(partitionDir, 0755); err != nil {
			return err
		}
	}
	// Write config file
	configPath := filepath.Join(topicDir, "config.json")
	f, err := os.Create(configPath)
	if err != nil {
		return err
	}
	defer f.Close()
	encoder := json.NewEncoder(f)
	return encoder.Encode(config)
}