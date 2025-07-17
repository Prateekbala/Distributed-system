// internal/storage/partition.go
package storage

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"Distributed-system/internal/config"
	"Distributed-system/internal/message"
)

// Partition manages a collection of segments for a single topic-partition.
type Partition struct {
	config        *config.Config
	path          string
	segments      []*Segment
	activeSegment *Segment
	hwm           int64 // high-water mark
	mu            sync.RWMutex
}

// NewPartition creates or loads a partition from a directory path.
func NewPartition(path string, cfg *config.Config) (*Partition, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	p := &Partition{
		config:   cfg,
		path:     path,
		segments: make([]*Segment, 0),
	}

	// Load existing segment files
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var baseOffsets []int64
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".log") {
			offsetStr := strings.TrimSuffix(file.Name(), ".log")
			baseOffset, err := strconv.ParseInt(offsetStr, 10, 64)
			if err == nil {
				baseOffsets = append(baseOffsets, baseOffset)
			}
		}
	}
	sort.Slice(baseOffsets, func(i, j int) bool { return baseOffsets[i] < baseOffsets[j] })

	for _, baseOffset := range baseOffsets {
		seg, err := NewSegment(path, baseOffset, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to load segment %d: %w", baseOffset, err)
		}
		p.segments = append(p.segments, seg)
	}

	// Set active segment or create a new one
	if len(p.segments) > 0 {
		p.activeSegment = p.segments[len(p.segments)-1]
	} else {
		seg, err := NewSegment(path, 0, cfg)
		if err != nil {
			return nil, err
		}
		p.segments = append(p.segments, seg)
		p.activeSegment = seg
	}

	p.hwm = p.activeSegment.NextOffset()

	return p, nil
}

// Append adds a message to the active segment, rolling if necessary.
func (p *Partition) Append(msg *message.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.activeSegment.IsFull() {
		if err := p.rollSegment(); err != nil {
			return err
		}
	}

	msg.Offset = p.hwm
	if err := p.activeSegment.Append(msg); err != nil {
		return err
	}

	p.hwm++
	return nil
}

// rollSegment creates a new active segment.
func (p *Partition) rollSegment() error {
	if err := p.activeSegment.Close(); err != nil {
		log.Printf("[WARN] Failed to close segment before rolling: %v", err)
	}

	newSegment, err := NewSegment(p.path, p.hwm, p.config)
	if err != nil {
		return fmt.Errorf("failed to create new segment: %w", err)
	}

	p.segments = append(p.segments, newSegment)
	p.activeSegment = newSegment
	log.Printf("[INFO] Rolled to new segment with base offset %d for path %s", p.hwm, p.path)
	return nil
}

// Read retrieves messages, potentially spanning multiple segments.
func (p *Partition) Read(startOffset int64, limit int) ([]*message.Message, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var results []*message.Message
	for i := len(p.segments) - 1; i >= 0; i-- {
		seg := p.segments[i]
		if startOffset >= seg.BaseOffset() {
			// This is the segment where we start reading
			msg, err := seg.Read(startOffset)
			if err == nil && msg != nil {
				results = append(results, msg)
				// For simplicity, this implementation reads one message at a time.
				// A real implementation would read batches from each segment.
				// We will read up to the limit.
				for len(results) < limit {
					nextOffset := startOffset + int64(len(results))
					if nextOffset >= p.hwm {
						break // Don't read past the high-water mark
					}
					nextMsg, _ := p.Read(nextOffset, 1)
					if len(nextMsg) > 0 {
						results = append(results, nextMsg[0])
					} else {
						break // No more messages
					}
				}
			}
			break // Stop searching for the starting segment
		}
	}
	return results, nil
}

// GetHighWaterMark returns the next available offset.
func (p *Partition) GetHighWaterMark() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.hwm
}

// Close closes all segments in the partition.
func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, seg := range p.segments {
		if err := seg.Close(); err != nil {
			log.Printf("[WARN] Failed to close segment %s: %v", seg.logFile.Name(), err)
		}
	}
	return nil
}
