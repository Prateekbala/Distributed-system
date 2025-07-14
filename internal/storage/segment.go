package storage

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"Distributed-system/internal/message"
)

type Segment struct {
    baseOffset int64
    file       *os.File
    writer     *bufio.Writer
    size       int64
    mu         sync.RWMutex
}

func NewSegment(dataDir string, topic string, partition int32, baseOffset int64) (*Segment, error) {
    segmentDir := filepath.Join(dataDir, "topics", topic, fmt.Sprintf("partition-%d", partition))
    if err := os.MkdirAll(segmentDir, 0755); err != nil {
        return nil, err
    }
    
    filename := fmt.Sprintf("%020d.log", baseOffset)
    filepath := filepath.Join(segmentDir, filename)
    
    file, err := os.OpenFile(filepath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
    if err != nil {
        return nil, err
    }
    
    // Get current file size
    stat, err := file.Stat()
    if err != nil {
        return nil, err
    }
    
    return &Segment{
        baseOffset: baseOffset,
        file:       file,
        writer:     bufio.NewWriter(file),
        size:       stat.Size(),
    }, nil
}

func (s *Segment) Append(msg *message.Message) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    data, err := msg.Serialize()
    if err != nil {
        return err
    }
    
    // Write message length first, then message data
    lengthBytes := fmt.Sprintf("%010d", len(data))
    if _, err := s.writer.WriteString(lengthBytes); err != nil {
        return err
    }
    
    if _, err := s.writer.Write(data); err != nil {
        return err
    }
    
    s.size += int64(len(lengthBytes) + len(data))
    return s.writer.Flush()
}

func (s *Segment) Size() int64 {
    s.mu.RLock()
    defer s.mu.RUnlock()
    return s.size
}

func (s *Segment) Close() error {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    if err := s.writer.Flush(); err != nil {
        return err
    }
    return s.file.Close()
}

// Read messages from segment starting at offset
func (s *Segment) ReadFrom(startOffset int64) ([]*message.Message, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    
    // Open file for reading
    file, err := os.Open(s.file.Name())
    if err != nil {
        return nil, err
    }
    defer file.Close()
    
    var messages []*message.Message
    reader := bufio.NewReader(file)
    currentOffset := s.baseOffset
    
    for {
        // Read message length
        lengthStr := make([]byte, 10)
        n, err := reader.Read(lengthStr)
        if err != nil || n != 10 {
            break
        }
        
        length, err := strconv.Atoi(strings.TrimSpace(string(lengthStr)))
        if err != nil {
            break
        }
        
        // Read message data
        msgData := make([]byte, length)
        n, err = reader.Read(msgData)
        if err != nil || n != length {
            break
        }
        
        // Only include messages at or after the requested offset
        if currentOffset >= startOffset {
            msg, err := message.Deserialize(msgData)
            if err == nil {
                messages = append(messages, msg)
            }
        }
        
        currentOffset++
    }
    
    return messages, nil
}