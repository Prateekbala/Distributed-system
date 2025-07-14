// internal/storage/wal.go
package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"Distributed-system/internal/config"
	"Distributed-system/internal/message"
	"Distributed-system/internal/topic"
)

type WALEntry struct {
    Type      string          `json:"type"`      // "message", "topic_create", "topic_delete"
    Topic     string          `json:"topic"`
    Config    *topic.TopicConfig `json:"config,omitempty"`  // Add topic configuration
    Partition int32           `json:"partition"`
    Message   *message.Message `json:"message,omitempty"`
    Timestamp time.Time       `json:"timestamp"`
    LSN       int64           `json:"lsn"` // Log Sequence Number
}

type WAL struct {
    config     *config.Config
    file       *os.File
    writer     *bufio.Writer
    lsn        int64
    mu         sync.Mutex
    syncTicker *time.Ticker
}

func NewWAL(cfg *config.Config) (*WAL, error) {
    walDir := filepath.Join(cfg.DataDir, "wal")
    if err := os.MkdirAll(walDir, 0755); err != nil {
        return nil, err
    }
    
    walFile := filepath.Join(walDir, "wal.log")
    file, err := os.OpenFile(walFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
    if err != nil {
        return nil, err
    }
    
    wal := &WAL{
        config: cfg,
        file:   file,
        writer: bufio.NewWriter(file),
        lsn:    0,
    }
    
    // Load existing LSN
    wal.loadLSN()
    
    // Start sync ticker if enabled
    if cfg.WALEnabled {
        wal.syncTicker = time.NewTicker(cfg.WALSyncInterval)
        go wal.syncLoop()
    }
    
    return wal, nil
}

func (w *WAL) WriteMessage(topic string, partition int32, msg *message.Message) error {
    if !w.config.WALEnabled {
        return nil
    }
    
    w.mu.Lock()
    defer w.mu.Unlock()
    
    w.lsn++
    
    entry := WALEntry{
        Type:      "message",
        Topic:     topic,
        Partition: partition,
        Message:   msg,
        Timestamp: time.Now(),
        LSN:       w.lsn,
    }
    
    data, err := json.Marshal(entry)
    if err != nil {
        return err
    }
    
    // Write entry length first, then entry data
    lengthLine := fmt.Sprintf("%010d\n", len(data))
    if _, err := w.writer.WriteString(lengthLine); err != nil {
        return err
    }
    
    if _, err := w.writer.Write(data); err != nil {
        return err
    }
    
    if _, err := w.writer.WriteString("\n"); err != nil {
        return err
    }
    
    return nil
}

func (w *WAL) WriteTopicEvent(eventType, topic string, config *topic.TopicConfig) error {
    if !w.config.WALEnabled {
        return nil
    }
    
    w.mu.Lock()
    defer w.mu.Unlock()
    
    w.lsn++
    
    entry := WALEntry{
        Type:      eventType,
        Topic:     topic,
        Config:    config,
        Timestamp: time.Now(),
        LSN:       w.lsn,
    }
    
    data, err := json.Marshal(entry)
    if err != nil {
        return err
    }
    
    lengthLine := fmt.Sprintf("%010d\n", len(data))
    if _, err := w.writer.WriteString(lengthLine); err != nil {
        return err
    }
    
    if _, err := w.writer.Write(data); err != nil {
        return err
    }
    
    if _, err := w.writer.WriteString("\n"); err != nil {
        return err
    }
    
    return nil
}

func (w *WAL) WriteTopicCreate(topic string, config *topic.TopicConfig) error {
	return w.WriteTopicEvent("topic_create", topic, config)
}

func (w *WAL) Sync() error {
    w.mu.Lock()
    defer w.mu.Unlock()
    
    if err := w.writer.Flush(); err != nil {
        return err
    }
    
    return w.file.Sync()
}

func (w *WAL) syncLoop() {
    for range w.syncTicker.C {
        w.Sync()
    }
}

func (w *WAL) Close() error {
    if w.syncTicker != nil {
        w.syncTicker.Stop()
    }
    
    w.mu.Lock()
    defer w.mu.Unlock()
    
    if err := w.writer.Flush(); err != nil {
        return err
    }
    
    return w.file.Close()
}

func (w *WAL) loadLSN() {
    // Read the WAL file to find the highest LSN
    file, err := os.Open(w.file.Name())
    if err != nil {
        return
    }
    defer file.Close()
    
    scanner := bufio.NewScanner(file)
    maxLSN := int64(0)
    
    for scanner.Scan() {
        lengthLine := scanner.Text()
        if len(lengthLine) != 10 {
            continue
        }
        
        // Read the actual entry
        if !scanner.Scan() {
            break
        }
        
        entryData := scanner.Bytes()
        var entry WALEntry
        if err := json.Unmarshal(entryData, &entry); err != nil {
            continue
        }
        
        if entry.LSN > maxLSN {
            maxLSN = entry.LSN
        }
    }
    
    w.lsn = maxLSN
}

// Recovery reads the WAL and returns all entries for replay
func (w *WAL) Recovery() ([]WALEntry, error) {
    file, err := os.Open(w.file.Name())
    if err != nil {
        return nil, err
    }
    defer file.Close()
    
    var entries []WALEntry
    scanner := bufio.NewScanner(file)
    
    for scanner.Scan() {
        lengthLine := scanner.Text()
        if len(lengthLine) != 10 {
            continue
        }
        
        // Read the actual entry
        if !scanner.Scan() {
            break
        }
        
        entryData := scanner.Bytes()
        var entry WALEntry
        if err := json.Unmarshal(entryData, &entry); err != nil {
            continue
        }
        
        entries = append(entries, entry)
    }
    
    return entries, scanner.Err()
}