// internal/storage/wal.go
package storage

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"Distributed-system/internal/config"
	"Distributed-system/internal/message"
)

// WALEntry represents a single entry in the Write-Ahead Log.
type WALEntry struct {
	Type      string           `json:"type"` // "message"
	Message   *message.Message `json:"message"`
	Timestamp time.Time        `json:"timestamp"`
}

// WAL provides a durable, append-only log for recovering in-flight messages.
type WAL struct {
	config     *config.Config
	file       *os.File
	writer     *bufio.Writer
	mu         sync.Mutex
	syncTicker *time.Ticker
}

// NewWAL creates or opens a WAL file.
func NewWAL(cfg *config.Config) (*WAL, error) {
	if !cfg.WALEnabled {
		return &WAL{config: cfg}, nil // Return a no-op WAL if disabled
	}

	walDir := filepath.Join(cfg.DataDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, err
	}

	walFile := filepath.Join(walDir, "wal.log")
	file, err := os.OpenFile(walFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		return nil, err
	}

	wal := &WAL{
		config: cfg,
		file:   file,
		writer: bufio.NewWriter(file),
	}

	// Start sync ticker
	wal.syncTicker = time.NewTicker(cfg.WALSyncInterval)
	go wal.syncLoop()

	return wal, nil
}

// WriteMessage writes a message entry to the log.
func (w *WAL) WriteMessage(topic string, partition int32, msg *message.Message) error {
	if !w.config.WALEnabled || w.file == nil {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	entry := WALEntry{
		Type:      "message",
		Message:   msg,
		Timestamp: time.Now(),
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	// Write with a newline delimiter for easier scanning.
	if _, err := w.writer.Write(data); err != nil {
		return err
	}
	if _, err := w.writer.WriteString("\n"); err != nil {
		return err
	}

	return nil
}

// Sync flushes the WAL writer buffer to disk.
func (w *WAL) Sync() error {
	if !w.config.WALEnabled || w.file == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// syncLoop periodically calls Sync.
func (w *WAL) syncLoop() {
	if w.syncTicker == nil {
		return
	}
	for range w.syncTicker.C {
		w.Sync()
	}
}

// Close flushes and closes the WAL file.
func (w *WAL) Close() error {
	if !w.config.WALEnabled || w.file == nil {
		return nil
	}
	if w.syncTicker != nil {
		w.syncTicker.Stop()
	}
	return w.Sync()
}

// Recovery reads the entire WAL and returns all valid entries for replay.
func (w *WAL) Recovery() ([]WALEntry, error) {
	if !w.config.WALEnabled {
		return []WALEntry{}, nil
	}

	// Ensure all data is flushed before reading
	w.Sync()

	file, err := os.Open(w.file.Name())
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var entries []WALEntry
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry WALEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err == nil {
			if entry.Type == "message" && entry.Message != nil {
				entries = append(entries, entry)
			}
		}
	}

	return entries, scanner.Err()
}
