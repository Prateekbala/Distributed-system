// internal/storage/segment.go
package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"Distributed-system/internal/config"
	"Distributed-system/internal/message"
)

const (
	lenWidth = 8 // Number of bytes to store the message length
)

// Segment represents a single log segment file on disk.
type Segment struct {
	config     *config.Config
	logFile    *os.File
	indexFile  *os.File
	baseOffset int64
	nextOffset int64
	path       string
	isClosed   bool
	mu         sync.RWMutex
}

// NewSegment creates a new segment file at the given base offset.
func NewSegment(path string, baseOffset int64, cfg *config.Config) (*Segment, error) {
	logPath := filepath.Join(path, fmt.Sprintf("%020d.log", baseOffset))
	logFile, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	indexPath := filepath.Join(path, fmt.Sprintf("%020d.index", baseOffset))
	indexFile, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	s := &Segment{
		config:     cfg,
		path:       path,
		logFile:    logFile,
		indexFile:  indexFile,
		baseOffset: baseOffset,
	}

	// Determine the next offset by reading the last entry
	if err := s.loadNextOffset(); err != nil {
		return nil, err
	}

	return s, nil
}

// loadNextOffset reads the log file to determine the next available offset.
func (s *Segment) loadNextOffset() error {
	s.nextOffset = s.baseOffset
	scanner := bufio.NewScanner(s.logFile)
	buf := make([]byte, 0, 1024*1024) // 1MB buffer
	scanner.Buffer(buf, 10*1024*1024) // 10MB max token size

	for {
		// Read length prefix
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return fmt.Errorf("error scanning log file %s: %w", s.logFile.Name(), err)
			}
			// End of file
			break
		}
		lengthBytes := scanner.Bytes()
		msgLen := binary.BigEndian.Uint64(lengthBytes)
		_ = msgLen // Silence unused variable warning

		// Read message data
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return fmt.Errorf("error scanning log file %s: %w", s.logFile.Name(), err)
			}
			return fmt.Errorf("log file %s is corrupted: expected message data but found EOF", s.logFile.Name())
		}

		s.nextOffset++
	}

	return nil
}

// Append writes a message to the segment and updates the index.
func (s *Segment) Append(msg *message.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isClosed {
		return fmt.Errorf("cannot append to closed segment")
	}

	// Get current size for the index entry
	stat, err := s.logFile.Stat()
	if err != nil {
		return err
	}
	position := stat.Size()

	// Write to log file
	data, err := msg.Serialize()
	if err != nil {
		return err
	}

	lenBuf := make([]byte, lenWidth)
	binary.BigEndian.PutUint64(lenBuf, uint64(len(data)))

	if _, err := s.logFile.Write(lenBuf); err != nil {
		return err
	}
	if _, err := s.logFile.Write(data); err != nil {
		return err
	}

	// Write to index file (relative offset, absolute position)
	off := uint32(msg.Offset - s.baseOffset)
	pos := uint64(position)

	idxBuf := make([]byte, 12) // 4 bytes for offset, 8 for position
	binary.BigEndian.PutUint32(idxBuf[0:4], off)
	binary.BigEndian.PutUint64(idxBuf[4:12], pos)

	if _, err := s.indexFile.Write(idxBuf); err != nil {
		return err
	}

	s.nextOffset = msg.Offset + 1
	return nil
}

// Read finds a message by its offset. It uses the index for efficiency.
func (s *Segment) Read(offset int64) (*message.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.isClosed {
		return nil, fmt.Errorf("cannot read from closed segment")
	}

	// Calculate relative offset
	relativeOffset := uint32(offset - s.baseOffset)

	// Scan the index file to find the position
	idxScanner := bufio.NewScanner(s.indexFile)
	for idxScanner.Scan() {
		entry := idxScanner.Bytes()
		if len(entry) != 12 {
			continue // Corrupt entry
		}
		off := binary.BigEndian.Uint32(entry[0:4])
		if off == relativeOffset {
			pos := binary.BigEndian.Uint64(entry[4:12])
			return s.readMessageAt(pos)
		}
	}
	if err := idxScanner.Err(); err != nil {
		return nil, err
	}

	return nil, fmt.Errorf("offset %d not found in segment starting at %d", offset, s.baseOffset)
}

// readMessageAt reads a single message from a specific position in the log file.
func (s *Segment) readMessageAt(position uint64) (*message.Message, error) {
	lenBuf := make([]byte, lenWidth)
	if _, err := s.logFile.ReadAt(lenBuf, int64(position)); err != nil {
		return nil, err
	}

	msgLen := binary.BigEndian.Uint64(lenBuf)
	msgBuf := make([]byte, msgLen)
	if _, err := s.logFile.ReadAt(msgBuf, int64(position)+int64(lenWidth)); err != nil {
		return nil, err
	}

	return message.Deserialize(msgBuf)
}

// IsFull checks if the segment has reached its configured size limit.
func (s *Segment) IsFull() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stat, err := s.logFile.Stat()
	if err != nil {
		return false
	}
	return stat.Size() >= s.config.SegmentSize
}

// Close flushes data and closes the file handles.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isClosed {
		return nil
	}

	if err := s.logFile.Sync(); err != nil {
		return err
	}
	if err := s.indexFile.Sync(); err != nil {
		return err
	}
	if err := s.logFile.Close(); err != nil {
		return err
	}
	if err := s.indexFile.Close(); err != nil {
		return err
	}

	s.isClosed = true
	return nil
}

// BaseOffset returns the first offset in this segment.
func (s *Segment) BaseOffset() int64 {
	return s.baseOffset
}

// NextOffset returns the next offset to be written to this segment.
func (s *Segment) NextOffset() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextOffset
}
