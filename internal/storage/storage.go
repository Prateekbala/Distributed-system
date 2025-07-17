// internal/storage/storage.go
package storage

import (
	"Distributed-system/internal/cluster"
	"Distributed-system/internal/message"
	"io"
)

// Storage defines the interface for message storage. It must be safe for
// concurrent use.
type Storage interface {
	// Store appends a message to the log. The message's offset should be
	// assigned by the storage engine before this call.
	Store(msg *message.Message) error

	// GetMessage retrieves a single message by its offset.
	GetMessage(topic string, partition int, offset int64) (*message.Message, error)

	// GetMessages retrieves a slice of messages starting from a given offset.
	GetMessages(topic string, partition int, startOffset int64, limit int) ([]*message.Message, error)

	// GetHighWaterMark returns the next offset that will be assigned to a new
	// message in the given partition.
	GetHighWaterMark(topic string, partition int) int64

	// EnsureTopicDirs makes sure the physical directories for a topic and its
	// partitions exist on disk. This is used to prepare storage for a new
	// topic defined in the cluster's authoritative state.
	EnsureTopicDirs(meta *cluster.TopicMetadata) error

	// io.Closer is embedded for graceful shutdown.
	io.Closer
}
