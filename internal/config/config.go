package config

import (
	"os"
	"path/filepath"
	"time"
)

// Config represents the broker configuration
type Config struct {
	
    // Storage settings
    DataDir           string
    SegmentSize       int64         // Max size per segment file
    RetentionTime     time.Duration // How long to keep messages
    RetentionSize     int64         // Max total size per partition
    FlushInterval    time.Duration // How often to flush to disk
    
    // Server settings
    Port             string
    
    // WAL settings
    WALEnabled       bool
    WALSyncInterval  time.Duration

    // Consumer settings
    OffsetCommitInterval time.Duration // How often to commit consumer offsets
    ClusterPort int // Cluster communication port for memberlist
}

func DefaultConfig() *Config {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = "."
	}
    return &Config{
        DataDir:          filepath.Join(homeDir, ".mini-kafka"),
        SegmentSize:      1024 * 1024 * 100, // 100MB per segment
        RetentionTime:    time.Hour * 24 * 7, // 7 days
        RetentionSize:    1024 * 1024 * 1024, // 1GB per partition
        FlushInterval:    time.Second * 5,
        Port:             ":9092",
        WALEnabled:       true,
        WALSyncInterval:  time.Millisecond * 100,
        OffsetCommitInterval: time.Second * 10,
        ClusterPort: 7946,
    }
}



