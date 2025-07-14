package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Message metrics
	MessagesProduced = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "broker_messages_produced_total",
			Help: "Total number of messages produced",
		},
		[]string{"topic", "partition"},
	)

	MessagesConsumed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "broker_messages_consumed_total",
			Help: "Total number of messages consumed",
		},
		[]string{"topic", "partition"},
	)

	// Latency metrics
	ProduceLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "broker_produce_latency_seconds",
			Help:    "Message production latency in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"topic", "partition"},
	)

	ConsumeLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "broker_consume_latency_seconds",
			Help:    "Message consumption latency in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"topic", "partition"},
	)

	// Storage metrics
	StorageSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "broker_storage_size_bytes",
			Help: "Current storage size in bytes",
		},
		[]string{"topic", "partition"},
	)

	// Consumer group metrics
	ConsumerGroupLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "broker_consumer_group_lag",
			Help: "Consumer group lag (messages behind)",
		},
		[]string{"group_id", "topic", "partition"},
	)

	// Broker metrics
	ActiveConnections = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "broker_active_connections",
			Help: "Number of active client connections",
		},
	)

	// Error metrics
	ErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "broker_errors_total",
			Help: "Total number of errors",
		},
		[]string{"type"},
	)

	// Replication metrics
	ReplicationLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "broker_replication_lag",
			Help: "Replication lag in messages",
		},
		[]string{"topic", "partition", "follower"},
	)

	// System metrics
	CPUUsage = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "broker_cpu_usage_percent",
			Help: "CPU usage percentage",
		},
	)

	MemoryUsage = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "broker_memory_usage_bytes",
			Help: "Memory usage in bytes",
		},
	)

	// Topic metrics
	TopicPartitions = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "broker_topic_partitions",
			Help: "Number of partitions per topic",
		},
		[]string{"topic"},
	)

	// WAL metrics
	WALSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "broker_wal_size_bytes",
			Help: "Current WAL size in bytes",
		},
	)

	WALOperations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "broker_wal_operations_total",
			Help: "Total number of WAL operations",
		},
		[]string{"operation"},
	)

	// Throughput metrics
	ProduceThroughput = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "broker_produce_throughput_msgs_per_sec",
			Help: "Message production throughput (messages/sec)",
		},
		[]string{"topic", "partition"},
	)

	ConsumeThroughput = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "broker_consume_throughput_msgs_per_sec",
			Help: "Message consumption throughput (messages/sec)",
		},
		[]string{"topic", "partition"},
	)

	// Consumer health metrics
	ConsumerLastSeen = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "broker_consumer_last_seen_unix",
			Help: "Last seen timestamp (unix seconds) for each consumer",
		},
		[]string{"group_id", "consumer_id"},
	)
) 