#!/bin/bash

# Test script for optimized Kafka-like cluster
echo "Starting optimized Kafka-like cluster test..."

# Clean up any existing data directories
rm -rf data-node1 data-node2

# Start node1 (leader)
echo "Starting node1 (leader) on port 8081..."
go run ./cmd/broker/main.go --node-id=node1 --port=8081 --cluster-port=7966 --address=127.0.0.1 --data-dir=data-node1 &
NODE1_PID=$!

# Wait for node1 to start
sleep 5

# Start node2 (follower)
echo "Starting node2 (follower) on port 8082..."
go run ./cmd/broker/main.go --node-id=node2 --port=8082 --cluster-port=7967 --address=127.0.0.1 --data-dir=data-node2 --seeds=127.0.0.1:7966 &
NODE2_PID=$!

# Wait for both nodes to start
sleep 10

echo "Cluster started. Testing endpoints..."

# Test health endpoints
echo "Testing node1 health..."
curl -s http://localhost:8081/health | jq .

echo "Testing node2 health..."
curl -s http://localhost:8082/health | jq .

# Test cluster status
echo "Testing cluster status..."
curl -s http://localhost:8081/cluster/status | jq .

# Test topic creation
echo "Creating test topic..."
curl -s -X POST http://localhost:8081/topics/test-topic \
  -H "Content-Type: application/json" \
  -d '{"num_partitions": 2, "replication_factor": 2}' | jq .

# Test message production
echo "Producing test message..."
curl -s -X POST http://localhost:8081/topics/test-topic/produce \
  -H "Content-Type: application/json" \
  -d '{"value": "Hello, optimized Kafka!", "key": "test-key"}' | jq .

# Test message consumption
echo "Consuming messages..."
curl -s "http://localhost:8081/topics/test-topic/partitions/0/consume?offset=0&limit=5" | jq .

echo "Test completed. Press Ctrl+C to stop the cluster."

# Wait for user to stop
trap "echo 'Stopping cluster...'; kill $NODE1_PID $NODE2_PID; exit" INT
wait 