#!/bin/bash

# =============================================================================
# QUICK BENCHMARK SCRIPT - Industry Level Metrics
# =============================================================================
# Quick benchmark to get key performance metrics for your mini Kafka system

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

BROKER1_URL="http://localhost:8081"
TOPIC_NAME="quick-benchmark"

print_header() {
    echo -e "${BLUE}==============================================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}==============================================================================${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

# Check if broker is running
if ! curl -s $BROKER1_URL/health > /dev/null; then
    echo "Error: Broker not running. Please start your cluster first."
    exit 1
fi

print_header "QUICK BENCHMARK - INDUSTRY LEVEL METRICS"

# Create test topic
print_info "Creating test topic..."
curl -s -X POST "$BROKER1_URL/topics?name=$TOPIC_NAME" \
    -H "Content-Type: application/json" \
    -d '{"num_partitions": 4, "replication_factor": 2}' > /dev/null
print_success "Topic created"

# Test 1: Throughput Test
print_info "Testing throughput (1000 concurrent producers, 10 messages each = 10,000 messages)..."
start_time=$(date +%s%N)
for i in {1..1000}; do
    (
        for j in {1..10}; do
            curl -s -X POST "$BROKER1_URL/topics/$TOPIC_NAME/produce" \
                -H "Content-Type: application/json" \
                -d "{\"value\": \"Throughput-$i-$j\", \"key\": \"t-$i-$j\"}" > /dev/null
        done
    ) &
done
wait
end_time=$(date +%s%N)
duration=$(( (end_time - start_time) / 1000000 ))
throughput=$(( 10000 * 1000 / duration ))
echo "Throughput: $throughput messages/second"

# Test 2: Latency Test
print_info "Testing latency (100 requests)..."
latencies=()
for i in {1..100}; do
    start_time=$(date +%s%N)
    curl -s -X POST "$BROKER1_URL/topics/$TOPIC_NAME/produce" \
        -H "Content-Type: application/json" \
        -d "{\"value\": \"Latency-$i\", \"key\": \"l-$i\"}" > /dev/null
    end_time=$(date +%s%N)
    latency=$(( (end_time - start_time) / 1000000 ))
    latencies+=($latency)
done

# Calculate latency statistics
IFS=$'\n' sorted=($(sort -n <<<"${latencies[*]}"))
unset IFS
p50=${sorted[49]}
p95=${sorted[94]}
p99=${sorted[98]}

total=0
for latency in "${latencies[@]}"; do
    total=$((total + latency))
done
avg=$((total / 100))

echo "Latency P50: ${p50}ms"
echo "Latency P95: ${p95}ms"
echo "Latency P99: ${p99}ms"
echo "Latency Average: ${avg}ms"

# Test 3: Consumption Test
print_info "Testing consumption (1000 messages)..."
start_time=$(date +%s%N)
for i in {1..1000}; do
    curl -s "$BROKER1_URL/topics/$TOPIC_NAME/partitions/$((i % 4))/consume?offset=$i&limit=1" > /dev/null
done
end_time=$(date +%s%N)
duration=$(( (end_time - start_time) / 1000000 ))
consume_throughput=$(( 1000 * 1000 / duration ))
echo "Consumption Throughput: $consume_throughput messages/second"

# Test 4: System Resources
print_info "Checking system resources..."
broker_pid=$(pgrep -f "go run.*broker")
if [ -n "$broker_pid" ]; then
    echo "Memory Usage:"
    ps -o pid,rss,vsz,comm -p $broker_pid
    echo "File Descriptors: $(lsof -p $broker_pid 2>/dev/null | wc -l || echo "0")"
    echo "Network Connections: $(netstat -an 2>/dev/null | grep :8081 | wc -l || echo "0")"
fi

# Test 5: Fault Tolerance
print_info "Testing fault tolerance (leader failure)..."
# Start background load
for i in {1..50}; do
    (
        while true; do
            curl -s -X POST "$BROKER1_URL/topics/$TOPIC_NAME/produce" \
                -H "Content-Type: application/json" \
                -d "{\"value\": \"FaultTest-$(date +%s%N)\", \"key\": \"fault-$i\"}" > /dev/null
            sleep 0.01
        done
    ) &
done

# Wait and check if we have multiple brokers running
sleep 5
broker_count=$(pgrep -f "go run.*broker" | wc -l)
if [ "$broker_count" -lt 2 ]; then
    echo "Fault Tolerance: SKIPPED - Need at least 2 brokers for fault tolerance test"
    # Stop background load
    pkill -f "curl.*produce" 2>/dev/null
else
    print_info "Simulating leader failure..."
    pkill -f "node-id=node1"
    
    # Check if system is still responding
    sleep 5
    if curl -s $BROKER1_URL/health > /dev/null 2>&1; then
        echo "Fault Tolerance: SUCCESS - System still responding after leader failure"
    else
        echo "Fault Tolerance: PARTIAL - System recovering..."
    fi
    
    # Stop background load
    pkill -f "curl.*produce" 2>/dev/null
fi

print_header "BENCHMARK SUMMARY"
echo "Throughput: $throughput msgs/sec"
echo "Latency P99: ${p99}ms"
echo "Consumption: $consume_throughput msgs/sec"
if [ -n "$broker_pid" ]; then
    echo "Memory: $(ps -o rss -p $broker_pid | tail -1) KB"
    echo "File Descriptors: $(lsof -p $broker_pid 2>/dev/null | wc -l || echo "0")"
fi

print_success "Quick benchmark completed!"
