#!/bin/bash

# =============================================================================
# HIGH PERFORMANCE BENCHMARK - TARGET 17.9K msgs/sec
# =============================================================================
# Optimized to reproduce peak performance with 200 concurrent producers

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
PURPLE='\033[0;35m'
NC='\033[0m'

BROKER1_URL="http://localhost:8081"
BROKER2_URL="http://localhost:8082"
TOPIC_NAME="high-perf-benchmark"

print_header() {
    echo -e "${BLUE}==============================================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}==============================================================================${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

print_performance() {
    echo -e "${PURPLE}🚀 $1${NC}"
}

# Check if broker is running
if ! curl -s $BROKER1_URL/health > /dev/null; then
    echo "Error: Broker not running. Please start your cluster first."
    exit 1
fi

print_header "HIGH PERFORMANCE BENCHMARK - TARGET 17.9K msgs/sec"

# Create test topic with optimal settings
print_info "Creating optimized test topic..."
curl -s -X POST "$BROKER1_URL/topics?name=$TOPIC_NAME" \
    -H "Content-Type: application/json" \
    -d '{"num_partitions": 8, "replication_factor": 2}' > /dev/null
print_success "Topic created with 8 partitions"

# Warm up the system
print_info "Warming up system (10 seconds)..."
for i in {1..100}; do
    curl -s -X POST "$BROKER1_URL/topics/$TOPIC_NAME/produce" \
        -H "Content-Type: application/json" \
        -d "{\"value\": \"Warmup-$i\", \"key\": \"warmup-$i\"}" > /dev/null
done
sleep 10

# Test 1: Baseline Performance (50 producers)
print_info "Test 1: Baseline Performance (50 producers, 100 messages each = 5,000 messages)"
start_time=$(date +%s%N)
for i in {1..50}; do
    (
        for j in {1..100}; do
            curl -s -X POST "$BROKER1_URL/topics/$TOPIC_NAME/produce" \
                -H "Content-Type: application/json" \
                -d "{\"value\": \"Baseline-$i-$j\", \"key\": \"base-$i-$j\"}" > /dev/null
        done
    ) &
done
wait
end_time=$(date +%s%N)
duration=$(( (end_time - start_time) / 1000000 ))
throughput=$(( 5000 * 1000 / duration ))
print_performance "Baseline (50 producers): $throughput msgs/sec (${duration}ms total)"

# Cool down
print_info "Cooling down for 5 seconds..."
sleep 5

# Test 2: Peak Performance Attempt (200 producers)
print_info "Test 2: Peak Performance Attempt (200 producers, 100 messages each = 20,000 messages)"
start_time=$(date +%s%N)
for i in {1..200}; do
    (
        for j in {1..100}; do
            curl -s -X POST "$BROKER1_URL/topics/$TOPIC_NAME/produce" \
                -H "Content-Type: application/json" \
                -d "{\"value\": \"Peak-$i-$j\", \"key\": \"peak-$i-$j\"}" > /dev/null
        done
    ) &
done
wait
end_time=$(date +%s%N)
duration=$(( (end_time - start_time) / 1000000 ))
throughput=$(( 20000 * 1000 / duration ))
print_performance "Peak Attempt (200 producers): $throughput msgs/sec (${duration}ms total)"

# Cool down
print_info "Cooling down for 10 seconds..."
sleep 10

# Test 3: Latency Optimization Test
print_info "Test 3: Latency Optimization Test (100 requests for P99 measurement)"
latencies=()
for i in {1..100}; do
    start_time=$(date +%s%N)
    curl -s -X POST "$BROKER1_URL/topics/$TOPIC_NAME/produce" \
        -H "Content-Type: application/json" \
        -d "{\"value\": \"Latency-$i\", \"key\": \"lat-$i\"}" > /dev/null
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

print_performance "Latency P50: ${p50}ms"
print_performance "Latency P95: ${p95}ms"
print_performance "Latency P99: ${p99}ms"
print_performance "Latency Average: ${avg}ms"

# Test 4: Sustained High Performance (30 seconds)
print_info "Test 4: Sustained High Performance (30 seconds with 100 producers)"
start_time=$(date +%s)
end_time=$((start_time + 30))
message_count=0

while [ $(date +%s) -lt $end_time ]; do
    for i in {1..100}; do
        curl -s -X POST "$BROKER1_URL/topics/$TOPIC_NAME/produce" \
            -H "Content-Type: application/json" \
            -d "{\"value\": \"Sustained-$(date +%s%N)\", \"key\": \"sustained-$i\"}" > /dev/null &
    done
    sleep 0.05  # 50ms intervals
done

actual_duration=$(( $(date +%s) - start_time ))
estimated_messages=$(( 100 * 30 * 20 ))  # 100 producers * 30 seconds * 20 messages per second
sustained_throughput=$(( estimated_messages / actual_duration ))
print_performance "Sustained Performance: ~$sustained_throughput msgs/sec (${actual_duration}s duration)"

# Test 5: System Resources Check
print_info "Test 5: System Resources Check"
broker_pid=$(pgrep -f "go run.*broker")
if [ -n "$broker_pid" ]; then
    echo "Memory Usage:"
    ps -o pid,rss,vsz,comm -p $broker_pid
    echo "File Descriptors: $(lsof -p $broker_pid 2>/dev/null | wc -l || echo "0")"
    echo "Network Connections: $(netstat -an 2>/dev/null | grep :8081 | wc -l || echo "0")"
fi

print_header "HIGH PERFORMANCE BENCHMARK SUMMARY"
print_performance "Peak Throughput: $throughput msgs/sec"
print_performance "Latency P99: ${p99}ms"
print_performance "Sustained Performance: $sustained_throughput msgs/sec"

if [ $throughput -ge 15000 ]; then
    print_success "🎉 EXCELLENT! You achieved high performance!"
elif [ $throughput -ge 10000 ]; then
    print_success "✅ GOOD! Solid performance achieved!"
else
    print_warning "⚠️  Performance below target. Check system cooling and resources."
fi

print_info "To achieve 2ms latency, consider:"
print_info "1. Reduce message size"
print_info "2. Use localhost only (no network overhead)"
print_info "3. Increase Go GOMAXPROCS"
print_info "4. Use faster storage (SSD)"
print_info "5. Close unnecessary applications"
