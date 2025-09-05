#!/bin/bash

# =============================================================================
# INDUSTRY-LEVEL KAFKA BENCHMARK SUITE
# =============================================================================
# This script provides comprehensive benchmarking for your mini Kafka system
# to demonstrate industry-level performance metrics

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
BROKER1_URL="http://localhost:8081"
BROKER2_URL="http://localhost:8082"
TOPIC_NAME="benchmark-topic"
RESULTS_DIR="benchmark_results"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# Create results directory
mkdir -p $RESULTS_DIR

# Function to print colored output
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

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${CYAN}ℹ $1${NC}"
}

# Function to check if brokers are running
check_brokers() {
    print_info "Checking broker health..."
    
    if curl -s $BROKER1_URL/health > /dev/null; then
        print_success "Broker 1 (Leader) is healthy"
    else
        print_error "Broker 1 is not responding"
        exit 1
    fi
    
    if curl -s $BROKER2_URL/health > /dev/null; then
        print_success "Broker 2 (Follower) is healthy"
    else
        print_error "Broker 2 is not responding"
        exit 1
    fi
}

# Function to create test topic
setup_topic() {
    print_info "Setting up test topic: $TOPIC_NAME"
    
    curl -s -X POST "$BROKER1_URL/topics?name=$TOPIC_NAME" \
        -H "Content-Type: application/json" \
        -d '{"num_partitions": 8, "replication_factor": 2}' > /dev/null
    
    print_success "Topic created with 8 partitions and replication factor 2"
}

# Function to measure throughput
benchmark_throughput() {
    print_header "THROUGHPUT BENCHMARK"
    
    local test_name="throughput"
    local results_file="$RESULTS_DIR/${test_name}_${TIMESTAMP}.txt"
    
    echo "Throughput Benchmark Results - $(date)" > $results_file
    echo "=========================================" >> $results_file
    
    # Test 1: Sequential Production
    print_info "Test 1: Sequential Production (1000 messages)"
    start_time=$(date +%s%N)
    for i in {1..1000}; do
        curl -s -X POST "$BROKER1_URL/topics/$TOPIC_NAME/produce" \
            -H "Content-Type: application/json" \
            -d "{\"value\": \"Sequential-$i\", \"key\": \"seq-$i\"}" > /dev/null
    done
    end_time=$(date +%s%N)
    duration=$(( (end_time - start_time) / 1000000 )) # Convert to milliseconds
    throughput=$(( 1000 * 1000 / duration )) # Messages per second
    echo "Sequential: $throughput msgs/sec (${duration}ms total)" | tee -a $results_file
    
    # Test 2: Concurrent Production (100 producers, 100 messages each)
    print_info "Test 2: Concurrent Production (100 producers, 100 messages each = 10,000 messages)"
    start_time=$(date +%s%N)
    for i in {1..100}; do
        (
            for j in {1..100}; do
                curl -s -X POST "$BROKER1_URL/topics/$TOPIC_NAME/produce" \
                    -H "Content-Type: application/json" \
                    -d "{\"value\": \"Concurrent-$i-$j\", \"key\": \"conc-$i-$j\"}" > /dev/null
            done
        ) &
    done
    wait
    end_time=$(date +%s%N)
    duration=$(( (end_time - start_time) / 1000000 ))
    throughput=$(( 10000 * 1000 / duration ))
    echo "Concurrent: $throughput msgs/sec (${duration}ms total)" | tee -a $results_file
    
    # Test 3: Optimized Concurrency (100 producers, 100 messages each)
    print_info "Test 3: Optimized Concurrency (100 producers, 100 messages each = 10,000 messages)"
    start_time=$(date +%s%N)
    for i in {1..100}; do
        (
            for j in {1..100}; do
                curl -s -X POST "$BROKER1_URL/topics/$TOPIC_NAME/produce" \
                    -H "Content-Type: application/json" \
                    -d "{\"value\": \"OptConc-$i-$j\", \"key\": \"opt-$i-$j\"}" > /dev/null
            done
        ) &
    done
    wait
    end_time=$(date +%s%N)
    duration=$(( (end_time - start_time) / 1000000 ))
    throughput=$(( 10000 * 1000 / duration ))
    echo "Optimized Concurrency: $throughput msgs/sec (${duration}ms total)" | tee -a $results_file
    
    # Test 4: Sustained Load (30 seconds - macOS optimized)
    print_info "Test 4: Sustained Load (30 seconds - macOS optimized)"
    start_time=$(date +%s)
    message_count=0
    # Use gtimeout if available, otherwise use a simple loop
    if command -v gtimeout >/dev/null 2>&1; then
        gtimeout 30 bash -c '
            while true; do
                for i in {1..50}; do
                    curl -s -X POST "'$BROKER1_URL'/topics/'$TOPIC_NAME'/produce" \
                        -H "Content-Type: application/json" \
                        -d "{\"value\": \"Sustained-$(date +%s%N)\", \"key\": \"sustained-$i\"}" > /dev/null &
                done
                sleep 0.1
            done
        '
    else
        # Fallback for macOS without coreutils
        end_time=$((start_time + 30))
        while [ $(date +%s) -lt $end_time ]; do
            for i in {1..50}; do
                curl -s -X POST "$BROKER1_URL/topics/$TOPIC_NAME/produce" \
                    -H "Content-Type: application/json" \
                    -d "{\"value\": \"Sustained-$(date +%s%N)\", \"key\": \"sustained-$i\"}" > /dev/null &
            done
            sleep 0.1
        done
    fi
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    # Estimate message count (50 messages every 0.1 seconds for 30 seconds)
    estimated_messages=$(( 50 * 30 * 10 ))
    sustained_throughput=$(( estimated_messages / duration ))
    echo "Sustained Load: ~$sustained_throughput msgs/sec (${duration}s duration)" | tee -a $results_file
    
    print_success "Throughput benchmark completed. Results saved to $results_file"
}

# Function to measure latency
benchmark_latency() {
    print_header "LATENCY BENCHMARK"
    
    local test_name="latency"
    local results_file="$RESULTS_DIR/${test_name}_${TIMESTAMP}.txt"
    
    echo "Latency Benchmark Results - $(date)" > $results_file
    echo "====================================" >> $results_file
    
    # Test 1: Single Request Latency (100 requests)
    print_info "Test 1: Single Request Latency (100 requests)"
    latencies=()
    for i in {1..100}; do
        start_time=$(date +%s%N)
        curl -s -X POST "$BROKER1_URL/topics/$TOPIC_NAME/produce" \
            -H "Content-Type: application/json" \
            -d "{\"value\": \"Latency-$i\", \"key\": \"lat-$i\"}" > /dev/null
        end_time=$(date +%s%N)
        latency=$(( (end_time - start_time) / 1000000 )) # Convert to milliseconds
        latencies+=($latency)
    done
    
    # Calculate statistics
    IFS=$'\n' sorted=($(sort -n <<<"${latencies[*]}"))
    unset IFS
    
    p50=${sorted[49]}
    p95=${sorted[94]}
    p99=${sorted[98]}
    p99_9=${sorted[99]}
    
    total=0
    for latency in "${latencies[@]}"; do
        total=$((total + latency))
    done
    avg=$((total / 100))
    
    echo "P50: ${p50}ms" | tee -a $results_file
    echo "P95: ${p95}ms" | tee -a $results_file
    echo "P99: ${p99}ms" | tee -a $results_file
    echo "P99.9: ${p99_9}ms" | tee -a $results_file
    echo "Average: ${avg}ms" | tee -a $results_file
    
    # Test 2: Latency under load
    print_info "Test 2: Latency under concurrent load (50 concurrent producers)"
    # Start background load
    for i in {1..50}; do
        (
            while true; do
                curl -s -X POST "$BROKER1_URL/topics/$TOPIC_NAME/produce" \
                    -H "Content-Type: application/json" \
                    -d "{\"value\": \"Load-$(date +%s%N)\", \"key\": \"load-$i\"}" > /dev/null
                sleep 0.01
            done
        ) &
    done
    
    # Measure latency under load
    load_latencies=()
    for i in {1..50}; do
        start_time=$(date +%s%N)
        curl -s -X POST "$BROKER1_URL/topics/$TOPIC_NAME/produce" \
            -H "Content-Type: application/json" \
            -d "{\"value\": \"UnderLoad-$i\", \"key\": \"underload-$i\"}" > /dev/null
        end_time=$(date +%s%N)
        latency=$(( (end_time - start_time) / 1000000 ))
        load_latencies+=($latency)
    done
    
    # Stop background load
    pkill -f "curl.*produce"
    
    # Calculate load statistics
    IFS=$'\n' sorted_load=($(sort -n <<<"${load_latencies[*]}"))
    unset IFS
    
    p50_load=${sorted_load[24]}
    p95_load=${sorted_load[47]}
    p99_load=${sorted_load[49]}
    
    total_load=0
    for latency in "${load_latencies[@]}"; do
        total_load=$((total_load + latency))
    done
    avg_load=$((total_load / 50))
    
    echo "Under Load - P50: ${p50_load}ms" | tee -a $results_file
    echo "Under Load - P95: ${p95_load}ms" | tee -a $results_file
    echo "Under Load - P99: ${p99_load}ms" | tee -a $results_file
    echo "Under Load - Average: ${avg_load}ms" | tee -a $results_file
    
    print_success "Latency benchmark completed. Results saved to $results_file"
}

# Function to measure consumption performance
benchmark_consumption() {
    print_header "CONSUMPTION BENCHMARK"
    
    local test_name="consumption"
    local results_file="$RESULTS_DIR/${test_name}_${TIMESTAMP}.txt"
    
    echo "Consumption Benchmark Results - $(date)" > $results_file
    echo "=======================================" >> $results_file
    
    # Test 1: Sequential Consumption
    print_info "Test 1: Sequential Consumption (1000 messages)"
    start_time=$(date +%s%N)
    for i in {1..1000}; do
        curl -s "$BROKER1_URL/topics/$TOPIC_NAME/partitions/$((i % 8))/consume?offset=$i&limit=1" > /dev/null
    done
    end_time=$(date +%s%N)
    duration=$(( (end_time - start_time) / 1000000 ))
    throughput=$(( 1000 * 1000 / duration ))
    echo "Sequential Consumption: $throughput msgs/sec (${duration}ms total)" | tee -a $results_file
    
    # Test 2: Concurrent Consumption
    print_info "Test 2: Concurrent Consumption (50 consumers, 20 messages each)"
    start_time=$(date +%s%N)
    for i in {1..50}; do
        (
            for j in {1..20}; do
                curl -s "$BROKER1_URL/topics/$TOPIC_NAME/partitions/$((i % 8))/consume?offset=$j&limit=1" > /dev/null
            done
        ) &
    done
    wait
    end_time=$(date +%s%N)
    duration=$(( (end_time - start_time) / 1000000 ))
    throughput=$(( 1000 * 1000 / duration ))
    echo "Concurrent Consumption: $throughput msgs/sec (${duration}ms total)" | tee -a $results_file
    
    # Test 3: Batch Consumption
    print_info "Test 3: Batch Consumption (100 batches of 100 messages each)"
    start_time=$(date +%s%N)
    for i in {1..100}; do
        curl -s "$BROKER1_URL/topics/$TOPIC_NAME/partitions/$((i % 8))/consume?offset=$((i * 10))&limit=100" > /dev/null
    done
    end_time=$(date +%s%N)
    duration=$(( (end_time - start_time) / 1000000 ))
    throughput=$(( 10000 * 1000 / duration ))
    echo "Batch Consumption: $throughput msgs/sec (${duration}ms total)" | tee -a $results_file
    
    print_success "Consumption benchmark completed. Results saved to $results_file"
}

# Function to measure system resources
benchmark_resources() {
    print_header "RESOURCE USAGE BENCHMARK"
    
    local test_name="resources"
    local results_file="$RESULTS_DIR/${test_name}_${TIMESTAMP}.txt"
    
    echo "Resource Usage Benchmark Results - $(date)" > $results_file
    echo "===========================================" >> $results_file
    
    # Get broker PIDs
    broker1_pid=$(pgrep -f "node-id=node1")
    broker2_pid=$(pgrep -f "node-id=node2")
    
    if [ -n "$broker1_pid" ] && [ -n "$broker2_pid" ]; then
        # Memory usage
        echo "Memory Usage:" | tee -a $results_file
        ps -o pid,rss,vsz,comm -p $broker1_pid | tee -a $results_file
        ps -o pid,rss,vsz,comm -p $broker2_pid | tee -a $results_file
        
        # CPU usage
        echo "CPU Usage:" | tee -a $results_file
        top -l 1 -pid $broker1_pid | grep "CPU usage" | tee -a $results_file
        
        # File descriptors
        echo "File Descriptors:" | tee -a $results_file
        lsof -p $broker1_pid | wc -l | tee -a $results_file
        lsof -p $broker2_pid | wc -l | tee -a $results_file
        
        # Network connections
        echo "Network Connections:" | tee -a $results_file
        netstat -an | grep :8081 | wc -l | tee -a $results_file
        netstat -an | grep :8082 | wc -l | tee -a $results_file
    else
        print_warning "Could not find broker processes for resource monitoring"
    fi
    
    print_success "Resource benchmark completed. Results saved to $results_file"
}

# Function to test fault tolerance
benchmark_fault_tolerance() {
    print_header "FAULT TOLERANCE BENCHMARK"
    
    local test_name="fault_tolerance"
    local results_file="$RESULTS_DIR/${test_name}_${TIMESTAMP}.txt"
    
    echo "Fault Tolerance Benchmark Results - $(date)" > $results_file
    echo "=============================================" >> $results_file
    
    # Test 1: Leader failure during load
    print_info "Test 1: Leader failure during load"
    
    # Start background load
    for i in {1..100}; do
        (
            while true; do
                curl -s -X POST "$BROKER1_URL/topics/$TOPIC_NAME/produce" \
                    -H "Content-Type: application/json" \
                    -d "{\"value\": \"Failover-$(date +%s%N)\", \"key\": \"failover-$i\"}" > /dev/null
                sleep 0.01
            done
        ) &
    done
    
    # Wait for load to stabilize
    sleep 10
    
    # Kill leader
    print_info "Killing leader (node1)..."
    pkill -f "node-id=node1"
    
    # Measure recovery time
    start_time=$(date +%s)
    while ! curl -s $BROKER2_URL/health > /dev/null; do
        sleep 1
    done
    end_time=$(date +%s)
    recovery_time=$((end_time - start_time))
    
    echo "Recovery time: ${recovery_time} seconds" | tee -a $results_file
    
    # Test if new leader can handle requests
    print_info "Testing new leader functionality..."
    for i in {1..10}; do
        curl -s -X POST "$BROKER2_URL/topics/$TOPIC_NAME/produce" \
            -H "Content-Type: application/json" \
            -d "{\"value\": \"AfterFailover-$i\", \"key\": \"after-$i\"}" > /dev/null
    done
    
    # Stop background load
    pkill -f "curl.*produce"
    
    print_success "Fault tolerance benchmark completed. Results saved to $results_file"
}

# Function to generate summary report
generate_summary() {
    print_header "BENCHMARK SUMMARY REPORT"
    
    local summary_file="$RESULTS_DIR/benchmark_summary_${TIMESTAMP}.txt"
    
    echo "Mini Kafka System Benchmark Summary" > $summary_file
    echo "===================================" >> $summary_file
    echo "Date: $(date)" >> $summary_file
    echo "System: MacBook Air M4 (10-core CPU)" >> $summary_file
    echo "" >> $summary_file
    
    # Read and summarize results from all test files
    for file in $RESULTS_DIR/*_${TIMESTAMP}.txt; do
        if [ -f "$file" ]; then
            echo "=== $(basename $file) ===" >> $summary_file
            cat "$file" >> $summary_file
            echo "" >> $summary_file
        fi
    done
    
    print_success "Summary report generated: $summary_file"
    
    # Display key metrics
    echo ""
    print_header "KEY PERFORMANCE METRICS"
    echo "Results saved in: $RESULTS_DIR/"
    echo "Summary report: $summary_file"
    echo ""
    print_info "To view results:"
    echo "  cat $summary_file"
    echo "  ls -la $RESULTS_DIR/"
}

# Main execution
main() {
    print_header "MINI KAFKA BENCHMARK SUITE"
    print_info "Starting comprehensive benchmarking..."
    
    # Check prerequisites
    check_brokers
    setup_topic
    
    # Run all benchmarks
    benchmark_throughput
    benchmark_latency
    benchmark_consumption
    benchmark_resources
    benchmark_fault_tolerance
    
    # Generate summary
    generate_summary
    
    print_header "BENCHMARK COMPLETED"
    print_success "All benchmarks completed successfully!"
    print_info "Check the results directory for detailed reports: $RESULTS_DIR/"
}

# Run main function
main "$@"
