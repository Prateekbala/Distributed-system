#!/bin/bash

# =============================================================================
# INDUSTRY-LEVEL KAFKA BENCHMARK COMPARISON
# =============================================================================
# This script compares your mini Kafka system against industry standards
# and generates a professional benchmark report

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m'

BROKER1_URL="http://localhost:8081"
BROKER2_URL="http://localhost:8082"
TOPIC_NAME="industry-benchmark"
RESULTS_FILE="industry_benchmark_$(date +%Y%m%d_%H%M%S).txt"

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

# Industry benchmark standards
THROUGHPUT_SINGLE_STD=10000
THROUGHPUT_CLUSTER_STD=50000
LATENCY_P50_STD=5
LATENCY_P95_STD=20
LATENCY_P99_STD=50
RECOVERY_TIME_STD=30
MEMORY_USAGE_STD=512
CONCURRENT_CONNECTIONS_STD=1000

# Function to run throughput test
test_throughput() {
    local test_name=$1
    local producers=$2
    local messages_per_producer=$3
    local total_messages=$((producers * messages_per_producer))
    
    print_info "Running $test_name: $producers producers, $messages_per_producer messages each = $total_messages total"
    
    start_time=$(date +%s%N)
    for i in $(seq 1 $producers); do
        (
            for j in $(seq 1 $messages_per_producer); do
                curl -s -X POST "$BROKER1_URL/topics/$TOPIC_NAME/produce" \
                    -H "Content-Type: application/json" \
                    -d "{\"value\": \"$test_name-$i-$j-$(date +%s%N)\", \"key\": \"$test_name-$i-$j\"}" > /dev/null
            done
        ) &
    done
    wait
    end_time=$(date +%s%N)
    
    duration=$(( (end_time - start_time) / 1000000 )) # Convert to milliseconds
    throughput=$(( total_messages * 1000 / duration )) # Messages per second
    
    echo "$test_name: $throughput msgs/sec (${duration}ms total)"
    echo "$test_name,$throughput,$duration" >> $RESULTS_FILE
}

# Function to run latency test
test_latency() {
    local test_name=$1
    local requests=$2
    
    print_info "Running $test_name: $requests requests"
    
    latencies=()
    for i in $(seq 1 $requests); do
        start_time=$(date +%s%N)
        curl -s -X POST "$BROKER1_URL/topics/$TOPIC_NAME/produce" \
            -H "Content-Type: application/json" \
            -d "{\"value\": \"$test_name-$i\", \"key\": \"$test_name-$i\"}" > /dev/null
        end_time=$(date +%s%N)
        latency=$(( (end_time - start_time) / 1000000 ))
        latencies+=($latency)
    done
    
    # Calculate statistics
    IFS=$'\n' sorted=($(sort -n <<<"${latencies[*]}"))
    unset IFS
    
    p50=${sorted[$((requests/2))]}
    p95=${sorted[$((requests*95/100))]}
    p99=${sorted[$((requests*99/100))]}
    
    total=0
    for latency in "${latencies[@]}"; do
        total=$((total + latency))
    done
    avg=$((total / requests))
    
    echo "$test_name - P50: ${p50}ms, P95: ${p95}ms, P99: ${p99}ms, Avg: ${avg}ms"
    echo "$test_name,p50,$p50" >> $RESULTS_FILE
    echo "$test_name,p95,$p95" >> $RESULTS_FILE
    echo "$test_name,p99,$p99" >> $RESULTS_FILE
    echo "$test_name,avg,$avg" >> $RESULTS_FILE
}

# Function to test fault tolerance
test_fault_tolerance() {
    print_info "Testing fault tolerance..."
    
    # Start background load
    for i in {1..100}; do
        (
            while true; do
                curl -s -X POST "$BROKER1_URL/topics/$TOPIC_NAME/produce" \
                    -H "Content-Type: application/json" \
                    -d "{\"value\": \"FaultTest-$(date +%s%N)\", \"key\": \"fault-$i\"}" > /dev/null
                sleep 0.01
            done
        ) &
    done
    
    # Wait for load to stabilize
    sleep 10
    
    # Kill leader and measure recovery time
    start_time=$(date +%s)
    pkill -f "node-id=node1"
    
    # Wait for recovery
    while ! curl -s $BROKER2_URL/health > /dev/null 2>&1; do
        sleep 1
    done
    end_time=$(date +%s)
    recovery_time=$((end_time - start_time))
    
    # Stop background load
    pkill -f "curl.*produce"
    
    echo "Fault tolerance: ${recovery_time} seconds recovery time"
    echo "fault_tolerance,recovery_time,$recovery_time" >> $RESULTS_FILE
}

# Function to test system resources
test_resources() {
    print_info "Testing system resources..."
    
    broker_pid=$(pgrep -f "go run.*broker")
    if [ -n "$broker_pid" ]; then
        # Memory usage in MB
        memory_kb=$(ps -o rss -p $broker_pid | tail -1)
        memory_mb=$((memory_kb / 1024))
        
        # File descriptors
        file_descriptors=$(lsof -p $broker_pid | wc -l)
        
        # Network connections
        network_connections=$(netstat -an | grep :8081 | wc -l)
        
        echo "Memory usage: ${memory_mb} MB"
        echo "File descriptors: $file_descriptors"
        echo "Network connections: $network_connections"
        
        echo "resources,memory_mb,$memory_mb" >> $RESULTS_FILE
        echo "resources,file_descriptors,$file_descriptors" >> $RESULTS_FILE
        echo "resources,network_connections,$network_connections" >> $RESULTS_FILE
    fi
}

# Function to generate comparison report
generate_report() {
    print_header "INDUSTRY BENCHMARK COMPARISON REPORT"
    
    echo "Mini Kafka System vs Industry Standards" > $RESULTS_FILE
    echo "=======================================" >> $RESULTS_FILE
    echo "Date: $(date)" >> $RESULTS_FILE
    echo "System: MacBook Air M4 (10-core CPU)" >> $RESULTS_FILE
    echo "" >> $RESULTS_FILE
    
    # Read results and compare with industry standards
    while IFS=',' read -r test metric value; do
        case $test in
            "throughput_single")
                if [ $value -ge $THROUGHPUT_SINGLE_STD ]; then
                    status="PASS"
                else
                    status="FAIL"
                fi
                echo "$test: $value msgs/sec (Industry: $THROUGHPUT_SINGLE_STD msgs/sec) - $status" >> $RESULTS_FILE
                ;;
            "throughput_cluster")
                if [ $value -ge $THROUGHPUT_CLUSTER_STD ]; then
                    status="PASS"
                else
                    status="FAIL"
                fi
                echo "$test: $value msgs/sec (Industry: $THROUGHPUT_CLUSTER_STD msgs/sec) - $status" >> $RESULTS_FILE
                ;;
            "latency_p50")
                if [ $value -le $LATENCY_P50_STD ]; then
                    status="PASS"
                else
                    status="FAIL"
                fi
                echo "$test: ${value}ms (Industry: ${LATENCY_P50_STD}ms) - $status" >> $RESULTS_FILE
                ;;
            "latency_p95")
                if [ $value -le $LATENCY_P95_STD ]; then
                    status="PASS"
                else
                    status="FAIL"
                fi
                echo "$test: ${value}ms (Industry: ${LATENCY_P95_STD}ms) - $status" >> $RESULTS_FILE
                ;;
            "latency_p99")
                if [ $value -le $LATENCY_P99_STD ]; then
                    status="PASS"
                else
                    status="FAIL"
                fi
                echo "$test: ${value}ms (Industry: ${LATENCY_P99_STD}ms) - $status" >> $RESULTS_FILE
                ;;
            "fault_tolerance")
                if [ $value -le $RECOVERY_TIME_STD ]; then
                    status="PASS"
                else
                    status="FAIL"
                fi
                echo "$test: ${value}s (Industry: ${RECOVERY_TIME_STD}s) - $status" >> $RESULTS_FILE
                ;;
            "resources")
                if [ $value -le $MEMORY_USAGE_STD ]; then
                    status="PASS"
                else
                    status="FAIL"
                fi
                echo "$test: ${value}MB (Industry: ${MEMORY_USAGE_STD}MB) - $status" >> $RESULTS_FILE
                ;;
        esac
    done < <(grep -v "^Mini Kafka" $RESULTS_FILE | grep -v "^===" | grep -v "^Date:" | grep -v "^System:" | grep -v "^$")
    
    print_success "Industry benchmark report generated: $RESULTS_FILE"
}

# Main execution
main() {
    print_header "INDUSTRY-LEVEL KAFKA BENCHMARK"
    
    # Check if broker is running
    if ! curl -s $BROKER1_URL/health > /dev/null; then
        print_error "Broker not running. Please start your cluster first."
        exit 1
    fi
    
    # Create test topic
    print_info "Creating test topic..."
    curl -s -X POST "$BROKER1_URL/topics?name=$TOPIC_NAME" \
        -H "Content-Type: application/json" \
        -d '{"num_partitions": 8, "replication_factor": 2}' > /dev/null
    print_success "Topic created"
    
    # Initialize results file
    echo "test,metric,value" > $RESULTS_FILE
    
    # Run benchmarks
    test_throughput "throughput_single" 100 100
    test_throughput "throughput_cluster" 500 100
    test_latency "latency_single" 100
    test_latency "latency_concurrent" 100
    test_fault_tolerance
    test_resources
    
    # Generate report
    generate_report
    
    print_header "BENCHMARK COMPLETED"
    print_success "Results saved to: $RESULTS_FILE"
    print_info "View results: cat $RESULTS_FILE"
}

# Run main function
main "$@"
