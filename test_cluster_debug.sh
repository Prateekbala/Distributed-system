#!/bin/bash

echo "=== Cluster Debug Test Script ==="

# Function to check if a port is in use
check_port() {
    local port=$1
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null ; then
        echo "Port $port is in use"
        return 0
    else
        echo "Port $port is not in use"
        return 1
    fi
}

# Function to test HTTP endpoints
test_endpoint() {
    local url=$1
    local description=$2
    echo "Testing $description: $url"
    curl -s "$url" | jq '.' 2>/dev/null || echo "Failed to parse JSON or endpoint not available"
    echo "---"
}

# Check if ports are in use
echo "=== Checking Port Usage ==="
check_port 8081
check_port 8082
check_port 8966
check_port 8967
check_port 7966
check_port 7967

echo ""
echo "=== Testing Node1 Endpoints ==="
test_endpoint "http://localhost:8081/debug/metadata" "Node1 Metadata"
test_endpoint "http://localhost:8081/debug/raft" "Node1 Raft State"
test_endpoint "http://localhost:8081/debug/memberlist" "Node1 Memberlist"
test_endpoint "http://localhost:8081/cluster/status" "Node1 Cluster Status"

echo ""
echo "=== Testing Node2 Endpoints ==="
test_endpoint "http://localhost:8082/debug/metadata" "Node2 Metadata"
test_endpoint "http://localhost:8082/debug/raft" "Node2 Raft State"
test_endpoint "http://localhost:8082/debug/memberlist" "Node2 Memberlist"
test_endpoint "http://localhost:8082/cluster/status" "Node2 Cluster Status"

echo ""
echo "=== Testing Health Endpoints ==="
test_endpoint "http://localhost:8081/health" "Node1 Health"
test_endpoint "http://localhost:8082/health" "Node2 Health"

echo ""
echo "=== Debug Complete ===" 