@echo off
echo Starting optimized Kafka-like cluster test...

REM Clean up any existing data directories
if exist data-node1 rmdir /s /q data-node1
if exist data-node2 rmdir /s /q data-node2

REM Start node1 (leader)
echo Starting node1 (leader) on port 8081...
start "Node1" cmd /c "go run ./cmd/broker/main.go --node-id=node1 --port=8081 --cluster-port=7966 --address=127.0.0.1 --data-dir=data-node1"

REM Wait for node1 to start
timeout /t 15 /nobreak > nul

REM Start node2 (follower)
echo Starting node2 (follower) on port 8082...
start "Node2" cmd /c "go run ./cmd/broker/main.go --node-id=node2 --port=8082 --cluster-port=7967 --address=127.0.0.1 --data-dir=data-node2 --seeds=127.0.0.1:7966"

REM Wait for both nodes to start
timeout /t 15 /nobreak > nul

echo Cluster started. Testing endpoints...

REM Test health endpoints
echo Testing node1 health...
curl -s http://localhost:8081/health

echo Testing node2 health...
curl -s http://localhost:8082/health

REM Test cluster status
echo Testing cluster status...
curl -s http://localhost:8081/cluster/status

REM Test topic creation
echo Creating test topic...
curl -s -X POST http://localhost:8081/topics/test-topic -H "Content-Type: application/json" -d "{\"num_partitions\": 2, \"replication_factor\": 2}"

REM Test message production
echo Producing test message...
curl -s -X POST http://localhost:8081/topics/test-topic/produce -H "Content-Type: application/json" -d "{\"value\": \"Hello, optimized Kafka!\", \"key\": \"test-key\"}"

REM Test message consumption
echo Consuming messages...
curl -s "http://localhost:8081/topics/test-topic/partitions/0/consume?offset=0&limit=5"

echo Test completed. Press any key to stop the cluster.
pause

REM Stop the processes
taskkill /f /im go.exe > nul 2>&1
taskkill /f /im broker.exe > nul 2>&1

echo Cluster stopped. 