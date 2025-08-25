#!/bin/bash
echo "🚀 Starting SlateDB 3-Node Cluster with Leader Election..."

# Start LocalStack if not running
docker-compose up -d localstack
sleep 5

# Create S3 bucket
aws --endpoint-url=http://localhost:4566 s3 mb s3://echodb-test 2>/dev/null || true

# Set S3 endpoint for LocalStack
export S3_ENDPOINT=http://localhost:4566

# Kill any existing processes
pkill -f "com.slatedb.Main" 2>/dev/null || true
rm -f node-*.pid node-*.log

echo "🎯 Starting 3 nodes with automatic leader election..."

# Start Node 1 (will try to become leader)
echo "Starting node-1..."
nohup mvn exec:java \
    -Dexec.mainClass="com.slatedb.Main" \
    -Dexec.args="node-1 auto --port=8081 --http-server" \
    > node-1.log 2>&1 &
echo $! > node-1.pid

sleep 3

# Start Node 2 (will try to become leader if node-1 fails)
echo "Starting node-2..."
nohup mvn exec:java \
    -Dexec.mainClass="com.slatedb.Main" \
    -Dexec.args="node-2 auto --port=8082 --http-server" \
    > node-2.log 2>&1 &
echo $! > node-2.pid

sleep 3

# Start Node 3 (will try to become leader if others fail)
echo "Starting node-3..."
nohup mvn exec:java \
    -Dexec.mainClass="com.slatedb.Main" \
    -Dexec.args="node-3 auto --port=8083 --http-server" \
    > node-3.log 2>&1 &
echo $! > node-3.pid

sleep 5

echo "✅ Cluster started!"
echo "📊 Node 1: http://localhost:8081 (PID: $(cat node-1.pid))"
echo "📊 Node 2: http://localhost:8082 (PID: $(cat node-2.pid))"
echo "📊 Node 3: http://localhost:8083 (PID: $(cat node-3.pid))"
echo ""
echo "🔍 Check leader status:"
echo "curl http://localhost:8081/api/status"
echo "curl http://localhost:8082/api/status"
echo "curl http://localhost:8083/api/status"