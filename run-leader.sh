#!/bin/bash
echo "🚀 Starting Leader as background service..."

# Start LocalStack if not running
docker-compose up -d localstack

# Wait for LocalStack
sleep 5

# Create S3 bucket
aws --endpoint-url=http://localhost:4566 s3 mb s3://slatedb-test 2>/dev/null || true

# ✅ Set S3 endpoint for LocalStack
export S3_ENDPOINT=http://localhost:4566

# ✅ Start leader using the working command
nohup mvn exec:java \
    -Dexec.mainClass="com.slatedb.Main" \
    -Dexec.args="node-1 leader --port=8081" \
    > leader.log 2>&1 &

echo $! > leader.pid
echo "✅ Leader started with PID: $(cat leader.pid)"
echo "📋 Logs: tail -f leader.log"