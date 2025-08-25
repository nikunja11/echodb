#!/bin/bash
echo "🚀 Starting Follower as background service..."

# ✅ Set S3 endpoint for LocalStack
export S3_ENDPOINT=http://localhost:4566

# Start follower in background
nohup mvn exec:java \
    -Dexec.mainClass="com.slatedb.Main" \
    -Dexec.args="node-2 follower --leader=node-1 --port=8082" \
    > leader.log 2>&1 &

echo $! > follower.pid
echo "✅ Follower started with PID: $(cat follower.pid)"
echo "📋 Logs: tail -f follower.log"