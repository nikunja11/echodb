#!/bin/bash
echo "🚀 Starting Follower as HTTP server..."

# ✅ Set S3 endpoint for LocalStack
export S3_ENDPOINT=http://localhost:4566

# ✅ Start follower in HTTP server mode (no console)
nohup mvn exec:java \
    -Dexec.mainClass="com.slatedb.Main" \
    -Dexec.args="node-2 follower --leader=node-1 --port=8082 --http-server" \
    > follower.log 2>&1 &

echo $! > follower.pid
echo "✅ Follower HTTP server started with PID: $(cat follower.pid)"
echo "🌐 HTTP API available at: http://localhost:8082"
echo "📋 Logs: tail -f follower.log"