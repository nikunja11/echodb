#!/bin/bash
echo "🚀 Starting Follower with interactive console..."

# ✅ Set S3 endpoint for LocalStack
export S3_ENDPOINT=http://localhost:4566

# ✅ Start follower in console mode (interactive)
mvn exec:java \
    -Dexec.mainClass="com.slatedb.Main" \
    -Dexec.args="node-2 follower --leader=node-1 --port=8082 --console"